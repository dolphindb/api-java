package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExclusiveDBConnectionPool implements DBConnectionPool {
	private final List<AsyncWorker> workers_ = new ArrayList<>();
	private final Object workersLock_ = new Object();
	private final LinkedList<DBTask> taskLists_ = new LinkedList<>();
	private final Map<DBTask, AsyncWorker> runningTasks_ = new IdentityHashMap<>();
	private final Set<DBTask> unfinishedTasks_ = Collections.newSetFromMap(new IdentityHashMap<DBTask, Boolean>());
	private int runningTaskCount_ = 0;
	private int minimumPoolSize_;
	private int maximumPoolSize_;
	private int idleTimeout_;
	private String host_;
	private int port_;
	private String uid_;
	private String pwd_;
	private boolean loadBalance_;
	private boolean enableHighAvailability_;
	private String[] highAvailabilitySites_;
	private String initialScript_;
	private boolean compress_;
	private boolean useSSL_;
	private boolean usePython_;
	private String[] hosts_;
	private int[] ports_;
	private int nextWorkerIndex_ = 0;
	private int creatingWorkerCount_ = 0;
	private boolean dynamicPool_ = false;
	private volatile boolean isShutdown_ = false;
	private ThreadPoolExecutor cancellationExecutor_;
	private static final int CANCEL_CONNECT_TIMEOUT_MS = 2000;
	private static final int CANCEL_READ_TIMEOUT_MS = 2000;
	private static final int CANCEL_GRACE_PERIOD_MS = 2000;
	private static final int CANCEL_THREAD_COUNT = 2;
	private static final int WORKER_CONNECT_TIMEOUT_MS = 2000;
	private static final int WORKER_CREATE_MAX_ATTEMPTS = 3;
	private static final int WORKER_CREATE_BACKOFF_MS = 200;
	// Finite only for background rebuild / submit-path connect();
	// preparePublishedWorker() afterwards so run() HA stays unbounded.
	// Initial workers keep unlimited reconnect.
	private static final int WORKER_CONNECT_TRY_RECONNECT_NUMS = 3;
	private static final int SUBMIT_CONNECT_TRY_RECONNECT_NUMS = 1;
	private static final AtomicInteger POOL_INDEX = new AtomicInteger();

	private static final Logger log = LoggerFactory.getLogger(ExclusiveDBConnectionPool.class);

	private class AsyncWorker implements Runnable {
		private final DBConnection conn_;
		private final Thread workThread_;
		private final boolean dynamicWorker_;
		private volatile boolean active_ = false;
		private volatile boolean retiring_ = false;
		private long lastUsedTime_ = System.currentTimeMillis();

		public AsyncWorker(DBConnection conn, int workerIndex, boolean dynamicWorker) {
			this.conn_ = conn;
			this.dynamicWorker_ = dynamicWorker;
			workThread_ = new Thread(this, "ExclusiveDBConnectionPool-AsyncWorker-" + workerIndex);
		}

		public void start() {
			workThread_.start();
		}

		@Override
		public void run() {
			if (dynamicWorker_) {
				runDynamicWorker();
			} else {
				runFixedSizeWorker();
			}
		}

		private void runFixedSizeWorker() {
			try {
				while (!workThread_.isInterrupted() && !retiring_) {
					DBTask task;
					synchronized (taskLists_) {
						while (taskLists_.isEmpty() && !workThread_.isInterrupted() && !retiring_) {
							try {
								taskLists_.wait();
							} catch (InterruptedException e) {
								workThread_.interrupt();
							}
						}
						if (workThread_.isInterrupted() || retiring_) {
							break;
						}
						task = pollNextTask(this);
					}
					if (task != null) {
						executeOwnedTask(this, task);
					}
				}
			} finally {
				retireOrRemoveWorker(this);
			}
		}

		private void runDynamicWorker() {
			try {
				while (!workThread_.isInterrupted() && !retiring_) {
					DBTask task = null;
					synchronized (taskLists_) {
						while (taskLists_.isEmpty() && !workThread_.isInterrupted() && !retiring_) {
							try {
								long waitTime = getIdleWaitTime();
								if (waitTime <= 0 && tryRetireWorker(this)) {
									return;
								}
								if (waitTime > 0) {
									taskLists_.wait(waitTime);
								} else {
									taskLists_.wait();
								}
							} catch (InterruptedException e) {
								workThread_.interrupt();
								break;
							}
						}
						if (workThread_.isInterrupted() || retiring_) {
							break;
						}
						task = pollNextTask(this);
					}
					if (task == null) {
						continue;
					}
					executeOwnedTask(this, task);
				}
			} finally {
				retireOrRemoveWorker(this);
			}
		}

		private long getIdleWaitTime() {
			synchronized (workersLock_) {
				if (workers_.size() <= minimumPoolSize_) {
					return 0;
				}
			}
			return idleTimeout_ - (System.currentTimeMillis() - lastUsedTime_);
		}
	}

	private DBTask pollNextTask(AsyncWorker worker) {
		while (true) {
			DBTask task = taskLists_.pollLast();
			if (task == null) {
				return null;
			}
			if (!(task instanceof BasicDBTask)) {
				log.error("Skipping unsupported DBTask implementation from worker queue: "
						+ task.getClass().getName());
				markTaskPhysicallyFinished(task);
				continue;
			}
			BasicDBTask basicTask = (BasicDBTask) task;
			if (!basicTask.markRunning(worker.conn_)) {
				basicTask.markExecutionSkipped();
				markTaskPhysicallyFinished(task);
				continue;
			}
			worker.active_ = true;
			runningTaskCount_++;
			runningTasks_.put(task, worker);
			return task;
		}
	}

	private void executeOwnedTask(AsyncWorker worker, DBTask task) {
		try {
			task.call();
		} catch (InterruptedException e) {
			asBasicTask(task).markFailed("DolphinDB task execution was interrupted.");
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			String message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
			boolean recorded = asBasicTask(task).markFailed(message);
			if (recorded) {
				log.error("ExclusiveDBConnectionPool task execution failed: " + message, e);
			} else {
				log.warn("ExclusiveDBConnectionPool worker observed a terminal task after abort or timeout: "
						+ message);
			}
		} finally {
			synchronized (taskLists_) {
				runningTasks_.remove(task);
				runningTaskCount_--;
				worker.lastUsedTime_ = System.currentTimeMillis();
				worker.active_ = false;
			}
			BasicDBTask basicTask = asBasicTask(task);
			if (!basicTask.isFinished()) {
				basicTask.markFailed("BasicDBTask.call() returned without completing.");
			}
			basicTask.markExecutionFinished();
			markTaskPhysicallyFinished(task);
		}
	}

	private void markTaskPhysicallyFinished(DBTask task) {
		synchronized (taskLists_) {
			if (unfinishedTasks_.remove(task) && unfinishedTasks_.isEmpty()) {
				taskLists_.notifyAll();
			}
		}
	}

	private static BasicDBTask asBasicTask(DBTask task) {
		if (!(task instanceof BasicDBTask)) {
			throw new IllegalArgumentException("ExclusiveDBConnectionPool only supports BasicDBTask.");
		}
		return (BasicDBTask) task;
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability) throws IOException {
		this(host, port, uid, pwd, count, loadBalance, enableHighAvailability, null, "",false, false, false);
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, String[] highAvailabilitySites, String initialScript,boolean compress, boolean useSSL, boolean usePython) throws IOException {
		if (count <= 0)
			throw new RuntimeException("The thread count can not be less than 0");
		initPool(host, port, uid, pwd, count, count, 600000, loadBalance, enableHighAvailability, highAvailabilitySites, initialScript, compress, useSSL, usePython, false);
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int minimumPoolSize, int maximumPoolSize, int idleTimeout, boolean loadBalance, boolean enableHighAvailability) throws IOException {
		this(host, port, uid, pwd, minimumPoolSize, maximumPoolSize, idleTimeout, loadBalance, enableHighAvailability, null, "", false, false, false);
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int minimumPoolSize, int maximumPoolSize, int idleTimeout, boolean loadBalance, boolean enableHighAvailability, String[] highAvailabilitySites, String initialScript, boolean compress, boolean useSSL, boolean usePython) throws IOException {
		ExclusiveDBConnectionPoolConfig config = createConfig(host, port, uid, pwd, minimumPoolSize, maximumPoolSize, idleTimeout, loadBalance, enableHighAvailability, highAvailabilitySites, initialScript, compress, useSSL, usePython);
		initPool(config);
	}

	public ExclusiveDBConnectionPool(ExclusiveDBConnectionPoolConfig config) throws IOException {
		initPool(config);
	}

	public void execute(List<DBTask> tasks) {
		int demand = enqueueTasks(tasks);
		ensureWorkerCapacity(demand, false);
		rejectIfNoLiveWorkers();
		for (int i = 0; i < tasks.size(); i++) {
			BasicDBTask basicTask = asBasicTask(tasks.get(i));
			try {
				basicTask.awaitCompletion(-1);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				cancelRemainingTasks(tasks, i, "DolphinDB task wait was interrupted.");
				return;
			}
			waitForPhysicalCompletion(basicTask);
			if (Thread.currentThread().isInterrupted()) {
				cancelRemainingTasks(tasks, i + 1, "DolphinDB task wait was interrupted.");
				return;
			}
		}
	}

	public void execute(DBTask task) {
		execute(task, -1);
	}

	public void execute(DBTask task, int timeOut) {
		BasicDBTask basicTask = asBasicTask(task);
		long startNanos = timeOut > 0 ? System.nanoTime() : 0L;
		int demand = enqueueTasks(Collections.singletonList(task));
		if (timeOut > 0) {
			int remainingMs = remainingTimeoutMs(timeOut, startNanos);
			if (remainingMs <= 0) {
				handleIncompleteTask(basicTask,
						"DolphinDB task timed out after " + timeOut + " ms.", true);
				return;
			}
			ensureWorkerCapacity(demand, false,
					remainingMs, remainingMs, SUBMIT_CONNECT_TRY_RECONNECT_NUMS);
		} else {
			ensureWorkerCapacity(demand, false);
		}
		rejectIfNoLiveWorkers();
		boolean completed;
		try {
			completed = awaitCompletionWithinBudget(basicTask, timeOut, startNanos);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			handleIncompleteTask(basicTask, "DolphinDB task wait was interrupted.", false);
			return;
		}
		if (!completed) {
			handleIncompleteTask(basicTask,
					"DolphinDB task timed out after " + timeOut + " ms.", true);
		} else {
			waitForPhysicalCompletion(basicTask);
		}
	}

	private static int remainingTimeoutMs(int timeOut, long startNanos) {
		long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
		long remainingMs = timeOut - elapsedMs;
		if (remainingMs <= 0) {
			return 0;
		}
		return remainingMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remainingMs;
	}

	private boolean awaitCompletionWithinBudget(BasicDBTask task, int timeOut, long startNanos)
			throws InterruptedException {
		if (timeOut <= 0) {
			return task.awaitCompletion(timeOut);
		}
		long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
		long remainingMs = timeOut - elapsedMs;
		if (remainingMs <= 0) {
			return task.isFinished();
		}
		int waitMs = remainingMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remainingMs;
		return task.awaitCompletion(waitMs);
	}

	private void waitForPhysicalCompletion(BasicDBTask task) {
		try {
			task.awaitExecution(-1, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void cancelRemainingTasks(List<DBTask> tasks, int fromIndex, String message) {
		for (int i = fromIndex; i < tasks.size(); i++) {
			handleIncompleteTask(asBasicTask(tasks.get(i)), message, false);
		}
	}

	/**
	 * Enqueue tasks that this pool can actually run. Non-{@link BasicDBTask}
	 * implementations throw {@link IllegalArgumentException}. Reusing the same
	 * task instance throws {@link IllegalStateException}. Both are intentional
	 * behavior changes from silent re-run / {@link ClassCastException}.
	 */
	private int enqueueTasks(List<DBTask> tasks) {
		if (tasks == null) {
			throw new IllegalArgumentException("The task list cannot be null.");
		}
		if (tasks.isEmpty()) {
			return currentDemand();
		}
		synchronized (taskLists_) {
			checkPoolOpen();
			IdentityHashMap<DBTask, Boolean> uniqueTasks = new IdentityHashMap<>();
			for (DBTask task : tasks) {
				BasicDBTask basicTask = asBasicTask(task);
				if (uniqueTasks.put(task, Boolean.TRUE) != null || !basicTask.isPending()) {
					throw new IllegalStateException("A DBTask instance can only be submitted once.");
				}
			}
			List<BasicDBTask> reservedTasks = new ArrayList<>();
			try {
				for (DBTask task : tasks) {
					BasicDBTask basicTask = asBasicTask(task);
					if (!basicTask.reserveSubmission()) {
						throw new IllegalStateException("A DBTask instance can only be submitted once.");
					}
					reservedTasks.add(basicTask);
				}
			} catch (RuntimeException e) {
				for (BasicDBTask reservedTask : reservedTasks) {
					reservedTask.releaseSubmission();
				}
				throw e;
			}
			unfinishedTasks_.addAll(tasks);
			taskLists_.addAll(tasks);
			int demand = taskLists_.size() + runningTaskCount_;
			taskLists_.notifyAll();
			return demand;
		}
	}

	private void handleIncompleteTask(BasicDBTask task, String message, boolean timedOut) {
		AsyncWorker owner;
		boolean removedFromQueue;
		synchronized (taskLists_) {
			if (task.isFinished()) {
				return;
			}
			removedFromQueue = taskLists_.remove(task);
			owner = removedFromQueue ? null : runningTasks_.get(task);
			boolean transitioned = timedOut ? task.markTimedOut(message) : task.markFailed(message);
			if (!transitioned) {
				return;
			}
			if (removedFromQueue) {
				task.markExecutionSkipped();
			}
			if (owner != null) {
				owner.retiring_ = true;
			}
		}

		if (removedFromQueue) {
			markTaskPhysicallyFinished(task);
			return;
		}
		if (owner == null) {
			log.warn("Timed-out task was neither queued nor owned by a worker.");
			task.markExecutionSkipped();
			markTaskPhysicallyFinished(task);
			return;
		}

		final AsyncWorker timedOutOwner = owner;
		try {
			cancellationExecutor_.execute(new Runnable() {
				@Override
				public void run() {
					cancelAndRetire(timedOutOwner, task);
				}
			});
		} catch (RuntimeException e) {
			log.warn("Unable to schedule DolphinDB task cancellation; aborting worker connection.", e);
			if (stillOwnsTimedOutTask(timedOutOwner, task)) {
				timedOutOwner.conn_.abort();
			}
		}
	}

	private void cancelAndRetire(AsyncWorker owner, BasicDBTask task) {
		if (!stillOwnsTimedOutTask(owner, task)) {
			return;
		}
		boolean cancelRequested = cancelRunningTask(owner, task);
		boolean executionStopped = false;
		if (cancelRequested) {
			try {
				executionStopped = task.awaitExecution(CANCEL_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		if (!executionStopped && stillOwnsTimedOutTask(owner, task)) {
			owner.conn_.abort();
		}
	}

	private boolean stillOwnsTimedOutTask(AsyncWorker owner, BasicDBTask task) {
		synchronized (taskLists_) {
			return runningTasks_.get(task) == owner;
		}
	}

	/**
	 * Cancel every console job on the owner worker session. This is safe only
	 * while one worker runs at most one task: {@code cancelConsoleJob} is keyed
	 * by sessionId, not by this task's rootJobId.
	 */
	private boolean cancelRunningTask(AsyncWorker owner, BasicDBTask task) {
		DBConnection control = null;
		try {
			String sessionId = owner.conn_.getSessionID();
			if (sessionId == null || sessionId.isEmpty()) {
				return false;
			}
			long targetSessionId = Long.parseLong(sessionId);
			// Management scripts use DolphinDB syntax even when worker tasks run in Python
			// mode. isUrgent=true so the cancel script can still be admitted when the
			// node is at maxConnections / license. Do not copy initialScript_, HA or
			// load-balance: the control connection must land on the owner worker node.
			control = new DBConnection(false, useSSL_, compress_, false, true);
			DBConnection.ConnectConfig config = DBConnection.ConnectConfig.builder()
					.hostName(owner.conn_.getHostName())
					.port(owner.conn_.getPort())
					.userId(uid_)
					.password(pwd_)
					.connectTimeout(CANCEL_CONNECT_TIMEOUT_MS)
					.readTimeout(CANCEL_READ_TIMEOUT_MS)
					.enableHighAvailability(false)
					.enableLoadBalance(false)
					.build();
			if (!control.connect(config)) {
				return false;
			}

			for (int attempt = 0; attempt < 3; attempt++) {
				if (task.awaitExecution(50, TimeUnit.MILLISECONDS)) {
					return true;
				}
				String script = "jobs = exec rootJobId from getConsoleJobs() where sessionId = " + targetSessionId + "\n"
						+ "if (size(jobs)) cancelConsoleJob(jobs)\n"
						+ "size(jobs)";
				Entity cancelledJobCount = control.run(script);
				int cancelled = 0;
				if (cancelledJobCount instanceof BasicInt) {
					cancelled = ((BasicInt) cancelledJobCount).getInt();
				} else {
					try {
						cancelled = Integer.parseInt(cancelledJobCount.getString());
					} catch (NumberFormatException ignored) {
						cancelled = 0;
					}
				}
				if (cancelled > 0) {
					return true;
				}
				if (attempt < 2) {
					Thread.sleep(50);
				}
			}
			return false;
		} catch (Exception e) {
			log.warn("Failed to cancel timed-out DolphinDB task: " + e.getMessage());
			return false;
		} finally {
			if (control != null) {
				control.close();
			}
		}
	}

	private int currentDemand() {
		synchronized (taskLists_) {
			return Math.max(minimumPoolSize_, taskLists_.size() + runningTaskCount_);
		}
	}

	public void waitForThreadCompletion() {
		waitForTasksRespectingInterrupt();
	}

	public int getConnectionCount() {
		return getCurrentConnectionCount();
	}

	public int getCurrentConnectionCount() {
		synchronized (workersLock_) {
			return workers_.size();
		}
	}

	/**
	 * Get the minimum number of connections kept by this pool.
	 */
	public int getMinimumPoolSize() {
		return minimumPoolSize_;
	}

	/**
	 * Get the maximum number of connections allowed by this pool.
	 */
	public int getMaximumPoolSize() {
		return maximumPoolSize_;
	}

	/**
	 * Get the idle timeout in milliseconds.
	 */
	public int getIdleTimeout() {
		return idleTimeout_;
	}

	/**
	 * Get the number of tasks currently waiting for a worker.
	 */
	int getPendingTaskCount() {
		synchronized (taskLists_) {
			return taskLists_.size();
		}
	}

	/**
	 * Get the number of connections currently executing tasks.
	 */
	public int getActiveConnectionsCount() {
		checkPoolOpen();
		synchronized (workersLock_) {
			return getActiveConnectionsCountInternal();
		}
	}

	/**
	 * Get the number of live connections currently not executing tasks.
	 */
	public int getIdleConnectionsCount() {
		checkPoolOpen();
		synchronized (workersLock_) {
			return workers_.size() - getActiveConnectionsCountInternal();
		}
	}

	/**
	 * Stop accepting new tasks, drain all previously accepted tasks without a
	 * timeout, and then stop the worker connections. An interrupt on the caller
	 * stops the drain wait, fails remaining accepted tasks so their waiters are
	 * released, gives in-flight cancel work a bounded window (then aborts
	 * leftover sockets), and interrupts workers so shutdown cannot hang forever.
	 */
	public void shutdown() {
		beginShutdown();
		if (!waitForTasksRespectingInterrupt()) {
			failRemainingAcceptedTasks("ExclusiveDBConnectionPool shutdown wait was interrupted.");
			drainCancellationExecutorThenAbort();
		} else {
			cancellationExecutor_.shutdownNow();
		}
		interruptWorkers();
	}

	private void drainCancellationExecutorThenAbort() {
		cancellationExecutor_.shutdown();
		boolean terminated;
		try {
			terminated = cancellationExecutor_.awaitTermination(
					CANCEL_CONNECT_TIMEOUT_MS + CANCEL_READ_TIMEOUT_MS + CANCEL_GRACE_PERIOD_MS,
					TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			terminated = false;
		}
		if (!terminated) {
			cancellationExecutor_.shutdownNow();
			abortLiveWorkers();
		}
	}

	private void abortLiveWorkers() {
		List<AsyncWorker> workers = getWorkersSnapshot();
		for (int i = 0; i < workers.size(); i++) {
			workers.get(i).conn_.abort();
		}
	}

	private void beginShutdown() {
		isShutdown_ = true;
		synchronized (taskLists_) {
			taskLists_.notifyAll();
		}
	}

	private void interruptWorkers() {
		List<AsyncWorker> workers = getWorkersSnapshot();
		for (AsyncWorker worker : workers) {
			worker.workThread_.interrupt();
		}
	}

	private boolean waitForTasksRespectingInterrupt() {
		synchronized (taskLists_) {
			log.info("Waiting for tasks to complete, remain Task: " + unfinishedTasks_.size());
			while (!unfinishedTasks_.isEmpty()) {
				try {
					taskLists_.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.warn("ExclusiveDBConnectionPool wait interrupted with "
							+ unfinishedTasks_.size() + " remaining tasks.");
					return false;
				}
			}
			return true;
		}
	}

	private void failRemainingAcceptedTasks(String message) {
		List<DBTask> remaining;
		synchronized (taskLists_) {
			remaining = new ArrayList<DBTask>(unfinishedTasks_);
		}
		for (int i = 0; i < remaining.size(); i++) {
			DBTask task = remaining.get(i);
			if (task instanceof BasicDBTask) {
				handleIncompleteTask((BasicDBTask) task, message, false);
			}
		}
	}

	private List<AsyncWorker> getWorkersSnapshot() {
		synchronized (workersLock_) {
			return new ArrayList<>(workers_);
		}
	}

	private int getActiveConnectionsCountInternal() {
		int count = 0;
		for (AsyncWorker worker : workers_) {
			if (worker.active_) {
				count++;
			}
		}
		return count;
	}

	private void checkPoolOpen() {
		if (isShutdown_) {
			throw new RuntimeException("The connection pool has been closed.");
		}
	}

	private static ExclusiveDBConnectionPoolConfig createConfig(String host, int port, String uid, String pwd, int minimumPoolSize, int maximumPoolSize, int idleTimeout, boolean loadBalance, boolean enableHighAvailability, String[] highAvailabilitySites, String initialScript, boolean compress, boolean useSSL, boolean usePython) {
		ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
		config.setHostName(host);
		config.setPort(port);
		config.setUserId(uid);
		config.setPassword(pwd);
		config.setMinimumPoolSize(minimumPoolSize);
		config.setMaximumPoolSize(maximumPoolSize);
		config.setIdleTimeout(idleTimeout);
		config.setLoadBalance(loadBalance);
		config.setEnableHighAvailability(enableHighAvailability);
		config.setHighAvailabilitySites(highAvailabilitySites);
		config.setInitialScript(initialScript);
		config.setCompress(compress);
		config.setUseSSL(useSSL);
		config.setUsePython(usePython);
		return config;
	}

	private void initPool(ExclusiveDBConnectionPoolConfig config) throws IOException {
		config.validate();
		initPool(config.getHostName(), config.getPort(), config.getUserId(), config.getPassword(), config.getMinimumPoolSize(), config.getMaximumPoolSize(), config.getIdleTimeout(), config.isLoadBalance(), config.isEnableHighAvailability(), config.getHighAvailabilitySites(), config.getInitialScript(), config.isCompress(), config.isUseSSL(), config.isUsePython(), true);
	}

	private void initPool(String host, int port, String uid, String pwd, int minimumPoolSize, int maximumPoolSize, int idleTimeout, boolean loadBalance, boolean enableHighAvailability, String[] highAvailabilitySites, String initialScript, boolean compress, boolean useSSL, boolean usePython, boolean dynamicPool) throws IOException {
		this.host_ = host;
		this.port_ = port;
		this.uid_ = uid;
		this.pwd_ = pwd;
		this.minimumPoolSize_ = minimumPoolSize;
		this.maximumPoolSize_ = maximumPoolSize;
		this.idleTimeout_ = idleTimeout;
		this.loadBalance_ = loadBalance;
		this.enableHighAvailability_ = enableHighAvailability;
		this.highAvailabilitySites_ = highAvailabilitySites;
		this.initialScript_ = initialScript;
		this.compress_ = compress;
		this.useSSL_ = useSSL;
		this.usePython_ = usePython;
		this.dynamicPool_ = dynamicPool;
		initHosts();
		initCancellationExecutor();
		for (int i = 0; i < minimumPoolSize_; ++i) {
			addInitialWorker();
		}
	}

	private void initCancellationExecutor() {
		final int poolIndex = POOL_INDEX.incrementAndGet();
		final AtomicInteger threadIndex = new AtomicInteger();
		ThreadFactory threadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				Thread thread = new Thread(runnable,
						"ExclusiveDBConnectionPool-Cancel-" + poolIndex + "-" + threadIndex.incrementAndGet());
				thread.setDaemon(true);
				return thread;
			}
		};
		cancellationExecutor_ = new ThreadPoolExecutor(
				CANCEL_THREAD_COUNT,
				CANCEL_THREAD_COUNT,
				0L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(Math.max(1, maximumPoolSize_)),
				threadFactory,
				new ThreadPoolExecutor.AbortPolicy());
	}

	private void initHosts() throws IOException {
		if (!loadBalance_) {
			this.hosts_ = new String[] { host_ };
			this.ports_ = new int[] { port_ };
			return;
		}
		BasicStringVector nodes = null;
		if (highAvailabilitySites_ != null) {
			nodes = new BasicStringVector(highAvailabilitySites_);
		} else {
			DBConnection entryPoint = new DBConnection(false, useSSL_, compress_, usePython_);
			try {
				if(!entryPoint.connect(host_, port_, uid_, pwd_))
					throw new RuntimeException("Can't connect to the specified host.");
				nodes = (BasicStringVector)entryPoint.run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
			} finally {
				entryPoint.close();
			}
		}
		int nodeCount = nodes.rows();
		this.hosts_ = new String[nodeCount];
		this.ports_ = new int[nodeCount];
		for (int i=0; i<nodeCount; ++i) {
			String[] fields = nodes.getString(i).split(":");
			if(fields.length < 2)
				throw new RuntimeException("Invalid data node address: " + nodes.getString(i));
			hosts_[i] = fields[0];
			ports_[i] = Integer.parseInt(fields[1]);
		}
	}

	private DBConnection createConnection(int workerIndex) throws IOException {
		String host;
		int port;
		if (!loadBalance_) {
			host = host_;
			port = port_;
		} else {
			int targetIndex = workerIndex % hosts_.length;
			host = hosts_[targetIndex];
			port = ports_[targetIndex];
		}
		DBConnection conn = new DBConnection(false, useSSL_, compress_, usePython_);
		try {
			connectWorkerUnbounded(conn, host, port);
			return conn;
		} catch (IOException e) {
			closeFailedConnection(conn);
			throw e;
		} catch (RuntimeException e) {
			closeFailedConnection(conn);
			throw e;
		}
	}

	private DBConnection createConnection(int workerIndex, int connectTimeoutMs,
			int readTimeoutMs, int tryReconnectNums) throws IOException {
		String host;
		int port;
		if (!loadBalance_) {
			host = host_;
			port = port_;
		} else {
			int targetIndex = workerIndex % hosts_.length;
			host = hosts_[targetIndex];
			port = ports_[targetIndex];
		}
		DBConnection conn = new DBConnection(false, useSSL_, compress_, usePython_);
		try {
			connectWorkerBounded(conn, host, port, connectTimeoutMs, readTimeoutMs,
					tryReconnectNums);
			return conn;
		} catch (IOException e) {
			closeFailedConnection(conn);
			throw e;
		} catch (RuntimeException e) {
			closeFailedConnection(conn);
			throw e;
		}
	}

	private static void closeFailedConnection(DBConnection conn) {
		try {
			conn.close();
		} catch (Exception ignored) {
		}
	}

	/**
	 * Initial workers keep the historical connect contract: unlimited HA
	 * reconnect, no connect/read timeout, and the original exception types.
	 */
	private void connectWorkerUnbounded(DBConnection conn, String host, int port) throws IOException {
		if (!loadBalance_) {
			try {
				boolean isConnected = conn.connect(host_, port_, uid_, pwd_, initialScript_,
						enableHighAvailability_, highAvailabilitySites_, false, loadBalance_);
				if (!isConnected) {
					throw new RuntimeException("Can't connect to the specified host.");
				}
			} catch (IOException e) {
				if (dynamicPool_) {
					throw e;
				}
				throw new RuntimeException("Can't connect to the specified host: ", e);
			}
			return;
		}
		if (!conn.connect(host, port, uid_, pwd_, initialScript_, enableHighAvailability_,
				highAvailabilitySites_, false, false)) {
			throw new RuntimeException("Can't connect to the host " + host + ":" + port);
		}
	}

	/**
	 * Background rebuild / submit-path expansion must not hang in HA
	 * {@code connect()}. Submit path passes an explicit handshake
	 * {@code readTimeoutMs} equal to the remaining execute budget; rebuild
	 * passes {@code 0} so {@code initialScript} / HA {@code getClusterPerf}
	 * are not capped. After success, {@link DBConnection#preparePublishedWorker}
	 * restores blocking reads, unlimited reconnect, and the pool's 2s
	 * background {@code connectTimeout_}. Restore failure is a failed connect.
	 */
	private void connectWorkerBounded(DBConnection conn, String host, int port,
			int connectTimeoutMs, int readTimeoutMs, int tryReconnectNums) throws IOException {
		try {
			DBConnection.ConnectConfig config = DBConnection.ConnectConfig.builder()
					.hostName(host)
					.port(port)
					.userId(uid_)
					.password(pwd_)
					.initialScript(initialScript_)
					.enableHighAvailability(enableHighAvailability_)
					.highAvailabilitySites(highAvailabilitySites_)
					.enableLoadBalance(false)
					.reconnect(false)
					.connectTimeout(connectTimeoutMs)
					.readTimeout(readTimeoutMs)
					.tryReconnectNums(tryReconnectNums)
					.build();
			if (!conn.connect(config)) {
				throw new RuntimeException("Can't connect to the host " + host + ":" + port);
			}
			conn.preparePublishedWorker(WORKER_CONNECT_TIMEOUT_MS);
		} catch (IOException e) {
			if (dynamicPool_ && !loadBalance_) {
				throw e;
			}
			throw new RuntimeException("Can't connect to the host " + host + ":" + port, e);
		}
	}

	private void addInitialWorker() throws IOException {
		int workerIndex = nextWorkerIndex_++;
		AsyncWorker worker = new AsyncWorker(createConnection(workerIndex), workerIndex + 1, dynamicPool_);
		synchronized (workersLock_) {
			workers_.add(worker);
		}
		worker.start();
	}

	private void ensureWorkerCapacity(int demand, boolean retry) {
		ensureWorkerCapacity(demand, retry, WORKER_CONNECT_TIMEOUT_MS, 0,
				WORKER_CONNECT_TRY_RECONNECT_NUMS);
	}

	private void ensureWorkerCapacity(int demand, boolean retry, int connectTimeoutMs,
			int readTimeoutMs, int tryReconnectNums) {
		int maxAttempts = retry ? WORKER_CREATE_MAX_ATTEMPTS : 1;
		while (true) {
			int workerIndex;
			int desiredWorkerCount = desiredWorkerCount(demand);
			if (desiredWorkerCount <= 0) {
				return;
			}
			synchronized (workersLock_) {
				int plannedWorkerCount = workers_.size() + creatingWorkerCount_;
				if (plannedWorkerCount >= desiredWorkerCount) {
					return;
				}
				creatingWorkerCount_++;
				workerIndex = nextWorkerIndex_++;
			}
			AsyncWorker worker = null;
			try {
				worker = createAndStartWorker(workerIndex, maxAttempts, connectTimeoutMs,
						readTimeoutMs, tryReconnectNums);
			} finally {
				boolean poolEmpty;
				synchronized (workersLock_) {
					creatingWorkerCount_--;
					poolEmpty = workers_.isEmpty() && creatingWorkerCount_ == 0;
				}
				if (worker == null && poolEmpty) {
					failQueuedTasksBecauseNoWorkers();
				}
			}
			if (worker == null) {
				return;
			}
		}
	}

	private int desiredWorkerCount(int demand) {
		if (isShutdown_) {
			synchronized (taskLists_) {
				int drainDemand = taskLists_.size() + runningTaskCount_;
				if (drainDemand <= 0) {
					return 0;
				}
				return Math.min(maximumPoolSize_, drainDemand);
			}
		}
		return Math.min(maximumPoolSize_, Math.max(minimumPoolSize_, demand));
	}

	private AsyncWorker createAndStartWorker(int workerIndex, int maxAttempts) {
		return createAndStartWorker(workerIndex, maxAttempts, WORKER_CONNECT_TIMEOUT_MS, 0,
				WORKER_CONNECT_TRY_RECONNECT_NUMS);
	}

	private AsyncWorker createAndStartWorker(int workerIndex, int maxAttempts,
			int connectTimeoutMs, int readTimeoutMs, int tryReconnectNums) {
		AsyncWorker worker = createWorker(workerIndex, maxAttempts, connectTimeoutMs,
				readTimeoutMs, tryReconnectNums);
		if (worker == null) {
			return null;
		}
		boolean rejectIdleShutdownWorker = false;
		if (isShutdown_) {
			synchronized (taskLists_) {
				rejectIdleShutdownWorker = taskLists_.size() + runningTaskCount_ <= 0;
			}
		}
		synchronized (workersLock_) {
			if (workers_.size() >= maximumPoolSize_ || rejectIdleShutdownWorker) {
				worker.conn_.close();
				return null;
			}
			workers_.add(worker);
		}
		worker.start();
		return worker;
	}

	private AsyncWorker createWorker(int workerIndex, int maxAttempts, int connectTimeoutMs,
			int readTimeoutMs, int tryReconnectNums) {
		// Sleep backoff is interruptible. If this thread is already interrupted
		// (external interrupt of a dying worker), only the first attempt runs.
		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			try {
				return new AsyncWorker(
						createConnection(workerIndex, connectTimeoutMs, readTimeoutMs,
								tryReconnectNums),
						workerIndex + 1, dynamicPool_);
			} catch (Exception e) {
				log.error("Failed to create new ExclusiveDBConnectionPool worker: " + e.getMessage());
				if (isShutdown_ || attempt >= maxAttempts) {
					return null;
				}
				try {
					Thread.sleep((long) WORKER_CREATE_BACKOFF_MS * attempt);
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return null;
				}
			}
		}
		return null;
	}

	private void rejectIfNoLiveWorkers() {
		if (hasLiveOrCreatingWorkers()) {
			return;
		}
		failQueuedTasksBecauseNoWorkers();
		if (hasLiveOrCreatingWorkers()) {
			return;
		}
		throw new RuntimeException("ExclusiveDBConnectionPool has no live connections.");
	}

	private boolean hasLiveOrCreatingWorkers() {
		synchronized (workersLock_) {
			return !workers_.isEmpty() || creatingWorkerCount_ > 0;
		}
	}

	private void failQueuedTasksBecauseNoWorkers() {
		synchronized (taskLists_) {
			synchronized (workersLock_) {
				if (!workers_.isEmpty() || creatingWorkerCount_ > 0) {
					return;
				}
			}
			List<DBTask> queued = new ArrayList<DBTask>(taskLists_);
			boolean emptied = false;
			for (int i = 0; i < queued.size(); i++) {
				DBTask task = queued.get(i);
				if (!(task instanceof BasicDBTask)) {
					continue;
				}
				if (!taskLists_.remove(task)) {
					continue;
				}
				BasicDBTask basicTask = (BasicDBTask) task;
				if (!basicTask.markFailed("ExclusiveDBConnectionPool has no live connections.")) {
					continue;
				}
				basicTask.markExecutionSkipped();
				if (unfinishedTasks_.remove(task) && unfinishedTasks_.isEmpty()) {
					emptied = true;
				}
			}
			if (emptied) {
				taskLists_.notifyAll();
			}
		}
	}

	private boolean tryRetireWorker(AsyncWorker worker) {
		synchronized (workersLock_) {
			if (workers_.size() <= minimumPoolSize_) {
				return false;
			}
			workers_.remove(worker);
			return true;
		}
	}

	private void retireOrRemoveWorker(AsyncWorker worker) {
		int reservedIndex;
		synchronized (workersLock_) {
			workers_.remove(worker);
			// Occupy a create slot before close() so concurrent execute() never
			// observes workers_.isEmpty() && creatingWorkerCount_==0 while this
			// thread is still going to rebuild.
			creatingWorkerCount_++;
			reservedIndex = nextWorkerIndex_++;
		}
		AsyncWorker replacement = null;
		boolean poolEmpty;
		try {
			worker.conn_.close();
			log.info("ExclusiveDBConnectionPool AsyncWorker terminated peacefully.");
			int desired = desiredWorkerCount(currentDemand());
			boolean needsRebuild;
			synchronized (workersLock_) {
				// creatingWorkerCount_ already includes this thread's reservation.
				needsRebuild = desired > 0
						&& (workers_.size() + creatingWorkerCount_ - 1) < desired;
			}
			if (needsRebuild) {
				replacement = createAndStartWorker(reservedIndex, WORKER_CREATE_MAX_ATTEMPTS);
			}
		} finally {
			synchronized (workersLock_) {
				creatingWorkerCount_--;
				poolEmpty = workers_.isEmpty() && creatingWorkerCount_ == 0;
			}
		}
		if (replacement == null && poolEmpty) {
			failQueuedTasksBecauseNoWorkers();
		} else {
			ensureWorkerCapacity(currentDemand(), true);
		}
	}
}
