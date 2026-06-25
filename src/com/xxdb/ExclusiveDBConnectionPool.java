package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import com.xxdb.data.BasicStringVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExclusiveDBConnectionPool implements DBConnectionPool {
	private final List<AsyncWorker> workers_ = new ArrayList<>();
	private final Object workersLock_ = new Object();
	private final LinkedList<DBTask> taskLists_ = new LinkedList<>();
	private int tasksCount_ = 0;
	private int runningTaskCount_ = 0;
	private final Object finishedTasklock_ = new Object();
	private int finishedTaskCount_ = 0;
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

	private static final Logger log = LoggerFactory.getLogger(ExclusiveDBConnectionPool.class);

	private class AsyncWorker implements Runnable {
		private final DBConnection conn_;
		private final Thread workThread_;
		private final boolean dynamicWorker_;
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
				while (!workThread_.isInterrupted()) {
					DBTask task = null;
					synchronized (taskLists_) {
						if (taskLists_.size() == 0) {
							try {
								taskLists_.wait();
							} catch (InterruptedException e) {
								break;
							}
						}
					}
					while (true) {
						synchronized (taskLists_) {
							task = taskLists_.pollLast();
						}
						if (task == null) {
							break;
						}
						try {
							task.setDBConnection(conn_);
							task.call();
						} catch (InterruptedException e) {
							break;
						} catch (Exception e) {
							e.printStackTrace();
						}
						((BasicDBTask)task).finish();
						synchronized (finishedTasklock_) {
							finishedTaskCount_++;
						}
					}
					synchronized (finishedTasklock_) {
						finishedTasklock_.notify();
					}
				}
			} finally {
				conn_.close();
				log.info("ExclusiveDBConnectionPool AsyncWorker terminated peacefully.");
			}
		}

		private void runDynamicWorker() {
			try {
				while (!workThread_.isInterrupted()) {
					DBTask task = null;
					synchronized (taskLists_) {
						while (taskLists_.size() == 0 && !workThread_.isInterrupted()) {
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
						if (workThread_.isInterrupted()) {
							break;
						}
						task = taskLists_.pollLast();
						if (task != null) {
							runningTaskCount_++;
						}
					}
					if (task == null) {
						continue;
					}
					boolean shouldFinish = true;
					try {
						task.setDBConnection(conn_);
						task.call();
					} catch (InterruptedException e) {
						shouldFinish = false;
						workThread_.interrupt();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						synchronized (taskLists_) {
							runningTaskCount_--;
							lastUsedTime_ = System.currentTimeMillis();
						}
						if (shouldFinish) {
							((BasicDBTask)task).finish();
							synchronized (finishedTasklock_) {
								finishedTaskCount_++;
								finishedTasklock_.notifyAll();
							}
						}
					}
				}
			} finally {
				removeWorker(this);
				conn_.close();
				log.info("ExclusiveDBConnectionPool AsyncWorker terminated peacefully.");
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
		if (!dynamicPool_) {
			synchronized (taskLists_) {
				tasksCount_ += tasks.size();
				taskLists_.addAll(tasks);
				taskLists_.notifyAll();
			}
			for (DBTask task : tasks) {
				((BasicDBTask)task).waitFor(-1);
			}
			return;
		}
		int demand;
		synchronized (taskLists_) {
			tasksCount_ += tasks.size();
			taskLists_.addAll(tasks);
			demand = taskLists_.size() + runningTaskCount_;
			taskLists_.notifyAll();
		}
		ensureWorkerCapacity(demand);
		synchronized (taskLists_) {
			taskLists_.notifyAll();
		}
		for (DBTask task : tasks) {
			((BasicDBTask)task).waitFor(-1);
		}
	}
	
	public void execute(DBTask task) {
		execute(task, -1);
	}

	public void execute(DBTask task, int timeOut) {
		if (!dynamicPool_) {
			synchronized (taskLists_){
				tasksCount_++;
				taskLists_.add(task);
				taskLists_.notify();
			}
			((BasicDBTask)task).waitFor(timeOut);
			return;
		}
		int demand;
		synchronized (taskLists_){
			tasksCount_++;
			taskLists_.add(task);
			demand = taskLists_.size() + runningTaskCount_;
			taskLists_.notify();
		}
		ensureWorkerCapacity(demand);
		synchronized (taskLists_) {
			taskLists_.notifyAll();
		}
		((BasicDBTask)task).waitFor(timeOut);
	}

	public void waitForThreadCompletion() {
		try {
			synchronized (finishedTasklock_) {
				log.info("Waiting for tasks to complete, remain Task: " + (tasksCount_-finishedTaskCount_));
				while (finishedTaskCount_ >= 0) {
					if (finishedTaskCount_ < tasksCount_) {
						finishedTasklock_.wait();
					} else if (finishedTaskCount_ == tasksCount_) {
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
	
	public void shutdown() {
		waitForThreadCompletion();
		if (dynamicPool_) {
			isShutdown_ = true;
		}
		List<AsyncWorker> workers = dynamicPool_ ? getWorkersSnapshot() : workers_;
		for (AsyncWorker one : workers) {
			synchronized (one.workThread_ ) {
				one.workThread_.interrupt();
			}
		}
	}

	private List<AsyncWorker> getWorkersSnapshot() {
		synchronized (workersLock_) {
			return new ArrayList<>(workers_);
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
		for (int i = 0; i < minimumPoolSize_; ++i) {
			addInitialWorker();
		}
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
		if (!loadBalance_) {
			DBConnection conn = new DBConnection(false, useSSL_, compress_, usePython_);
			try {
				boolean isConnected = conn.connect(host_, port_, uid_, pwd_, initialScript_, enableHighAvailability_, highAvailabilitySites_, false, loadBalance_);
				if (!isConnected) {
					throw new RuntimeException("Can't connect to the specified host.");
				}
			} catch (IOException e) {
				if (dynamicPool_) {
					throw e;
				}
				throw new RuntimeException("Can't connect to the specified host: ", e);
			}
			return conn;
		}
		int targetIndex = workerIndex % hosts_.length;
		DBConnection conn = new DBConnection(false, useSSL_, compress_, usePython_);
		if(!conn.connect(hosts_[targetIndex], ports_[targetIndex], uid_, pwd_, initialScript_, enableHighAvailability_, highAvailabilitySites_,false,false))
			throw new RuntimeException("Can't connect to the host " + hosts_[targetIndex] + ":" + ports_[targetIndex]);
		return conn;
	}

	private void addInitialWorker() throws IOException {
		int workerIndex = nextWorkerIndex_++;
		AsyncWorker worker = new AsyncWorker(createConnection(workerIndex), workerIndex + 1, dynamicPool_);
		synchronized (workersLock_) {
			workers_.add(worker);
		}
		worker.start();
	}

	private void ensureWorkerCapacity(int demand) {
		while (true) {
			int workerIndex;
			synchronized (workersLock_) {
				int plannedWorkerCount = workers_.size() + creatingWorkerCount_;
				int desiredWorkerCount = Math.min(maximumPoolSize_, Math.max(minimumPoolSize_, demand));
				if (isShutdown_ || plannedWorkerCount >= desiredWorkerCount) {
					return;
				}
				creatingWorkerCount_++;
				workerIndex = nextWorkerIndex_++;
			}
			AsyncWorker worker;
			try {
				worker = new AsyncWorker(createConnection(workerIndex), workerIndex + 1, true);
			} catch (Exception e) {
				synchronized (workersLock_) {
					creatingWorkerCount_--;
				}
				log.error("Failed to create new ExclusiveDBConnectionPool worker: " + e.getMessage());
				return;
			}
			synchronized (workersLock_) {
				creatingWorkerCount_--;
				if (isShutdown_ || workers_.size() >= maximumPoolSize_) {
					worker.conn_.close();
					return;
				}
				workers_.add(worker);
			}
			worker.start();
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

	private void removeWorker(AsyncWorker worker) {
		synchronized (workersLock_) {
			workers_.remove(worker);
		}
	}
}
