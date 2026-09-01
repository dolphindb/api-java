package com.xxdb;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.xxdb.data.Entity;

public class BasicDBTask implements DBTask {
	private final String script;
	private final List<Entity> args;
	private volatile DBConnection conn;
	private volatile Entity result = null;
	private volatile String errMsg = null;
	private volatile TaskStatus status = TaskStatus.PENDING;
	private boolean submitted = false;
	private final CountDownLatch completionLatch = new CountDownLatch(1);
	private final CountDownLatch executionLatch = new CountDownLatch(1);

	public BasicDBTask(String script, List<Entity> args) {
		this.script = script;
		this.args = args;
	}

	public BasicDBTask(String script) {
		this(script, null);
	}

	@Override
	public Entity call() {
		Entity taskResult = null;
		String taskError = null;
		boolean successful = false;
		try {
			if (args != null)
				taskResult = conn.run(script, args);
			else
				taskResult = conn.run(script);
			successful = true;
		} catch (Exception t) {
			taskError = t.getMessage();
			if (taskError == null) {
				taskError = t.getClass().getName();
			}
		} finally {
			completeExecution(successful, taskResult, taskError);
		}
		return result;
	}

	@Override
	public void setDBConnection(DBConnection conn) {
		markRunning(conn);
	}

	@Override
	public Entity getResult() {
		if (status != TaskStatus.SUCCESS) {
			throw new RuntimeException("Current status is: " + status + "!");
		}
		return result;
	}

	@Override
	public String getErrorMsg() {
		return errMsg;
	}

	@Override
	public boolean isSuccessful() {
		return status == TaskStatus.SUCCESS;
	}

	@Override
	public String getScript() {
		return script;
	}

	@Override
	public boolean isFinished() {
		TaskStatus current = status;
		return current != TaskStatus.PENDING && current != TaskStatus.RUNNING;
	}

	/**
	 * Wait for the task to reach a caller-visible terminal state.
	 * <p>
	 * This method only waits. A timeout here does not put the task into
	 * {@link TaskStatus#TIMED_OUT} or cancel a Server job; the task can remain
	 * {@link TaskStatus#RUNNING} with {@code errMsg == null}. Only
	 * {@code ExclusiveDBConnectionPool.execute(task, timeout)} owns that
	 * transition.
	 *
	 * @param timeOut timeout in milliseconds; non-positive values wait indefinitely
	 */
	public void waitFor(int timeOut) {
		try {
			awaitCompletion(timeOut);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Return the current task status.
	 */
	public TaskStatus getStatus() {
		return status;
	}

	boolean awaitCompletion(int timeOut) throws InterruptedException {
		if (timeOut > 0) {
			return completionLatch.await(timeOut, TimeUnit.MILLISECONDS);
		}
		completionLatch.await();
		return true;
	}

	boolean awaitExecution(long timeOut, TimeUnit unit) throws InterruptedException {
		if (timeOut > 0) {
			return executionLatch.await(timeOut, unit);
		}
		executionLatch.await();
		return true;
	}

	synchronized boolean markRunning(DBConnection connection) {
		if (status != TaskStatus.PENDING) {
			return false;
		}
		conn = connection;
		status = TaskStatus.RUNNING;
		return true;
	}

	synchronized boolean markTimedOut(String message) {
		if (status != TaskStatus.PENDING && status != TaskStatus.RUNNING) {
			return false;
		}
		result = null;
		errMsg = message;
		status = TaskStatus.TIMED_OUT;
		completionLatch.countDown();
		return true;
	}

	synchronized boolean markFailed(String message) {
		if (status != TaskStatus.PENDING && status != TaskStatus.RUNNING) {
			return false;
		}
		result = null;
		errMsg = message;
		status = TaskStatus.FAILED;
		completionLatch.countDown();
		return true;
	}

	/**
	 * Count down the execution latch when a queued task is removed without
	 * {@link #call()}. Same latch as {@link #markExecutionFinished()}; the names
	 * record why the worker is done with the task.
	 */
	void markExecutionSkipped() {
		executionLatch.countDown();
	}

	/**
	 * Count down the execution latch after {@link #call()} returns, including
	 * abort-induced failures.
	 */
	void markExecutionFinished() {
		executionLatch.countDown();
	}

	boolean isPending() {
		return status == TaskStatus.PENDING;
	}

	synchronized boolean reserveSubmission() {
		if (submitted || status != TaskStatus.PENDING) {
			return false;
		}
		submitted = true;
		return true;
	}

	synchronized void releaseSubmission() {
		if (status == TaskStatus.PENDING) {
			submitted = false;
		}
	}

	/**
	 * Retained for source and binary compatibility. Completion is owned by the
	 * task state machine; calling this method before a terminal state must not
	 * make a pending task appear complete.
	 */
	public void finish() {
		if (isFinished()) {
			completionLatch.countDown();
		}
	}

	private synchronized void completeExecution(boolean successful, Entity taskResult, String taskError) {
		if (status != TaskStatus.PENDING && status != TaskStatus.RUNNING) {
			return;
		}
		if (successful) {
			result = taskResult;
			errMsg = null;
			status = TaskStatus.SUCCESS;
		} else {
			result = null;
			errMsg = taskError;
			status = TaskStatus.FAILED;
		}
		completionLatch.countDown();
	}

	public enum TaskStatus {
		PENDING,
		RUNNING,
		SUCCESS,
		FAILED,
		TIMED_OUT
	}
}
