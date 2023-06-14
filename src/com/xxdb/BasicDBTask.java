package com.xxdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;

public class BasicDBTask implements DBTask {
	private String script;
	private List<Entity> args;
	private DBConnection conn;
	private Entity result = null;
	private String errMsg = null;
	private TaskStatus status = TaskStatus.PENDING;
	private CountDownLatch latch;
	private int timeOut = -1;

	public BasicDBTask(String script, List<Entity> args) {
		this.script = script;
		this.args = args;
		latch = new CountDownLatch(1);
	}

	public BasicDBTask(String script) {
		this(script, null);
	}

	@Override
	public Entity call() {
		try {
			if (args != null)
				result = conn.run(script, args);
			else
				result = conn.run(script);
			errMsg = null;
			synchronized (this) {
				status = TaskStatus.SUCCESS;
			}
		} catch (Exception t) {
			synchronized (this) {
				status = TaskStatus.FAILED;
			}
			result = null;
			errMsg = t.getMessage();
		} finally {
			latch.countDown();
		}
		return result;
	}

	@Override
	public void setDBConnection(DBConnection conn) {
		this.conn = conn;
	}

	@Override
	public Entity getResult() {
		if (status != TaskStatus.SUCCESS) {
			throw new RuntimeException("Current status is: " + status + "!");
		} else {
			return result;
		}
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
		return status != TaskStatus.PENDING;
	}

	public void waitFor(int timeOut) {
		try {
			if (timeOut >= 0) {
				boolean completed = latch.await(timeOut, TimeUnit.MILLISECONDS);
				if (!completed) {
					synchronized (this) {
						if (status == TaskStatus.PENDING) {
							DBConnection connection = new DBConnection();
							connection.connect(conn.getHostName(), conn.getPort(), conn.getUserID(), conn.getPwd());
							String sessionId = connection.getSessionID();
							BasicStringVector bs = (BasicStringVector) connection.run("exec rootJobId from getConsoleJobs() where sessionId = " + sessionId);
							List<Entity> arguments = new ArrayList<>();
							arguments.add(bs);
							connection.run("cancelConsoleJob", arguments);
						}
					}
				}
			} else {
				latch.await();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void finish() {
		latch.countDown();
	}

	private enum TaskStatus {
		PENDING,
		SUCCESS,
		FAILED
	}
}
