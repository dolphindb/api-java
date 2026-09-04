package com.xxdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import com.xxdb.data.Void;

public class BasicDBTask implements DBTask {
	private enum Kind {
		LEGACY,
		SCRIPT,
		FUNCTION,
		UPLOAD
	}

	private String script;
	private List<Entity> args;
	private Map<String, Entity> uploadVars;
	private DBConnection.RunOptions runOptions;
	private DBConnection.UploadOptions uploadOptions;
	private Kind kind = Kind.LEGACY;
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

	public static BasicDBTask forScript(String script, DBConnection.RunOptions options) {
		BasicDBTask task = new BasicDBTask(script, null);
		task.kind = Kind.SCRIPT;
		task.runOptions = options;
		return task;
	}

	public static BasicDBTask forFunction(String function, List<Entity> args, DBConnection.RunOptions options) {
		if (function == null)
			throw new IllegalArgumentException("function cannot be null");
		if (args == null)
			throw new IllegalArgumentException("arguments cannot be null");
		BasicDBTask task = new BasicDBTask(function, args);
		task.kind = Kind.FUNCTION;
		task.runOptions = options;
		return task;
	}

	public static BasicDBTask forUpload(Map<String, Entity> variables, DBConnection.UploadOptions options) {
		if (variables == null)
			throw new IllegalArgumentException("variables cannot be null");
		BasicDBTask task = new BasicDBTask(joinVariableNames(variables), null);
		task.kind = Kind.UPLOAD;
		task.uploadVars = variables;
		task.uploadOptions = options;
		return task;
	}

	private static String joinVariableNames(Map<String, Entity> variables) {
		if (variables == null || variables.isEmpty())
			return "";
		StringBuilder names = new StringBuilder();
		Iterator<String> it = variables.keySet().iterator();
		while (it.hasNext()) {
			if (names.length() > 0)
				names.append(",");
			names.append(it.next());
		}
		return names.toString();
	}

	@Override
	public Entity call() {
		try {
			if (kind == Kind.UPLOAD) {
				if (uploadOptions != null)
					conn.upload(uploadVars, uploadOptions);
				else
					conn.upload(uploadVars);
				result = new Void();
			} else if (kind == Kind.SCRIPT) {
				result = conn.runWithOptions(script, runOptions);
			} else if (kind == Kind.FUNCTION) {
				result = conn.runWithOptions(script, args, runOptions);
			} else if (args != null) {
				result = conn.run(script, args);
			} else {
				result = conn.run(script);
			}
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
			if (timeOut > 0) {
				boolean completed = latch.await(timeOut, TimeUnit.MILLISECONDS);
				if (!completed) {
					synchronized (this) {
						if (status == TaskStatus.PENDING) {
							DBConnection connection = new DBConnection();
							connection.connect(conn.getHostName(), conn.getPort(), conn.getUserId(), conn.getPwd());
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
