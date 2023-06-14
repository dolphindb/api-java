package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.xxdb.data.BasicStringVector;


public class ExclusiveDBConnectionPool implements DBConnectionPool {
	private List<AsyncWorker> workers_ = new ArrayList<>();
	private final LinkedList<DBTask> taskLists_ = new LinkedList<>();
	private int tasksCount_ = 0;
	private final Object finishedTasklock_ = new Object();
	private int finishedTaskCount_ = 0;

	private class AsyncWorker implements Runnable {
		private DBConnection conn_;
		private final Thread workThread_;

		public AsyncWorker(DBConnection conn) {
			this.conn_ = conn;
			workThread_ = new Thread(this);
			workThread_.start();
		}

		@Override
		public void run() {
			while (!workThread_.isInterrupted()) {
				DBTask task = null;
				synchronized (taskLists_) {
					if (taskLists_.size() == 0) {
						try {
							taskLists_.wait();
						} catch (Exception e) {
							e.printStackTrace();
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
						 e.printStackTrace();
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
			conn_.close();
			System.out.println("workThread_ shutdown.");
		}
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability) throws IOException {
		this(host, port, uid, pwd, count, loadBalance, enableHighAvailability, null, "",false, false, false);
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, String[] highAvailabilitySites, String initialScript,boolean compress, boolean useSSL, boolean usePython) throws IOException {
		if (count <= 0)
			throw new RuntimeException("The thread count can not be less than 0");
		if (!loadBalance) {
			for (int i=0; i<count; ++i) {
				DBConnection conn = new DBConnection(false, useSSL, compress, usePython);
				conn.setLoadBalance(false);
				if(!conn.connect(host, port, uid, pwd, initialScript, enableHighAvailability, highAvailabilitySites))
					throw new RuntimeException("Can't connect to the specified host.");
				workers_.add(new AsyncWorker(conn));
			}
		} else {
			BasicStringVector nodes = null;
			if (highAvailabilitySites != null) {
				nodes = new BasicStringVector(highAvailabilitySites);
			} else {
				DBConnection entryPoint = new DBConnection(false, useSSL, compress, usePython);
				if(!entryPoint.connect(host, port, uid, pwd))
					throw new RuntimeException("Can't connect to the specified host.");
				nodes = (BasicStringVector)entryPoint.run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
				entryPoint.close();
			}
			int nodeCount = nodes.rows();;
			String[] hosts = new String[nodeCount];
			int[] ports = new int[nodeCount];
			for (int i=0; i<nodeCount; ++i) {
				String[] fields = nodes.getString(i).split(":");
				if(fields.length < 2)
					throw new RuntimeException("Invalid data node address: " + nodes.getString(i));
				hosts[i] = fields[0];
				ports[i] = Integer.parseInt(fields[1]);
			}
			for (int i=0; i<count; ++i) {
				DBConnection conn = new DBConnection(false, useSSL, compress, usePython);
				conn.setLoadBalance(false);
				if(!conn.connect(hosts[i % nodeCount], ports[i % nodeCount], uid, pwd, initialScript, enableHighAvailability, highAvailabilitySites))
					throw new RuntimeException("Can't connect to the host " + nodes.getString(i));
				workers_.add(new AsyncWorker(conn));
			}
		}
	}
	
	public void execute(List<DBTask> tasks) {
		synchronized (taskLists_) {
			tasksCount_ += tasks.size();
			taskLists_.addAll(tasks);
			taskLists_.notifyAll();
		}
		for (DBTask task : tasks) {
			((BasicDBTask)task).waitFor(-1);
			((BasicDBTask)task).finish();
		}
	}
	
	public void execute(DBTask task) {
		execute(task, -1);
	}

	public void execute(DBTask task, int timeOut) {
		synchronized (taskLists_){
			tasksCount_++;
			taskLists_.add(task);
			taskLists_.notify();
		}
		((BasicDBTask)task).waitFor(timeOut);
		((BasicDBTask)task).finish();
	}

	public void waitForThreadCompletion() {
		try {
			synchronized (finishedTasklock_) {
				System.out.println("Waiting for tasks to complete, remain Task: " + (tasksCount_-finishedTaskCount_));
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
		return workers_.size();
	}
	
	public void shutdown() {
		waitForThreadCompletion();
		for (AsyncWorker one : workers_) {
			synchronized (one.workThread_ ) {
				one.workThread_.interrupt();
			}
		}
	}
}
