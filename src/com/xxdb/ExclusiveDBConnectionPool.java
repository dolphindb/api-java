package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.xxdb.data.BasicStringVector;


public class ExclusiveDBConnectionPool implements DBConnectionPool{
	private List<AsynWorker> workers_ = new ArrayList<>();
	private final LinkedList<DBTask> taskLists_ = new LinkedList<>();
	private int tasksCount_ = 0;
	private Object finishedTasklock_ = new Object();
	private int finishedTaskCount_ = 0;

	private class AsynWorker implements Runnable{
		private DBConnection conn_;
		private Thread workThread_;

		public AsynWorker(DBConnection conn){
			this.conn_ = conn;
			workThread_ = new Thread(this);
			workThread_.start();
		}

		@Override
		public void run() {
			while (true){
				DBTask task = null;
				synchronized (taskLists_) {
					if (taskLists_.size() == 0) {
						try {
							taskLists_.wait();
						} catch (Exception e) {
							break;
						}
					}
				}

				while (true){
					synchronized (taskLists_){
						task = taskLists_.pollLast();
					}
					if (task == null){
						break;
					}
					try {
						task.setDBConnection(conn_);
						task.call();
					}catch (Exception e){
						e.printStackTrace();
					}
					synchronized (finishedTasklock_){
						finishedTaskCount_++;
					}
				}

				synchronized (finishedTasklock_){
					finishedTasklock_.notify();
				}
			}
			conn_.close();
		}
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability) throws IOException{
		this(host, port, uid, pwd, count, loadBalance, enableHighAvailability, false);
	}
	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, boolean compress) throws IOException{
		if(!loadBalance){
			for(int i=0; i<count; ++i){
				DBConnection conn = new DBConnection(false, false, compress);
				if(!conn.connect(host, port, uid, pwd, enableHighAvailability))
					throw new RuntimeException("Can't connect to the specified host.");
				workers_.add(new AsynWorker(conn));
			}
		}
		else{
			DBConnection entryPoint = new DBConnection(false, false, compress);
			if(!entryPoint.connect(host, port, uid, pwd))
				throw new RuntimeException("Can't connect to the specified host.");
			BasicStringVector nodes = (BasicStringVector)entryPoint.run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
			int nodeCount = nodes.rows();
			String[] hosts = new String[nodeCount];
			int[] ports = new int[nodeCount];
			for(int i=0; i<nodeCount; ++i){
				String[] fields = nodes.getString(i).split(":");
				if(fields.length < 2)
					throw new RuntimeException("Invalid data node address: " + nodes.getString(i));
				hosts[i] = fields[0];
				ports[i] = Integer.parseInt(fields[1]);
			}
			
			for(int i=0; i<count; ++i){
				DBConnection conn = new DBConnection(false, false, compress);
				if(!conn.connect(hosts[i % nodeCount], ports[i % nodeCount], uid, pwd, enableHighAvailability))
					throw new RuntimeException("Can't connect to the host " + nodes.getString(i));
				workers_.add(new AsynWorker(conn));
			}
		}
	}
	
	public void execute(List<DBTask> tasks){
		tasksCount_ += tasks.size();
		synchronized (taskLists_){
			taskLists_.addAll(tasks);
			taskLists_.notifyAll();
		}
	}
	
	public void execute(DBTask task){
		tasksCount_++;
		synchronized (taskLists_){
			taskLists_.add(task);
			taskLists_.notify();
		}
	}

	public void waitForThreadCompletion(){
		try {
			synchronized (finishedTasklock_){
				while (finishedTaskCount_ >= 0){
					if (finishedTaskCount_ < tasksCount_){
						finishedTasklock_.wait();
					}else if (finishedTaskCount_ == tasksCount_){
						break;
					}
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public int getConnectionCount(){
		return workers_.size();
	}
	
	public void shutdown(){
		for (AsynWorker one : workers_){
			synchronized (one.workThread_){
				one.workThread_.interrupt();
			}
		}
	}
}
