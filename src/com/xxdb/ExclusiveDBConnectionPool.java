package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.xxdb.data.BasicStringVector;


public class ExclusiveDBConnectionPool implements DBConnectionPool{
	private List<DBConnection> conns;
	private List<AsynWorker> workers_ = new ArrayList<>();
	private final LinkedList<DBTask> taskList_ = new LinkedList<>();
	private int taskcount = 0;
	private Object lock = new Object();
	private int FinishedTaskCount = 0;

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
				synchronized (taskList_) {
					if (taskList_.size() == 0) {
						try {
							taskList_.wait();
						} catch (Exception e) {
							break;
						}
					}
				}

				while (true){
					synchronized (taskList_){
						task = taskList_.poll();
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
					synchronized (lock){
						FinishedTaskCount++;
					}
				}

				synchronized (lock){
					lock.notify();
				}
			}
			conn_.close();
		}
	}

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability) throws IOException{
		this(host, port, uid, pwd, count, loadBalance, enableHighAvailability, false);
	}
	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, boolean compress) throws IOException{
		conns = new ArrayList<DBConnection>(count);
		if(!loadBalance){
			for(int i=0; i<count; ++i){
				DBConnection conn = new DBConnection(false, false, compress);
				if(!conn.connect(host, port, uid, pwd, enableHighAvailability))
					throw new RuntimeException("Can't connect to the specified host.");
				conns.add(conn);
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
				conns.add(conn);
				workers_.add(new AsynWorker(conn));
			}
		}
	}
	
	public void execute(List<DBTask> tasks){
		taskcount += tasks.size();
		synchronized (taskList_){
			taskList_.addAll(tasks);
			taskList_.notifyAll();
		}
	}
	
	public void execute(DBTask task){
		taskcount ++;
		synchronized (taskList_){
			taskList_.add(task);
			taskList_.notify();
		}
	}

	public void waitForThreadCompletion(){
		try {
			synchronized (lock){
				while (FinishedTaskCount >= 0){
					if (FinishedTaskCount < taskcount){
						lock.wait();
					}else if (FinishedTaskCount == taskcount){
						break;
					}
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public int getConnectionCount(){
		return conns.size();
	}
	
	public void shutdown(){
		try{
			for (AsynWorker one : workers_){
				synchronized (one.workThread_){
					one.workThread_.interrupt();
				}
			}
			conns.clear();
		}
		catch(Throwable t){
			throw new RuntimeException(t);
		}
	}
}
