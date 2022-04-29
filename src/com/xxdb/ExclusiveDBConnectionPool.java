package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;

public class ExclusiveDBConnectionPool implements DBConnectionPool{
	private List<DBConnection> conns;
	private ExecutorService executor;

	public ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability) throws IOException{
		conns = new ArrayList<DBConnection>(count);
		if(!loadBalance){
			for(int i=0; i<count; ++i){
				DBConnection conn = new DBConnection();
				if(!conn.connect(host, port, uid, pwd, enableHighAvailability))
					throw new RuntimeException("Can't connect to the specified host.");
				conns.add(conn);
			}
		}
		else{
			DBConnection entryPoint = new DBConnection();
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
				DBConnection conn = new DBConnection();
				if(!conn.connect(hosts[i % nodeCount], ports[i % nodeCount], uid, pwd, enableHighAvailability))
					throw new RuntimeException("Can't connect to the host " + nodes.getString(i));
				conns.add(conn);
			}
		}
		
		executor = Executors.newFixedThreadPool(count);
	}
	
	public void execute(List<DBTask> tasks){
		synchronized(executor){
			try{
				int taskCount = tasks.size();
				if(taskCount > conns.size())
					throw new RuntimeException("The number of tasks can't exceed the number of connections in the pool.");
				for(int i=0; i<taskCount; ++i)
					tasks.get(i).setDBConnection(conns.get(i));
				List<Future<Entity>> futures = executor.invokeAll(tasks);
				for(int i=0; i<taskCount; ++i)
					futures.get(i).get();
			}
			catch(InterruptedException ie){
				throw new RuntimeException(ie);
			}
			catch(ExecutionException ie){
				throw new RuntimeException(ie);
			}
		}
	}
	
	public void execute(DBTask task){
		try {
			synchronized(executor){
				task.setDBConnection(conns.get(0));
				task.call();
			}
		} 
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public int getConnectionCount(){
		return conns.size();
	}
	
	public void shutdown(){
		try{
			executor.shutdown();
			for(int i=0; i<conns.size(); ++i){
				conns.get(i).close();
			}
			conns.clear();
		}
		catch(Throwable t){
			throw new RuntimeException(t);
		}
	}
}
