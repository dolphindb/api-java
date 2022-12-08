package com.xxdb;

import java.util.List;
import java.util.concurrent.Semaphore;

import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;

public class BasicDBTask implements DBTask{
	private String script;
	private List<Entity> args;
	private DBConnection conn;
	private Entity result = null;
	private String errMsg = null;
	private boolean successful = false;
	private Semaphore semaphore = new Semaphore(1);
	private int timeOut = -1;

	public void waitFor(int timeOut){
		Thread ts = null;
		if (timeOut >= 0){
			this.timeOut = timeOut;
			ts = new Thread(()->{
				try {
					Thread.sleep(timeOut);
					DBConnection connection = new DBConnection();
					connection.connect(conn.getHostName(), conn.getPort(), conn.getUserID(), conn.getPwd());
					String sessionId = connection.getSessionID();
					BasicStringVector bs = (BasicStringVector) connection.run("exec rootJobId from getConsoleJobs() where sessionId = " + sessionId);
					connection.run("cancelConsoleJob(\"" + bs.getString(bs.rows()-1) + "\")");
					semaphore.release();
				}catch (Exception e){

				}
			});
			ts.start();
		}
		try {
			semaphore.acquire();
			if (ts != null)
				ts.join();
		}catch (InterruptedException e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void finish(){
		semaphore.release();
	}

	public BasicDBTask(String script, List<Entity> args){
		this.script = script;
		this.args = args;
		try {
			semaphore.acquire();
		}catch (InterruptedException e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public BasicDBTask(String script){
		this.script = script;
		this.args = null;
		try {
			semaphore.acquire();
		}catch (InterruptedException e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	@Override
	public Entity call() {
		try{
			if(args != null)
				result = conn.run(script, args);
			else
				result = conn.run(script);
			errMsg = null;
			successful = true;
			return result;
		}
		catch(Exception t){
			successful = false;
			result = null;
			errMsg = t.getMessage();
			return null;
		}
	}

	@Override
	public void setDBConnection(DBConnection conn) {
		this.conn = conn;
	}

	@Override
	public Entity getResult() {
		return result;
	}

	@Override
	public String getErrorMsg() {
		return errMsg;
	}

	@Override
	public boolean isSuccessful() {
		return successful;
	}

	@Override
	public String getScript() {
		return script;
	}

	@Override
	public boolean isFinished(){
		if (successful || errMsg.length() > 0){
			return true;
		}else {
			return false;
		}
	}
}
