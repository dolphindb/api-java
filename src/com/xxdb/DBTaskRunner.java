package com.xxdb;

import java.io.IOException;

public class DBTaskRunner implements Runnable {
	private DBConnection conn;
	private String taskString, taskId, connStr;
	private Thread t;
	public DBTaskRunner(String taskId, String taskString, DBConnection conn) {
		this.conn = conn;
		this.taskString = taskString;
		this.taskId = taskId;
		this.connStr = conn.getHostName() + ":" + conn.getPort();
	}
	
	public void run(){
		try {
			System.out.println(connStr + " running " + taskId);
			conn.run(taskString);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void start(){
		if(t == null){
			t = new Thread(this);
		}
		//System.out.println(connStr + " starting " + taskId);
		t.start();
	}
	

	public void sleep(long millis){
		if(t == null){
			t = new Thread(this);
		}
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
