package com.xxdb.streaming;

import java.io.IOException;
import java.util.ArrayList;

import com.xxdb.streaming.data.Entity;

public class DBTaskRunner implements Runnable {
	private DBConnection conn;
	private String connStr, threadName;
	private ArrayList<String> taskList;
	private Thread t;
	private long breath;
	public DBTaskRunner(String threadName, ArrayList<String> tasks, DBConnection conn, long breath) {
		this.conn = conn;
		this.taskList = tasks;
		this.connStr = conn.getHostName() + ":" + conn.getPort();
		this.breath = breath;
		this.threadName = threadName;
	}
	
	public void run(){
		String taskId = "";
		String taskString = null;
		try {
			for(String ts: taskList){
				taskString = ts;
				String[] lines = taskString.split("\n");
				if(lines[0].startsWith("//")){
					taskId = lines[0].substring(2, lines[0].length());
					System.out.println(threadName + ", " + connStr + " running " + taskId);
				}
				Thread.sleep(breath);
				Entity et = conn.run(taskString);
				if(et!=null){
					System.out.println(et);
				}
				else{
					System.out.println("failed");
				}
			}
			
		} catch (IOException | InterruptedException e) {
			if(!taskId.toLowerCase().contains("_ex")){
				e.printStackTrace();
				System.out.println(taskString);
			}
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
