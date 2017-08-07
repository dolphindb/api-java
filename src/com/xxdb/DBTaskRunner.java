package com.xxdb;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import com.xxdb.data.Entity;

public class DBTaskRunner implements Runnable {
	private DBConnection conn;
	private ArrayList<DBConnection> connections;
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
	
	public DBTaskRunner(String threadName, ArrayList<String> tasks, ArrayList<DBConnection> connections, long breath) {
		this.connections = connections;
		this.taskList = tasks;
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
				if(connections != null){
					int randomNum = ThreadLocalRandom.current().nextInt(0, connections.size());
					DBConnection tmp = connections.get(randomNum);
					conn = tmp;
					connStr = conn.getHostName() + ":" + conn.getPort();
				}
				if(lines[0].startsWith("//")){
					taskId = lines[0].substring(2, lines[0].length());
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					Date date = new Date();
					System.out.println(threadName + ", " + connStr + " running " + taskId + " " + dateFormat.format(date));
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
