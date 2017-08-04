package com.xxdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DBTaskCollector{
	public DBTaskCollector() {
		
	}
	
	public void assembleTasks(Path taskFile, Path serverFile){
		ArrayList<String> tasks = this.parseScript(taskFile);
		ArrayList<String> servers = this.parseServerList(serverFile);
		ArrayList<DBConnection> connectionList = new ArrayList<DBConnection>();
		
		if(tasks.size() == 0){
			System.out.println("No tasks collected, please put a list of tasks in a text file seperated by at least two empty lines");
			return;
		}
		if(servers.size() == 0){
			System.out.println("No servers collected, please put a list of servers in a text file with format: x.x.x.x:xxxx");
			return;
		}
		
		for(String ss: servers){
			String[] parts = ss.split(":");
			DBConnection conn = new DBConnection();
			try{
				if(conn.connect(parts[0], Integer.parseInt(parts[1]))){
					connectionList.add(conn);
				}
				
			} catch (Exception e) {
				System.out.println("Connection failed: " + ss);
				e.printStackTrace();
			}
		}
		if(connectionList.size() == 0){
			System.out.println("No valid DB Connections!");
			return;
		}
		
		ArrayList<Integer> lista = new ArrayList <Integer>();
		lista.add(100);
		lista.add(300);
		lista.add(500);
		lista.add(1000);
		lista.add(2000);
		lista.add(3000);
		
		ArrayList<Integer> listb = new ArrayList <Integer>();
		listb.add(1000000);
		listb.add(1000000);
		listb.add(1000000);
		listb.add(3000000);
		listb.add(5000000);
		int ctt = 0;
		while(true){
			ctt++;
			System.out.println("RUNNING XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX " + ctt);
			//int randomNum = ThreadLocalRandom.current().nextInt(0, connectionList.size());
			Collections.shuffle(tasks);
			String threadName = "Thread" + (ctt);
			Collections.shuffle(lista);
			DBTaskRunner runner = new DBTaskRunner(threadName, tasks, connectionList,lista.get(0));
			runner.start();
			Collections.shuffle(listb);
			try {
				System.out.print("TAKING A BREAK for " + listb.get(0) + " ms");
				Thread.sleep(listb.get(0));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	private ArrayList<String> parseServerList(Path filePath){
		ArrayList<String> serverList = new ArrayList<String>();
		try {
			List<String> lines = Files.readAllLines(filePath);
			for(String line: lines){
				line = line.trim();
				if(!line.equals(""))
					serverList.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		Collections.shuffle(serverList);
		return serverList;
	}
	
	private ArrayList<String> parseScript(Path filePath){
		ArrayList<String> tasks = new ArrayList<String>(100);
		try {
			List<String> lines = Files.readAllLines(filePath);
			int emptyCt = 0;
			String script = "";
			String prevLine = "";
			for(String line: lines){
				line = line.trim();
				if(line.equals("")){
					emptyCt += 1;
				}
				else{
					script += line +"\n";
				}
				if(emptyCt>=2 &&prevLine.equals("")&&line.startsWith("//")){
					tasks.add(script);
					script = line + "\n";
					emptyCt = 0;
				}
				prevLine = line;
			}
			if(!script.trim().equals("")){
				tasks.add(script);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		Collections.shuffle(tasks);
		return tasks;
	}
	
	
	

	public static void main(String[] args) {
		if(args.length <2){
			System.out.println("Usage: java DBTaskCollector taskFile serverFile");
		}
		DBTaskCollector dc = new DBTaskCollector();
		dc.assembleTasks(Paths.get(args[0]), Paths.get(args[1]));
		System.out.println("Done!");
	}
}
