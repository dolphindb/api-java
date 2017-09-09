package com.xxdb;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class DBTaskSoak{
	private int MAXSYMBOLCOUNT = 10000;
	private int concurrent=64;
	private long totalSymbolCount=new BigInteger("153391860010").longValueExact();
	private int totalDateCount = 34454;
	private int breath = 100;
	public DBTaskSoak() {
		
	}
	
	public void assembleTasks(Path serverFile, Path symbolFile, Path dateFile){
		ArrayList<DBConnection> connectionList = this.parseConnectionList(serverFile, concurrent);
		NavigableMap<Long, String> symbolMap = parseFreqMap(symbolFile);
		NavigableMap<Long, String> dateMap = parseFreqMap(dateFile);
		if(connectionList.size() == 0){
			System.out.println("No servers collected, please put a list of servers in a text file with format: x.x.x.x:xxxx");
			return;
		}
		
		if(symbolMap.size() == 0){
			System.out.println("No symbols collected, please put a list of symbols in a text file with format: symbol, count");
			return;
		}
			
		
		ExecutorService pool = Executors.newFixedThreadPool(concurrent);
	    Set<Future<String>> set = new HashSet<Future<String>>();
	    int ct = 0;
	    ArrayList <String> queryTypeList = new ArrayList<String>();
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("download");
	    queryTypeList.add("groupbyMinute");
	    queryTypeList.add("groupbyDate");
	    Collections.shuffle(connectionList);
	    ArrayList<String> mysymbols;
	    Callable<String> callable;
	    while (true) {
	    	  DBConnection conn = connectionList.get(ct % connectionList.size());
	    	  long symbolIdx = ThreadLocalRandom.current().nextLong(0, totalSymbolCount);
	    	  String symbol = symbolMap.floorEntry(symbolIdx).getValue();
	    	  int dateIdx = ThreadLocalRandom.current().nextInt(0, totalDateCount);
	    	  String queryType = queryTypeList.get(ThreadLocalRandom.current().nextInt(0, queryTypeList.size()));
	    	  mysymbols = new ArrayList<String>();
	    	  if(queryType.equalsIgnoreCase("groupbyDate")){
	    		  ArrayList<Integer> szList = new ArrayList<Integer>();
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(1);
	    		  szList.add(2);
	    		  szList.add(2);
	    		  szList.add(3);
	    		  
	    		  int idx = ThreadLocalRandom.current().nextInt(0, szList.size());
	    		  while(mysymbols.size()<szList.get(idx)){
	    			  symbolIdx = ThreadLocalRandom.current().nextLong(0, totalSymbolCount);
	    			  String tmpSymbol = symbolMap.floorEntry(symbolIdx).getValue();
	    			  if(mysymbols.contains(tmpSymbol))
	    				  continue;
	    			  mysymbols.add(tmpSymbol);
	    		  }
	    	  }
	    	  else{
	    		  mysymbols.add(symbol);
	    		  
	    	  }
	    	  String dateStr = dateMap.floorEntry((long)dateIdx).getValue();
	    	  callable = new DBTaskCallable(conn,mysymbols,dateStr, queryType,breath);
		      Future<String> future = pool.submit(callable);
		      set.add(future);
		      System.out.println("Task " +ct + "\t" + queryType + "\t" + String.join("\",\"", mysymbols) +"\t" + dateStr);
		      ct++;
		      if(ct>= MAXSYMBOLCOUNT){
	    		  break;
	    	  }
	    }
	   
		pool.shutdown();		
	    for (Future<String> future : set) {
	    	try {
				System.out.println(future.get());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
	    }
	    for(DBConnection c1 : connectionList){
	    	c1.close();
	    }
	}
	
	private ArrayList<DBConnection> parseConnectionList(Path filePath, int numOfSession){
		ArrayList<DBConnection> connectionList = new ArrayList<DBConnection>();
		try {
			List<String> lines = Files.readAllLines(filePath);
			ArrayList<String> mylist = new ArrayList<String>();
			for(String line: lines){
				line = line.trim();
				if(!line.equals("")){
					mylist.add(line);
				}
			}
			if(mylist.size() < numOfSession){
				for(int i=0;i<numOfSession; i++){
					String line = mylist.get(i % mylist.size());
					DBConnection conn = new DBConnection();
					conn.connect(line.split(":")[0], Integer.parseInt(line.split(":")[1]));
					connectionList.add(conn);
				}
			}
			else{
				for(String line: mylist){
					DBConnection conn = new DBConnection();
					conn.connect(line.split(":")[0], Integer.parseInt(line.split(":")[1]));
					connectionList.add(conn);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return connectionList;
	}
	
	private NavigableMap<Long, String> parseFreqMap(Path filePath){
		final NavigableMap<Long, String> map = new TreeMap<Long, String>();

		try {
			List<String> lines = Files.readAllLines(filePath);
			map.put((long)0, lines.get(0).split(",")[0]);
			for(int i=1; i<lines.size(); i++){
				String line = lines.get(i);
				map.put(Long.parseLong(line.split(",")[1].trim()), line.split(",")[0]);
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	
	
	public static void main(String[] args) {
		if(args.length <3){
			System.out.println("Usage: java DBTaskCollector serverFile symbolFile dateFile");
		}
		 System.out.println("Working Directory = " +
	              System.getProperty("user.dir"));
		DBTaskSoak dc = new DBTaskSoak();
		dc.assembleTasks(Paths.get("soak_server.txt"), Paths.get("symbols.txt"), Paths.get("dates.txt"));
		System.out.println("Done!");
	}
}
