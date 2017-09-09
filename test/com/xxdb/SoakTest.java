package com.xxdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicTable;

public class SoakTest {
	
	public void test(Path serverFile, Path symbolFile, Path dateFile, int sessions, int breath){
		ArrayList<DBConnection> connectionList = parseConnectionList(serverFile, sessions);
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
		
		JobGenerator jobGenerator = new JobGenerator(symbolMap, dateMap);
		for(int i=0; i<sessions; ++i){
			new Thread(new Executor(jobGenerator, connectionList.get(i), breath)).start();
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
	
    private class JobDesc {
    	private String category;
    	private ArrayList<String> symbols;
    	private String date;
    	
    	public JobDesc(String category, ArrayList<String> symbols, String date){
    		this.category = category;
    		this.symbols = symbols;
    		this.date = date;
    	}
    	
    	public String getCategory(){
    		return category;
    	}
    	
    	public ArrayList<String> getSymbols(){
    		return symbols;
    	}
    	
    	public String getDate(){
    		return date;
    	}
    }
    
    private class JobGenerator {
    	private NavigableMap<Long, String> symbolMap;
		private NavigableMap<Long, String> dateMap;
		private ArrayList <String> queryTypeList;
		private ArrayList<Integer> szList;
		private long totalSymbolCount;
		private long totalDateCount;
		
    	public JobGenerator(NavigableMap<Long, String> symbolMap, NavigableMap<Long, String> dateMap){
    		this.symbolMap = symbolMap;
    		this.dateMap = dateMap;
    		totalSymbolCount = symbolMap.lastKey();
    		totalDateCount = dateMap.lastKey();
    		
    		queryTypeList = new ArrayList<String>();
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
    	    
    	    szList = new ArrayList<Integer>();
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
    	}
    	
    	public synchronized JobDesc next(){
    		long symbolIdx = ThreadLocalRandom.current().nextLong(0, totalSymbolCount);
	    	String symbol = symbolMap.floorEntry(symbolIdx).getValue();
	    	long dateIdx = ThreadLocalRandom.current().nextLong(0, totalDateCount);
	    	String queryType = queryTypeList.get(ThreadLocalRandom.current().nextInt(0, queryTypeList.size()));
	    	ArrayList<String> mysymbols = new ArrayList<String>();
	    	if(queryType.equalsIgnoreCase("groupbyDate")){
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
	    	String dateStr = dateMap.floorEntry(dateIdx).getValue();
	    	return new JobDesc(queryType, mysymbols, dateStr);
    	}
    }
	
	private class Executor implements Runnable {
		private JobGenerator generator;
		private DBConnection conn;
		private int breath;
		private DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		
		public Executor(JobGenerator generator, DBConnection conn, int breath){
			this.generator = generator;
			this.conn = conn;
			this.breath = breath;
		}
		
		private void print(String message){
			System.out.println(dateFormat.format(new Date()) + " " + message);
		}
		
		@Override
		public void run(){
			String connStr = conn.getHostName() + ":" + conn.getPort();
			
			while(true){
				if(breath > 0){
					try {
						Thread.sleep(breath);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
						print(connStr + " thread exit");
						break;
					}
				}
				
				JobDesc job = generator.next();
				String queryType = job.getCategory();
				String date = job.getDate();
				ArrayList<String> symbolList = job.getSymbols();

				long sum = 0;
				long total = 0;
				String sql;
				BasicTable table = null;
				
				try {
					long start = 0;
					int step = 100000; 
					if(queryType.equalsIgnoreCase("groupbyMinute")){
						sql = "select sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol=\"" +symbolList.get(0) + "\", date=" + date + " group by minute(time)";
						table = (BasicTable)conn.run(sql);
						print(connStr + "   " + sql);
					}
					else if(queryType.equalsIgnoreCase("groupbyDate")){
						BasicDateVector dateVec = (BasicDateVector) conn.run( "(" + date + "-4)..(" +date + "+5)");
						if(symbolList.size()>1)
							sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol in [\"" + String.join("\",\"", symbolList) + "\"], date>="+dateVec.get(0).toString() + " and date<=" + dateVec.get(9).toString() +" group by symbol, date" ;
						else
							sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol =\"" + symbolList.get(0) + "\", date>= " +dateVec.get(0).toString() + " and date<=" + dateVec.get(9).toString() +" group by date" ;
						print(connStr + "   " + sql);
						table = (BasicTable)conn.run(sql);
						
					}
					else{
						String symbol = symbolList.get(0);
						sql = "select  count(*) from TAQ where symbol=\"" + symbol + "\", date=" + date;
						table = (BasicTable)conn.run(sql);
						total = table.getColumn(0).get(0).getNumber().longValue();
						print(connStr + "   " + sql);
						while(sum < total){
							if(total<=step){
								sql = "select * from TAQ where symbol=\"" + symbol + "\", date=" + date;
								print(connStr + "   " + sql);
								table = (BasicTable)conn.run(sql);
							}
							else{
								sql = "select top " + start + ":" + (start+step) + " * from TAQ where symbol=\"" + symbol + "\", date=" + date;
								print(connStr + "   " + sql);
								table = (BasicTable)conn.run(sql);
							}
							
							start += step;
							sum += table.rows();
						}
					}
						
					
				} catch (Exception e) {
					e.printStackTrace();
					print(connStr + ": " + String.join("\",\"", symbolList)  + ": " + date + " Failed ");
					continue;
				}
				
				if(queryType.equalsIgnoreCase("download")){
					print(connStr + ": " + String.join("\",\"", symbolList)  + ": " + date + ": " + String.valueOf(total) + "==" + String.valueOf(sum) + " is " + String.valueOf(total==sum));
				}
				else{
					print(connStr + ": " + String.join("\",\"", symbolList)  + ": " + date + ": " + queryType + ": " + table.rows());
				}
			}
		}
	}
	
	public static void main(String[] args){
		SoakTest dc = new SoakTest();
		dc.test(Paths.get("soak_server.txt"), Paths.get("symbols.txt"), Paths.get("dates.txt"), 32, 100);
	}
}
