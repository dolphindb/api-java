package com.xxdb;

import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class SoakTestStream {
	
	public void test(Path serverFile, Path symbolFile, Path dateFile, int sessions, int breath){
		List<String> symbolList = new ArrayList<String>();
		List<Integer> symbolFreqPrefixSum = new ArrayList<Integer>();
		parseSymbolFreq(symbolFile, symbolList, symbolFreqPrefixSum);
		List<String> dateList = parseColumn(dateFile);
		ArrayList<DBConnection> connectionList = parseConnectionList(serverFile, sessions);

		if(connectionList.size() == 0){
			System.out.println("No servers collected, please put a list of servers in a text file with format: x.x.x.x:xxxx");
			return;
		}
		if(symbolList.size() == 0){
			System.out.println("No symbols collected, please put a list of symbols in a text file with format: symbol, count");
			return;
		}
		
		JobGenerator jobGenerator = new JobGenerator(symbolList, symbolFreqPrefixSum, dateList);
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
					String host = line.split(":")[0];
					int port = Integer.parseInt(line.split(":")[1]);
					DBConnection conn = new DBConnection();
					conn.connect(host, port);
					System.out.println("connected to " + line);
					connectionList.add(conn);
				}
			}
			else{
				while (connectionList.size() < numOfSession) {
					int idx = ThreadLocalRandom.current().nextInt(0, mylist.size());
					String line = mylist.get(idx % mylist.size());
					String host = line.split(":")[0];
					int port = Integer.parseInt(line.split(":")[1]);
					DBConnection conn = new DBConnection();
					conn.connect(host, port);
					System.out.println("connected to " + line);
					connectionList.add(conn);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return connectionList;
	}
	private void parseSymbolFreq(Path filePath, List<String> symbolList, List<Integer> symbolFreqPrefixSum) {
		try {
			int sum = 0;
			List<String> lines = Files.readAllLines(filePath);
			for (int i = 0; i < lines.size(); i++) {
				String[] parts = lines.get(i).split(",");
				String symbol = parts[1];
				Integer freq = (int)(Double.parseDouble(parts[3]) * 10000);
				symbolList.add(symbol);
				sum += freq;
				symbolFreqPrefixSum.add(sum);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private ArrayList<String> parseColumn(Path filePath) {
		final ArrayList<String> list = new ArrayList<>();
		final NavigableMap<Long, String> map = new TreeMap<Long, String>();
		try {
			List<String> lines = Files.readAllLines(filePath);
			for (int i = 0; i < lines.size(); i++) {
				list.add(lines.get(i).split(",")[0]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return list;
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
		private List<Integer> symbolFreqPrefixSum;
    	private List<String> symbolList;
		private List<String> dateList;
		private ArrayList <String> queryTypeList;
		private ArrayList<Integer> szList;
		
		public void queryTypeListAdd(int num,String x){
			for(int i = 0 ;i < num;i++)
				queryTypeList.add(x);
		}
		
    	public JobGenerator(List<String> symbolList, List<Integer> symbolFreqPrefixSum, List<String> dateList){
    		this.symbolFreqPrefixSum = symbolFreqPrefixSum;
    		this.symbolList = symbolList;
    		this.dateList = dateList;

    		queryTypeList = new ArrayList<String>();
			queryTypeListAdd(2,"SELECTTOP");
			queryTypeListAdd(6,"SELECTLAST");
			queryTypeListAdd(1,"GROUPBY_MINUTE");
			queryTypeListAdd(1,"BETWEENTIME");
    	    
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
			szList.add(4);
    	}

		private int lowerBound(List<Integer> nums, int target) {
			int low = 0, high = nums.size() - 1;
			while(low < high) {
				int mid = low + (high - low) / 2;
				if(nums.get(mid) < target)
					low = mid + 1;
				else
					high = mid;
			}
			return low;
		}

    	public String generateNextSymbol() {
    		//int x = ThreadLocalRandom.current().nextInt(0, symbolFreqPrefixSum.get(symbolFreqPrefixSum.size() - 1));
    		//int idx = lowerBound(symbolFreqPrefixSum, x);
    		//return symbolList.get(idx);
    		int x = ThreadLocalRandom.current().nextInt(0, symbolList.size());
    		return symbolList.get(x);
		}


    	public synchronized JobDesc next(){
	    	String symbol = generateNextSymbol();
	    	String queryType = queryTypeList.get(ThreadLocalRandom.current().nextInt(0, queryTypeList.size()));
	    	ArrayList<String> mysymbols = new ArrayList<String>();
	    	if(queryType.equalsIgnoreCase("GROUPBY_DATE")){
	    		int idx = ThreadLocalRandom.current().nextInt(0, szList.size());
	    		while(mysymbols.size()<szList.get(idx)){
					symbol = generateNextSymbol();
	    			if(mysymbols.contains(symbol))
	    				continue;
	    			mysymbols.add(symbol);
	    		}
	    	}
	    	else{
	    		mysymbols.add(symbol);  
	    	}
	    	String dateStr = "2016.12.30";
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
			System.out.println(Thread.currentThread().getId() + " " + dateFormat.format(new Date()) + " " + message);
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
				String sql =" ";
				BasicTable table = null;
				
				try {
				if(queryType.equalsIgnoreCase("SELECTTOP")) {
						
						sql = "select top 10 * from globalTrades where symbol=\"" +symbolList.get(0) + "\"" ;
						table = (BasicTable)conn.run(sql);
						//print(connStr + "   " + sql);
					}
					else if(queryType.equalsIgnoreCase("GROUPBY_MINUTE")) {
					
						sql = "select sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from globalTrades  where symbol=\"" +symbolList.get(0) + "\"  group by minute(time)";
						table = (BasicTable)conn.run(sql);
						//print(connStr + "   " + sql);
					}
					else if(queryType.equalsIgnoreCase("BETWEENTIME")) {
						
						String beginTime = "09:00:00.000";
						int rand1 = ThreadLocalRandom.current().nextInt(0, 5*3600*1000);	
						int rand2 = ThreadLocalRandom.current().nextInt(0, 1000);	
						sql = "select * from globalTrades where symbol=\"" + symbolList.get(0) + "\" and time > (" + beginTime + " + " + Integer.toString(rand1) + ") and time <= (" +  beginTime + " + " + Integer.toString(rand1) + "+" + Integer.toString(rand2) + ")";
						table = (BasicTable)conn.run(sql);
						//print(connStr + "   " + sql);
					}
					else if(queryType.equalsIgnoreCase("SELECTLAST")) {
						
						sql = "select last(askPrice)  from globalTrades where symbol=\"" +symbolList.get(0) + "\"";
						table = (BasicTable)conn.run(sql);
						//print(connStr + "   " + sql);
					}
					
				} catch (Exception e) {
					e.printStackTrace();
					print(connStr + ": " + String.join("\",\"", symbolList)  + ": " + date + " Failed ");
					continue;
				}

				print(connStr + "   " + sql);
			}
		}
	}
	
	public static void main(String[] args){
		int sessions = 32;
		if (args.length > 0) {
			sessions = Integer.parseInt(args[0]);;
		}

		System.out.println("sessions " + sessions);
		SoakTestStream dc = new SoakTestStream();
		dc.test(Paths.get("servers.txt"), Paths.get("QuotesSymbolDistProb.csv"), Paths.get("dates.txt"), sessions, 0);
	}
}