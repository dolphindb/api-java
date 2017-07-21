package com.xxdb.consumer;

import java.util.Date;
import java.io.PrintWriter;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TimingLogger {
	
	
	private static List<LogItem> _timelog = new ArrayList<LogItem>();
	
	public static void AddLog(long b,long ed,int size){
		
		long timediff = ed-b;
		LogItem item = new LogItem(timediff, size);
		_timelog.add(item);
		//Êä³ö¼ÆÊ±
		System.out.println(item.getTimeDiff() + "," + item.getNum());
	}
	public static int getAvg(){
		
		int size = _timelog.size();
		if(size ==0 ) return 0;
		
		long sum = 0;
		for (LogItem t : _timelog) {
			sum += t.getTimeDiff();
		}
		int re =  Math.round(sum/size);
		_timelog.clear();
		return re;
	}
}
