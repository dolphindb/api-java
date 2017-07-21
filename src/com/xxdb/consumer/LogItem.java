package com.xxdb.consumer;

public class LogItem {

	private long _time;
	private int _num;
		
	public long getTimeDiff(){
		return this._time;
	}
	
	public int getNum() {
		return this._num;
	}
	public LogItem(long t,int n) {
		this._time = t;
		this._num = n;
	}
}
