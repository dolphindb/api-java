package com.xxdb.io;

public class Long2 {
	public long high;
	public long low;
	
	public Long2(long high, long low){
		this.high = high;
		this.low = low;
	}
	
	public boolean isNull(){
		return high==0 && low==0;
	}
	
	public void setNull(){
		high = 0;
		low = 0;
	}
	
	public boolean equals(Object o){
		if(! (o instanceof Long2) || o == null)
			return false;
		else
			return high== ((Long2)o).high && low== ((Long2)o).low;
	}
	
	@Override
	public int hashCode(){
		return (int)(high^low>>>32);
	}
}
