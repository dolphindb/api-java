package com.xxdb.io;

public class Double2 {
	public double x;
	public double y;
	
	public Double2(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	public boolean isNull(){
		return x==-Double.MAX_VALUE || y==-Double.MAX_VALUE;
	}
	
	public void setNull(){
		x = -Double.MAX_VALUE;
		y = -Double.MAX_VALUE;
	}
	
	public boolean equals(Object o){
		if(! (o instanceof Double2) || o == null)
			return false;
		else
			return x== ((Double2)o).x && y== ((Double2)o).y;
	}
	
	@Override
	public int hashCode(){
		return new Double(x).hashCode() ^ new Double(y).hashCode();
	}
	
	public int hashBucket(int buckets){
		return -1;
	}
}
