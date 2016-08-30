package com.xxdb.data;

public interface Vector extends Entity{
	final int DISPLAY_ROWS = 10;
	
	boolean isNull(int index);
	void setNull(int index);
	Scalar get(int index);
	void set(int index, Scalar value) throws Exception;
}
