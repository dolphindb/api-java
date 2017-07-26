package com.xxdb.streaming.data;

import java.time.temporal.Temporal;

public interface Scalar extends Entity{
	boolean isNull();
	void setNull();
	Number getNumber() throws Exception;
	Temporal getTemporal() throws Exception;
}
