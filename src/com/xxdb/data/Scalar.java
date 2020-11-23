package com.xxdb.data;

import java.time.temporal.Temporal;
/**
 * 
 * Interface for scalar object
 *
 */

public interface Scalar extends Entity{
	boolean isNull();
	void setNull();
	Number getNumber() throws Exception;
	Temporal getTemporal() throws Exception;
	int hashBucket(int buckets);
	String getJsonString();
}
