package com.xxdb.data;
/**
 * 
 * Interface for DolphinDB data form: ARRAY, BIGARRAY
 *
 */

public interface Vector extends Entity{
	final int DISPLAY_ROWS = 10;
	Vector combine(Vector vector);
	boolean isNull(int index);
	void setNull(int index);
	int hashBucket(int index, int buckets);
	Scalar get(int index);
	void set(int index, Scalar value) throws Exception;
	Class<?> getElementClass();
}
