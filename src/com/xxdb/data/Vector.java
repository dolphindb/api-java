package com.xxdb.data;

import java.util.List;

/**
 * 
 * Interface for DolphinDB data form: ARRAY, BIGARRAY
 *
 */

public interface Vector extends Entity{
	final int DISPLAY_ROWS = 10;
	Vector combine(Vector vector);
	Vector getSubVector(int[] indices);
	boolean isNull(int index);
	void setNull(int index);
	int hashBucket(int index, int buckets);
	Scalar get(int index);
	void set(int index, Scalar value) throws Exception;
	Class<?> getElementClass();
}
