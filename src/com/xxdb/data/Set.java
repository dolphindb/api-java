package com.xxdb.data;

/*
 * Interface for DolphinDB data form: SET
 */

public interface Set extends Entity{
	boolean contains(Scalar key);
	boolean add(Scalar key);
}
