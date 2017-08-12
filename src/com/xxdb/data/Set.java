package com.xxdb.data;

/*
 * Interface for set object
 */

public interface Set extends Entity{
	boolean contains(Scalar key);
	boolean add(Scalar key);
}
