package com.xxdb.data;

public interface Set extends Entity{
	boolean contains(Scalar key);
	boolean add(Scalar key);
}
