package com.xxdb.data;

/*
 * Interface for dictionary object
 */

public interface Dictionary extends Entity{
	DATA_TYPE getKeyDataType();
	Entity get(Scalar key);
	boolean put(Scalar key, Entity value);
}
