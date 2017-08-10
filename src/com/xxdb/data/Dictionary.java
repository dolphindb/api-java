package com.xxdb.data;

/*
 * Interface for DolphinDB data form: DICTIONARY.
 */

public interface Dictionary extends Entity{
	DATA_TYPE getKeyDataType();
	Entity get(Scalar key);
	boolean put(Scalar key, Entity value);
}
