package com.xxdb.streaming.data;

public interface Dictionary extends Entity{
	DATA_TYPE getKeyDataType();
	Entity get(Scalar key);
	boolean put(Scalar key, Entity value);
}
