package com.xxdb.data;

import java.time.temporal.Temporal;
/**
 * 
 * Interface for DolphinDB data form: SCALAR
 *
 */

public interface Scalar extends Entity{
	boolean isNull();
	void setNull();
	Number getNumber() throws Exception;
	Temporal getTemporal() throws Exception;
}
