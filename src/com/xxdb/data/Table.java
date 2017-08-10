package com.xxdb.data;

/**
 * 
 * Interface for DolphinDB data form: TABLE
 *
 */

public interface Table extends Entity{
	Vector getColumn(int index);
	Vector getColumn(String name);
	String getColumnName(int index);
}
