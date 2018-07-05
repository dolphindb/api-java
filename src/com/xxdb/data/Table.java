package com.xxdb.data;

/**
 * 
 * Interface for table object
 *
 */

public interface Table extends Entity{
	Vector getColumn(int index);
	Vector getColumn(String name);
	String getColumnName(int index);
}
