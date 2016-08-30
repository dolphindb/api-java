package com.xxdb.data;

public interface Table extends Entity{
	Vector getColumn(int index);
	Vector getColumn(String name);
	String getColumnName(int index);
}
