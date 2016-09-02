package com.xxdb.data;

public interface Matrix extends Entity{
	boolean isNull(int row, int column);
	void setNull(int row, int column);
	Scalar getRowLabel(int row);
	Scalar getColumnLabel(int column);
	Scalar get(int row, int column);
	boolean hasRowLabel();
	boolean hasColumnLabel();
	Class<?> getElementClass();
}
