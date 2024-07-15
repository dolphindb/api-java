package com.xxdb.data;

/**
 * 
 * Interface for matrix object
 *
 */

public interface Matrix extends Entity{
	boolean isNull(int row, int column);
	void setNull(int row, int column);
	Entity getRowLabel(int row);
	Entity getColumnLabel(int column);
	Scalar get(int row, int column);
	Vector getRowLabels();
	Vector getColumnLabels();
	boolean hasRowLabel();
	boolean hasColumnLabel();
	Class<?> getElementClass();
	int getScale();
}
