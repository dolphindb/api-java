package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

/**
 * 
 * Corresponds to DolphinDB nanotime matrix.
 *
 */

public class BasicNanoTimeMatrix extends BasicLongMatrix{
	public BasicNanoTimeMatrix(int rows, int columns){
		super(rows, columns);
	}

	public BasicNanoTimeMatrix(int rows, int columns, List<long[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}

	public BasicNanoTimeMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setNanoTime(int row, int column, LocalTime value){
		setLong(row, column, Utils.countNanoseconds(value));
	}
	
	public LocalTime getNanoTime(int row, int column){
		return Utils.parseNanoTime(getLong(row, column));
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicNanoTime(getLong(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIME;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTime.class;
	}
}
