package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

public class BasicSecondMatrix extends BasicIntMatrix{
	public BasicSecondMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicSecondMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicSecondMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setSecond(int row, int column, LocalTime value){
		setInt(row, column, Utils.countSeconds(value));
	}
	
	public LocalTime getSecond(int row, int column){
		return Utils.parseSecond(getInt(row, column));
	}
	
	@Override
	public Scalar get(int row, int column) {
		return new BasicSecond(getInt(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_SECOND;
	}
}
