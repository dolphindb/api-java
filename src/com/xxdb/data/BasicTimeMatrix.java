package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

public class BasicTimeMatrix extends BasicIntMatrix{
	public BasicTimeMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicTimeMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicTimeMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setTime(int row, int column, LocalTime value){
		setInt(row, column, Utils.countMilliseconds(value));
	}
	
	public LocalTime getTime(int row, int column){
		return Utils.parseTime(getInt(row, column));
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicTime(getInt(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_TIME;
	}
}
