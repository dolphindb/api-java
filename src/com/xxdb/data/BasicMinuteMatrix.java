package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

public class BasicMinuteMatrix extends BasicIntMatrix{
	public BasicMinuteMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicMinuteMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicMinuteMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setMinute(int row, int column, LocalTime value){
		setInt(row, column, Utils.countMinutes(value));
	}
	
	public LocalTime getMinute(int row, int column){
		return Utils.parseMinute(getInt(row, column));
	}
	

	@Override
	public Scalar get(int row, int column) {
		return new BasicMinute(getInt(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_MINUTE;
	}

}
