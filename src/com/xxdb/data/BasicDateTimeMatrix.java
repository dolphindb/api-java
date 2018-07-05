package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB datetime matrix
 *
 */

public class BasicDateTimeMatrix extends BasicIntMatrix{
	public BasicDateTimeMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicDateTimeMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicDateTimeMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setDateTime(int row, int column, LocalDateTime value){
		setInt(row, column, Utils.countSeconds(value));
	}
	
	public LocalDateTime getDateTime(int row, int column){
		return Utils.parseDateTime(getInt(row, column));
	}
	
	@Override
	public Scalar get(int row, int column) {
		return new BasicDateTime(getInt(row, column));
	}

	@Override
	public Class<?> getElementClass(){
		return BasicDateTime.class;
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DATETIME;
	}
}
