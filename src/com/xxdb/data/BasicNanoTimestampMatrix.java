package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 
 * Corresponds to DolphinDB nanotimestamp matrix
 *
 */

public class BasicNanoTimestampMatrix extends BasicLongMatrix{
	public BasicNanoTimestampMatrix(int rows, int columns){
		super(rows, columns);
	}

	public BasicNanoTimestampMatrix(int rows, int columns, List<long[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}

	public BasicNanoTimestampMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setTimestamp(int row, int column, LocalDateTime value){
		setLong(row, column, Utils.countNanoseconds(value));
	}
	
	public LocalDateTime getTimestamp(int row, int column){
		return Utils.parseNanoTimestamp(getLong(row, column));
	}
	

	@Override
	public Scalar get(int row, int column) {
		return new BasicNanoTimestamp(getLong(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIMESTAMP;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTimestamp.class;
	}

}
