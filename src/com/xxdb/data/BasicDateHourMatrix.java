package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB datehour matrix
 *
 */

public class BasicDateHourMatrix extends BasicIntMatrix {
	public BasicDateHourMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicDateHourMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicDateHourMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setDateHour(int row, int column, LocalDateTime value){
		setInt(row, column, Utils.countHours(value));
	}
	
	public LocalDateTime getDateHour(int row, int column){
		return Utils.parseDateHour(getInt(row, column));
	}
	
	@Override
	public Scalar get(int row, int column) {
		return new BasicDateHour(getInt(row, column));
	}

	@Override
	public Class<?> getElementClass(){
		return BasicDateHour.class;
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DATEHOUR;
	}
}
