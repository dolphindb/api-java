package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB DATE MATRIX
 *
 */

public class BasicDateMatrix extends BasicIntMatrix{
	public BasicDateMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicDateMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicDateMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setDate(int row, int column, LocalDate value){
		setInt(row, column, Utils.countDays(value));
	}
	
	public LocalDate getDate(int row, int column){
		return Utils.parseDate(getInt(row, column));
	}
	

	@Override
	public Scalar get(int row, int column) {
		return new BasicDate(getInt(row, column));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDate.class;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DATE;
	}
}
