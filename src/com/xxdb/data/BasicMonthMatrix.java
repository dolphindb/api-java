package com.xxdb.data;

import java.io.IOException;
import java.time.YearMonth;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB MONTH MATRIX
 *
 */

public class BasicMonthMatrix extends BasicIntMatrix{
	public BasicMonthMatrix(int rows, int columns){
		super(rows, columns);
	}
	
	public BasicMonthMatrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
		super(rows,columns, listOfArrays);
	}
	
	public BasicMonthMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setMonth(int row, int column, YearMonth value){
		setInt(row, column, Utils.countMonths(value));
	}
	
	public YearMonth getMonth(int row, int column){
		return Utils.parseMonth(getInt(row, column));
	}
	

	@Override
	public Scalar get(int row, int column) {
		return new BasicMonth(getInt(row, column));
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_MONTH;
	}
	
	@Override
	public Class<?> getElementClass(){
		return YearMonth.class;
	}

}
