package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;

import com.xxdb.io.ExtendedDataInput;

public class BasicDateTimeMatrix extends BasicIntMatrix{
	public BasicDateTimeMatrix(int rows, int columns){
		super(rows, columns);
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
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DATETIME;
	}
}
