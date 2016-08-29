package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicLongMatrix extends AbstractMatrix{
	private long[] values;
	
	public BasicLongMatrix(int rows, int columns){
		super(rows, columns);
		values = new long[rows * columns];
	}
	
	public BasicLongMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setLong(int row, int column, long value){
		values[getIndex(row, column)] = value;
	}
	
	public long getLong(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == Long.MIN_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = Long.MIN_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicLong(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_LONG;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new long[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readLong();
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(long value : values)
			out.writeLong(value);
	}

}
