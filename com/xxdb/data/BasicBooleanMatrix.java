package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicBooleanMatrix extends AbstractMatrix{
private byte[] values;
	
	public BasicBooleanMatrix(int rows, int columns){
		super(rows, columns);
		values = new byte[rows * columns];
	}
	
	public BasicBooleanMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setBoolean(int row, int column, boolean value){
		values[getIndex(row, column)] = value ? (byte)1 : (byte)0;
	}
	
	public boolean getBoolean(int row, int column){
		return values[getIndex(row, column)] == 1;
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == Byte.MIN_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = Byte.MIN_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicBoolean(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.LOGICAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_BOOL;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new byte[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readByte();
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(byte value : values)
			out.writeByte(value);
	}
}
