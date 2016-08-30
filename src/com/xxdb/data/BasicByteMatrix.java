package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicByteMatrix extends AbstractMatrix{
	private byte[] values;
	
	public BasicByteMatrix(int rows, int columns){
		super(rows, columns);
		values = new byte[rows * columns];
	}
	
	public BasicByteMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setInt(int row, int column, byte value){
		values[getIndex(row, column)] = value;
	}
	
	public byte getByte(int row, int column){
		return values[getIndex(row, column)];
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
		return new BasicByte(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_BYTE;
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
