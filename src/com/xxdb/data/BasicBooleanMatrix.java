package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;


/**
 * 
 * Corresponds to DolphinDB bool matrix
 *
 */

public class BasicBooleanMatrix extends AbstractMatrix{
private byte[] values;
	
	public BasicBooleanMatrix(int rows, int columns){
		super(rows, columns);
		values = new byte[rows * columns];
	}
	
	public BasicBooleanMatrix(int rows, int columns, List<byte[]> listOfArrays) throws Exception {
		super(rows,columns);
		values = new byte[rows*columns];
		if (listOfArrays == null || listOfArrays.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			byte[] array = listOfArrays.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
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
	public Class<?> getElementClass(){
		return BasicBoolean.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new byte[size];
		int off = 0;
		while (off < size) {
			int len = Math.min(BUF_SIZE, size - off);
			in.readFully(values, off, len);
			off += len;
		}
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(byte value : values)
			out.writeByte(value);
	}
}
