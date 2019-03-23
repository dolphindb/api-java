package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB long matrix
 *
 */

public class BasicLongMatrix extends AbstractMatrix{
	private long[] values;
	
	public BasicLongMatrix(int rows, int columns){
		super(rows, columns);
		values = new long[rows * columns];
	}
	
	public BasicLongMatrix(int rows, int columns, List<long[]> list) throws Exception {
		super(rows,columns);
		values = new long[rows*columns];
		if (list == null || list.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			long[] array = list.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
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
	public Class<?> getElementClass(){
		return BasicLong.class;
	}
	
	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new long[size];
		int totalBytes = size * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 8, end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getLong(i * 8);
			off += len;
		}
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(long value : values)
			out.writeLong(value);
	}

}
