package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB short matrix
 *
 */

public class BasicShortMatrix extends AbstractMatrix{
	private short[] values;
	
	public BasicShortMatrix(int rows, int columns){
		super(rows, columns);
		values = new short[rows * columns];
	}
	
	public BasicShortMatrix(int rows, int columns, List<short[]> list) throws Exception {
		super(rows,columns);
		values = new short[rows*columns];
		if (list == null || list.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			short[] array = list.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
	}
	
	public BasicShortMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setShort(int row, int column, short value){
		values[getIndex(row, column)] = value;
	}
	
	public short getShort(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == Short.MIN_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = Short.MIN_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicShort(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_SHORT;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicShort.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new short[size];
		int totalBytes = size * 2, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 2, end = len / 2;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getShort(i * 2);
			off += len;
		}
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(short value : values)
			out.writeInt(value);
	}
}
