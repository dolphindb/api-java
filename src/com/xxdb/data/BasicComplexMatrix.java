package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicComplexMatrix extends AbstractMatrix{
	private Double2[] values;
	
	public BasicComplexMatrix(int rows, int columns){
		super(rows, columns);
		values = new Double2[rows * columns];
	}
	
	public BasicComplexMatrix(int rows, int columns, List<Double2[]> listOfArrays) throws Exception {
		super(rows,columns);
		values = new Double2[rows*columns];
		if (listOfArrays == null || listOfArrays.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			Double2[] array = listOfArrays.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
	}
	
	public BasicComplexMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setComplex(int row, int column, double real, double image){
		values[getIndex(row, column)] = new Double2(real, image);
	}
	
	public Double2 getDouble(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)].isNull();
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)].setNull();
	}

	@Override
	public Scalar get(int row, int column) {
		int index = getIndex(row, column);
		return new BasicComplex(values[index].x, values[index].y);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.BINARY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_COMPLEX;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicComplex.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new Double2[size];
		int totalBytes = size * 16, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 16, end = len / 16;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = new Double2(byteBuffer.getDouble(i * 16), byteBuffer.getDouble(i*16 + 8));
			off += len;
		}
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(Double2 value : values)
			out.writeDouble2(value);
	}
}
