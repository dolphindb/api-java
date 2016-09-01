package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

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
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new short[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readShort();
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(short value : values)
			out.writeInt(value);
	}
}
