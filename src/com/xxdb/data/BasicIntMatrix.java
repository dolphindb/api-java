package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB INT MATRIX
 *
 */

public class BasicIntMatrix extends AbstractMatrix{
	private int[] values;
	
	public BasicIntMatrix(int rows, int columns){
		super(rows, columns);
		values = new int[rows * columns];
	}
	
	public BasicIntMatrix(int rows, int columns, List<int[]> list) throws Exception {
		super(rows,columns);
		values = new int[rows*columns];
		if (list == null || list.size() != columns)
			throw new Exception("input list of arrays does not have " + columns + " columns");
		for (int i=0; i<columns; ++i) {
			int[] array = list.get(i);
			if (array == null || array.length != rows)
				throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
			System.arraycopy(array, 0, values, i*rows, rows);
		}
	}
	
	public BasicIntMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setInt(int row, int column, int value){
		values[getIndex(row, column)] = value;
	}
	
	public int getInt(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == Integer.MIN_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = Integer.MIN_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicInt(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_INT;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicInt.class;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new int[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readInt();
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(int value : values)
			out.writeInt(value);
	}
}
