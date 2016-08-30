package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicFloatMatrix extends AbstractMatrix{
	private float[] values;
	
	public BasicFloatMatrix(int rows, int columns){
		super(rows, columns);
		values = new float[rows * columns];
	}
	
	public BasicFloatMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setFloat(int row, int column, float value){
		values[getIndex(row, column)] = value;
	}
	
	public float getFloat(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == -Float.MAX_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] =  -Float.MAX_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicFloat(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_FLOAT;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new float[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readFloat();
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(float value : values)
			out.writeFloat(value);
	}
}
