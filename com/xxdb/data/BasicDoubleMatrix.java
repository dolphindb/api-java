package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDoubleMatrix extends AbstractMatrix{
	private double[] values;
	
	public BasicDoubleMatrix(int rows, int columns){
		super(rows, columns);
		values = new double[rows * columns];
	}
	
	public BasicDoubleMatrix(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public void setDouble(int row, int column, double value){
		values[getIndex(row, column)] = value;
	}
	
	public double getDouble(int row, int column){
		return values[getIndex(row, column)];
	}
	
	@Override
	public boolean isNull(int row, int column) {
		return values[getIndex(row, column)] == -Double.MAX_VALUE;
	}

	@Override
	public void setNull(int row, int column) {
		values[getIndex(row, column)] = -Double.MAX_VALUE;
	}

	@Override
	public Scalar get(int row, int column) {
		return new BasicDouble(values[getIndex(row, column)]);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DOUBLE;
	}

	@Override
	protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
		int size = rows * columns;
		values =new double[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readDouble();
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(double value : values)
			out.writeDouble(value);
	}
}
