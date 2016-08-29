package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDoubleVector extends AbstractVector{
	private double[] values;
	
	public BasicDoubleVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	protected BasicDoubleVector(DATA_FORM df, int size){
		super(df);
		values = new double[size];
	}
	
	protected BasicDoubleVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new double[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readDouble();
	}
	
	public Scalar get(int index){
		return new BasicDouble(values[index]);
	}
	
	public double getDouble(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getNumber().doubleValue();
	}
	
	public void setDouble(int index, double value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == -Double.MAX_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = -Double.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DOUBLE;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(double value : values)
			out.writeDouble(value);
	}

}
