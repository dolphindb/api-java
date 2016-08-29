package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicFloatVector extends AbstractVector{
	private float[] values;
	
	public BasicFloatVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	protected BasicFloatVector(DATA_FORM df, int size){
		super(df);
		values = new float[size];
	}
	
	protected BasicFloatVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new float[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readFloat();
	}
	
	public Scalar get(int index){
		return new BasicFloat(values[index]);
	}
	
	public float getFloat(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getNumber().floatValue();
	}
	
	public void setFloat(int index, float value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == -Float.MAX_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = -Float.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_FLOAT;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(float value : values)
			out.writeFloat(value);
	}
}
