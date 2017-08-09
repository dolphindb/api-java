package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicFloatVector extends AbstractVector{
	private float[] values;
	
	public BasicFloatVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicFloatVector(List<Float> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new float[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicFloatVector(float[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
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
	
	@Override
	public Class<?> getElementClass(){
		return BasicFloat.class;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeFloatArray(values);
	}
}
