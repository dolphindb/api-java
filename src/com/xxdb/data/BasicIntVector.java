package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicIntVector extends AbstractVector{
	private int[] values;
	
	public BasicIntVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicIntVector(List<Integer> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new int[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicIntVector(int[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicIntVector(DATA_FORM df, int size){
		super(df);
		values = new int[size];
	}
	
	protected BasicIntVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new int[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readInt();
	}
	
	public Scalar get(int index){
		return new BasicInt(values[index]);
	}
	
	public int getInt(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getNumber().intValue();
	}
	
	public void setInt(int index, int value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == Integer.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Integer.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_INT;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(int value : values)
			out.writeInt(value);
	}
}
