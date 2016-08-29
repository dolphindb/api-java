package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicBooleanVector extends AbstractVector{
	private byte[] values;
	
	public BasicBooleanVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	protected BasicBooleanVector(DATA_FORM df, int size){
		super(df);
		values = new byte[size];
	}
	
	protected BasicBooleanVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new byte[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readByte();
	}
	
	public Scalar get(int index){
		return new BasicBoolean(values[index]);
	}
	
	public boolean getBoolean(int index){
		return values[index] != 0;
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getNumber().byteValue();
	}
	
	public void setBoolean(int index, boolean value){
		values[index] = value ? (byte)1 : (byte)0;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == Byte.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Byte.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LOGICAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_BOOL;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		for(byte value : values)
			out.writeByte(value);
	}
}
