package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB bool vector
 *
 */

public class BasicBooleanVector extends AbstractVector{
	private byte[] values;
	
	public BasicBooleanVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicBooleanVector(List<Byte> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new byte[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicBooleanVector(byte[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
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
		int off = 0;
		while (off < size) {
			int len = Math.min(BUF_SIZE, size - off);
			in.readFully(values, off, len);
			off += len;
		}
	}
	
	public Scalar get(int index){
		return new BasicBoolean(values[index]);
	}
	
	public boolean getBoolean(int index){
		return values[index] != 0;
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = Byte.MIN_VALUE;
		}else{
			values[index] = value.getNumber().byteValue();
		}


	}
	
	public void setBoolean(int index, boolean value){
		values[index] = value ? (byte)1 : (byte)0;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicBooleanVector v = (BasicBooleanVector)vector;
		int newSize = this.rows() + v.rows();
		byte[] newValue = new byte[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicBooleanVector(newValue);
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
	public Class<?> getElementClass(){
		return BasicBoolean.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.write(values);
	}
}
