package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB short vector
 *
 */

public class BasicShortVector extends AbstractVector{
	private short[] values;
	
	public BasicShortVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicShortVector(List<Short> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new short[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicShortVector(short[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicShortVector(DATA_FORM df, int size){
		super(df);
		values = new short[size];
	}
	
	protected BasicShortVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new short[size];
		for(int i=0; i<size; ++i)
			values[i] = in.readShort();
	}
	
	public short getShort(int index){
		return values[index];
	}
	
	public Scalar get(int index){
		return new BasicShort(values[index]);
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value.getNumber().shortValue();
	}
	
	public void setShort(int index, short value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == Short.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Short.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_SHORT;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicShort.class;
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeShortArray(values);
	}
}
