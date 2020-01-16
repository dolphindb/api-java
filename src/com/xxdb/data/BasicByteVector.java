package com.xxdb.data;

import java.io.IOException;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB char vector
 *
 */

public class BasicByteVector extends AbstractVector{
	private byte[] values;
	
	public BasicByteVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicByteVector(List<Byte> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new byte[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicByteVector(byte[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicByteVector(DATA_FORM df, int size){
		super(df);
		values = new byte[size];
	}
	
	protected BasicByteVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
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
		return new BasicByte(values[index]);
	}
	
	public byte getByte(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = Byte.MIN_VALUE;
		}else{
			values[index] = value.getNumber().byteValue();
		}
	}
	
	public void setByte(int index, byte value){
		values[index] = value;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		int value = values[index];
		if(value >= 0)
			return value % buckets;
		else if(value == Byte.MIN_VALUE)
			return -1;
		else{
			return (int)((4294967296l + value) % buckets);
		}
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
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_BYTE;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicByte.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.write(values);
	}
}
