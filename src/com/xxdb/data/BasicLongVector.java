package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB long vector
 *
 */

public class BasicLongVector extends AbstractVector{
	private long[] values;
	
	public BasicLongVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicLongVector(List<Long> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new long[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicLongVector(long[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicLongVector(DATA_FORM df, int size){
		super(df);
		values = new long[size];
	}
	
	protected BasicLongVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new long[size];
		int totalBytes = size * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 8, end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getLong(i * 8);
			off += len;
		}
	}
	
	public Scalar get(int index){
		return new BasicLong(values[index]);
	}
	
	public long getLong(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = Long.MIN_VALUE;
		}else{
			values[index] = value.getNumber().longValue();
		}


	}
	
	public void setLong(int index, long value){
		values[index] = value;
	}
	
	@Override
	public boolean isNull(int index) {
		return values[index] == Long.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Long.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_LONG;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicLong.class;
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeLongArray(values);
	}
}
