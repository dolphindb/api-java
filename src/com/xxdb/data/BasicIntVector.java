package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB int vector
 *
 */

public class BasicIntVector extends AbstractVector{
	protected int[] values;
	
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
		int totalBytes = size * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 4, end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getInt(i * 4);
			off += len;
		}
	}
	
	public Scalar get(int index){
		return new BasicInt(values[index]);
	}
	
	public int getInt(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = Integer.MIN_VALUE;
		}else{
			values[index] = value.getNumber().intValue();
		}
	}
	
	public void setInt(int index, int value){
		values[index] = value;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		int value = values[index];
		if(value >= 0)
			return value % buckets;
		else if(value == Integer.MIN_VALUE)
			return -1;
		else{
			return (int)((4294967296l + value) % buckets);
		}
	}

	@Override
	public Vector combine(Vector vector) {
		BasicIntVector v = (BasicIntVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicIntVector(newValue);
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
	public Class<?> getElementClass(){
		return BasicInt.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeIntArray(values);
	}
}
