package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
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
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i] = Byte.MIN_VALUE;
				}
			}
		}
	}

	public BasicByteVector(byte[] array){
		this(array, true);
	}
	
	protected BasicByteVector(byte[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
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
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		in.readFully(values, start, count);
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeByte(values[start + i]);
		}
	}

	public Vector combine(Vector vector){
		BasicByteVector v = (BasicByteVector)vector;
		int newSize = this.rows() + v.rows();
		byte[] newValue = new byte[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicByteVector(newValue);
	}
	
	public Scalar get(int index){
		return new BasicByte(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		byte[] sub = new byte[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicByteVector(sub, false);
	}
	
	public byte getByte(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value == null){
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

	@Override
	public int getUnitLength(){
		return 1;
	}
	
	@Override
	public int asof(Scalar value) {
		byte target;
		try{
			target = value.getNumber().byteValue();
		}
		catch(Exception ex){
			throw new RuntimeException(ex);
		}
		
		int start = 0;
		int end = values.length - 1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(values[mid] <= target)
				start = mid + 1;
			else
				end = mid - 1;
		}
		return end;
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (byte val: values) {
			buffer.put(val);
		}
		return buffer;
	}
}
