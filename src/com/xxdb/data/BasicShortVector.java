package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i] = Short.MIN_VALUE;
				}
			}
		}
	}
	
	public BasicShortVector(short[] array){
		this(array, true);
	}
	
	public BasicShortVector(short[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
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
		int totalBytes = size * 2, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 2, end = len / 2;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getShort(i * 2);
			off += len;
		}
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 2, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 2;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getShort(i * 2);
			off += len;
			start += end;
		}
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeShort(values[start + i]);
		}
	}

	public short getShort(int index){
		return values[index];
	}
	
	public Scalar get(int index){
		return new BasicShort(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		short[] sub = new short[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicShortVector(sub, false);
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value == null){
			values[index] = Short.MIN_VALUE;
		}else{
			values[index] = value.getNumber().shortValue();
		}

	}

	public void setShort(int index, short value){
		values[index] = value;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		short value = values[index];
		if(value >= 0)
			return value % buckets;
		else if(value == Short.MIN_VALUE)
			return -1;
		else{
			return (int)((4294967296l + value) % buckets);
		}
	}

	@Override
	public int getUnitLength() {
		return 2;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicShortVector v = (BasicShortVector)vector;
		int newSize = this.rows() + v.rows();
		short[] newValue = new short[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicShortVector(newValue);
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

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (short val: values) {
			buffer.putShort(val);
		}
		return buffer;
	}
	
	@Override
	public int asof(Scalar value) {
		short target;
		try{
			target = value.getNumber().shortValue();
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
}
