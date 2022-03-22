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
	protected long[] values;
	
	public BasicLongVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicLongVector(List<Long> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new long[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null)
					values[i] = list.get(i);
				else
					values[i] = Long.MIN_VALUE;
			}
		}
	}
	
	public BasicLongVector(long[] array){
		this(array, true);
	}
	
	protected BasicLongVector(long[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
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
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getLong(i * 8);
			off += len;
			start += end;
		}
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeLong(values[start + i]);
		}
	}

	public Scalar get(int index){
		return new BasicLong(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		long[] sub = new long[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicLongVector(sub, false);
	}
	
	protected long[] getSubArray(int[] indices){
		int length = indices.length;
		long[] sub = new long[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return sub;
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
	public int hashBucket(int index, int buckets){
		long value = values[index];
		if(value >= 0)
			return (int)(value % buckets);
		else if(value == Long.MIN_VALUE)
			return -1;
		else{
			return (int)(((Long.MAX_VALUE % buckets) +2 + ((Long.MAX_VALUE + value) % buckets)) % buckets);
		}
	}

	@Override
	public int getUnitLength() {
		return 16;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicLongVector v = (BasicLongVector)vector;
		int newSize = this.rows() + v.rows();
		long[] newValue = new long[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicLongVector(newValue);
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

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (long val: values) {
			buffer.putLong(val);
		}
		return buffer;
	}
	
	@Override
	public int asof(Scalar value) {
		long target;
		try{
			target = value.getNumber().longValue();
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
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i)
		{
			out.putLong(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 8;
	}
}
