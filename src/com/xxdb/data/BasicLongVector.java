package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
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
	protected int size;
	protected int capacity;
	
	public BasicLongVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}

	public BasicLongVector(int size, int capacity) {
		super(DATA_FORM.DF_VECTOR);
		if (capacity < size) {
			capacity = size;
		}

		this.values = new long[capacity];
		this.size = size;
		this.capacity = capacity;
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

		this.size = values.length;
		capacity = values.length;
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

		this.size = values.length;
		capacity = values.length;
	}
	
	protected BasicLongVector(DATA_FORM df, int size){
		super(df);
		values = new long[size];

		this.size = values.length;;
		capacity = values.length;
	}
	
	protected BasicLongVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		values = new long[rows];
		long totalBytes = (long)rows * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 8), end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getLong(i * 8);
			off += len;
		}

		this.size = values.length;
		capacity = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		long totalBytes = (long)count * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getLong(i * 8);
			off += len;
			start += end;
		}

		this.size = values.length;
		capacity = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeLong(values[start + i]);
		}
	}

	public Entity get(int index){
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
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = Long.MIN_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().longValue();
		}
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Long) {
			setLong(index, (long) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Long or null is supported.");
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
	public void add(Object value) {
		if (value == null) {
			add(Long.MIN_VALUE);
		} else if (value instanceof Long) {
			add((long) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Long or null is supported.");
		}
	}

	public void add(long value) {
		if (size + 1 > capacity && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capacity = values.length;
		values[size] = value;
		size++;
	}


	public void addRange(long[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().longValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicLongVector)value).getdataArray());
	}

	@Override
	public void checkCapacity(int requiredCapacity) {
		if (requiredCapacity > values.length) {
			int newCapacity = Math.max(
					(int)(values.length * GROWTH_FACTOR),
					requiredCapacity
			);
			values = Arrays.copyOf(values, newCapacity);
			capacity = newCapacity;
		}
	}

	public long[] getdataArray(){
		long[] data = new long[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
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
		return size;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicLong.class;
	}

	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		long[] data = new long[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeLongArray(data);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		long[] data = new long[size];
		System.arraycopy(values, 0, data, 0, size);
		for (long val: data) {
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
		int end = size - 1;
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

	public long[] getValues() {
		return values;
	}
}
