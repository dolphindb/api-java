package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
	private int size;
	private int capacity;
	
	public BasicByteVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}

	public BasicByteVector(int size, int capacity) {
		super(DATA_FORM.DF_VECTOR);
		if (capacity < size) {
			capacity = size;
		}

		this.values = new byte[capacity];
		this.size = size;
		this.capacity = capacity;
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
		size = values.length;
		capacity = values.length;
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

		size = values.length;
		capacity = values.length;
	}
	
	protected BasicByteVector(DATA_FORM df, int size){
		super(df);
		values = new byte[size];

		this.size = values.length;
		capacity = values.length;
	}
	
	protected BasicByteVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		values = new byte[rows];
		int off = 0;
		while (off < rows) {
			int len = Math.min(4096, rows - off);
			in.readFully(values, off, len);
			off += len;
		}

		this.size = values.length;
		capacity = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		in.readFully(values, start, count);
		this.size = values.length;
		capacity = values.length;
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
	
	public Entity get(int index){
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
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = Byte.MIN_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().byteValue();
		}
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Byte) {
			setByte(index, (byte) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Byte or null is supported.");
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
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		out.write(data);
	}

	@Override
	public int getUnitLength(){
		return 1;
	}

	@Override
	public void add(Object value) {
		if (value == null) {
			add(Byte.MIN_VALUE);
		} else if (value instanceof Byte) {
			add((byte) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Byte or null is supported.");
		}
	}

	public void add(byte value) {
		if (size + 1 > capacity && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capacity = values.length;
		values[size] = value;
		size++;
	}

	public void addRange(byte[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().byteValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicByteVector)value).getdataArray());
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

	public byte[] getdataArray(){
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
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
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		for (byte val: data) {
			buffer.put(val);
		}
		return buffer;
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i)
		{
			out.put(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement;
	}

	public byte[] getValues() {
		return values;
	}
}
