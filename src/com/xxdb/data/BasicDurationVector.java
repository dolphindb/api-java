package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDurationVector extends AbstractVector{
	private int[] values;
	private int size;
	private int capacity;
	
	public BasicDurationVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}

	public BasicDurationVector(int size, int capacity) {
		super(DATA_FORM.DF_VECTOR);
		if (capacity < size) {
			capacity = size;
		}

		this.values = new int[2 * capacity];
		this.size = 2 * size;
		this.capacity = 2 * capacity;
	}
		
	protected BasicDurationVector(DATA_FORM df, int size){
		super(df);
		values = new int[2*size];

		this.size = values.length;
		capacity = values.length;
	}
	
	protected BasicDurationVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * 2;
		values = new int[size];
		long totalBytes = (long)size * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 4), end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getInt(i * 4);
			off += len;
		}

		this.size = values.length;
		capacity = values.length;
	}
	
	public Entity get(int index){
		int unitIndex = values[2*index + 1];
		if(unitIndex == Integer.MIN_VALUE)
			return new BasicDuration(DURATION.NS, Integer.MIN_VALUE);
		else
			return new BasicDuration(values[2*index + 1], values[2*index]);
	}
		
	public void set(int index, Entity value) throws Exception {
		BasicDuration duration = (BasicDuration)value;
		values[2*index] = duration.getDuration();
		values[2*index + 1] = duration.getExchangeInt();
	}

	@Override
	public void set(int index, Object value) {
		throw new RuntimeException("BasicDurationVector.set not implemented yet.");
	}

	@Override
	public int hashBucket(int index, int buckets){
		return 0;
	}

	@Override
	public int getUnitLength() {
		return 4;
	}

	@Override
	public void add(Object value) {
		throw new RuntimeException("BasicDurationVector.add not implemented yet.");
	}

	public void add(int value, int duration) {
		if (size + 1 > capacity && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capacity = values.length;
		values[size] = value;
		values[size+1] = duration;
		size+=2;
	}


	public void addRange(int[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(((BasicDuration)value).getDuration(), ((BasicDuration)value).getExchangeInt());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicDurationVector)value).getdataArray());
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

	public int[] getdataArray(){
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	@Override
	public Vector combine(Vector vector) {
		throw new RuntimeException("BasicDurationVector.combine not implemented yet.");
	}

	@Override
	public boolean isNull(int index) {
		return values[2*index+1] == Integer.MIN_VALUE || values[2*index] == Integer.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[2*index] = 0;
		values[2*index + 1] = Integer.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.SYSTEM;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DURATION;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDuration.class;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeInt(values[start + i]);
		}
	}

	@Override
	public int rows() {
		return size/2;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeIntArray(data);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		for (int val: data) {
			buffer.putInt(val);
		}
		return buffer;
	}

	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicDurationVector.asof not implemented yet.");
	}

	@Override
	public Vector getSubVector(int[] indices) {
		throw new RuntimeException("BasicDurationVector.getSubVector not implemented yet.");
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i){
			out.putInt(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 4;
	}

	public int[] getValues() {
		return values;
	}
}