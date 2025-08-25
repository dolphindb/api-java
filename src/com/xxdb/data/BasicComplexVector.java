package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Double2;

public class BasicComplexVector extends AbstractVector{
	protected Double2[] values;
	private int size;
	private int capacity;
	
	public BasicComplexVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}

	public BasicComplexVector(int size, int capacity) {
		super(DATA_FORM.DF_VECTOR);
		if (capacity < size) {
			capacity = size;
		}

		this.values = new Double2[capacity];
		for (int i = 0; i < size; i++) {
			values[i] = new Double2(0, 0);
		}

		this.size = size;
		this.capacity = capacity;
	}
	
	public BasicComplexVector(List<Double2> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new Double2[list.size()];
			for (int i=0; i<list.size(); ++i) {
				values[i] = list.get(i);
				if(values[i] == null){
					values[i] = new Double2(-Double.MAX_VALUE, -Double.MAX_VALUE);
				}
			}
		}

		this.size = values.length;
		this.capacity = values.length;
	}
	
	public BasicComplexVector(Double2[] array){
		this(array, true);
	}
	
	protected BasicComplexVector(Double2[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
		for(int i = 0; i < values.length; i++){
			if(values[i] == null){
				values[i] = new Double2(-Double.MAX_VALUE, -Double.MAX_VALUE);
			}
		}

		this.size = values.length;
		this.capacity = values.length;
	}
	
	protected BasicComplexVector(DATA_FORM df, int size){
		super(df);
		values = new Double2[size];
		for(int i=0; i<size; ++i)
			values[i] = new Double2(0, 0);

		this.size = values.length;
		this.capacity = values.length;
	}
	
	protected BasicComplexVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		values = new Double2[rows];
		long totalBytes = (long)rows * 16, off = 0;
		boolean littleEndian = in.isLittleEndian();
		ByteOrder bo = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 16), end = len / 16;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++){
				double x = byteBuffer.getDouble(i * 16);
				double y = byteBuffer.getDouble(i * 16 + 8);
				values[i + start] = new Double2(x, y);
			}
			off += len;
		}

		this.size = values.length;
		this.capacity = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		long totalBytes = (long)count * 16, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 16;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++){
				double x = byteBuffer.getDouble(i * 16);
				double y = byteBuffer.getDouble(i * 16 + 8);
				values[i + start] = new Double2(x, y);
			}
			off += len;
			start += end;
		}

		this.size = values.length;
		this.capacity = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeDouble2(values[start + i]);
		}
	}

	@Override
	public int getUnitLength(){
		return 16;
	}

	@Override
	public void add(Object value) {
		if (value == null) {
			add(new Double2(-Double.MAX_VALUE, -Double.MAX_VALUE));
		} else if (value instanceof Double2) {
			add((Double2) value);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Double2 or null is supported.");
		}
	}

	public void add(Double2 value) {
		if (size + 1 > capacity && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capacity = values.length;
		values[size] = value;
		size++;
	}

	public void addRange(Double2[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(new Double2(((BasicComplex)value).getReal(), ((BasicComplex)value).getImage()));
	}

	@Override
	public void Append(Vector value) {
		addRange(((BasicComplexVector)value).getdataArray());
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

	public Double2[] getdataArray(){
		Double2[] data = new Double2[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	public Entity get(int index){
		return new BasicComplex(values[index].x, values[index].y);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		Double2[] sub = new Double2[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicComplexVector(sub, false);
	}
	
	protected Double2[] getSubArray(int[] indices){
		int length = indices.length;
		Double2[] sub = new Double2[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return sub;
	}
	
	public void set(int index, Entity value) throws Exception {
		values[index].x = ((BasicComplex)value).getReal();
		values[index].y = ((BasicComplex)value).getImage();
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Double2) {
			Double2 d2 = (Double2) value;
			setComplex(index, d2.x, d2.y);
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only Double2 or null is supported.");
		}
	}
	
	public void setComplex(int index, double real, double image){
		values[index].x = real;
		values[index].y = image;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return values[index].hashBucket(buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicComplexVector v = (BasicComplexVector)vector;
		int newSize = this.rows() + v.rows();
		Double2[] newValue = new Double2[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicComplexVector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index].isNull();
	}

	@Override
	public void setNull(int index) {
		values[index].setNull();
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.BINARY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_COMPLEX;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicComplex.class;
	}

	@Override
	public int rows() {
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		Double2[] data = new Double2[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeDouble2Array(data);
	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicComplexVector.asof not supported.");
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		boolean isLittleEndian = buffer.order() == ByteOrder.LITTLE_ENDIAN;
		Double2[] data = new Double2[size];
		System.arraycopy(values, 0, data, 0, size);
		for (Double2 val: data) {
			if (isLittleEndian) {
				buffer.putDouble(val.x);
				buffer.putDouble(val.y);
			}else {
				buffer.putDouble(val.y);
				buffer.putDouble(val.x);
			}
		}
		return buffer;
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		boolean isLittleEndian = out.order() == ByteOrder.LITTLE_ENDIAN;
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i){
			if (isLittleEndian) {
				out.putDouble(values[indexStart + i].x);
				out.putDouble(values[indexStart + i].y);
			}else {
				out.putDouble(values[indexStart + i].y);
				out.putDouble(values[indexStart + i].x);
			}
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 16;
	}

	public Double2[] getValues() {
		return values;
	}
}
