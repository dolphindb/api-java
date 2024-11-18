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
 * Corresponds to DolphinDB short vector
 *
 */

public class BasicShortVector extends AbstractVector{
	private short[] values;
	private int size;
	private int capaticy;
	
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

		this.size = values.length;
		capaticy = values.length;
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

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicShortVector(DATA_FORM df, int size){
		super(df);
		values = new short[size];

		this.size = values.length;;
		capaticy = values.length;
	}
	
	protected BasicShortVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new short[size];
		long totalBytes = (long)size * 2, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 2), end = len / 2;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getShort(i * 2);
			off += len;
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		long totalBytes = (long)count * 2, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 2;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getShort(i * 2);
			off += len;
			start += end;
		}

		this.size = values.length;
		capaticy = values.length;
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
	
	public Entity get(int index){
		return new BasicShort(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		short[] sub = new short[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicShortVector(sub, false);
	}
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = Short.MIN_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().shortValue();
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


	public void add(short value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}


	public void addRange(short[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().shortValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicShortVector)value).getdataArray());
	}

	@Override
	public void checkCapacity(int requiredCapacity) {
		if (requiredCapacity > values.length) {
			int newCapacity = Math.max(
					(int)(values.length * GROWTH_FACTOR),
					requiredCapacity
			);
			values = Arrays.copyOf(values, newCapacity);
			capaticy = newCapacity;
		}
	}

	public short[] getdataArray(){
		short[] data = new short[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
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
		return size;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicShort.class;
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		short[] data = new short[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeShortArray(data);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		short[] data = new short[size];
		System.arraycopy(values, 0, data, 0, size);
		for (short val: data) {
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
			out.putShort(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 2;
	}

	public short[] getValues() {
		return values;
	}
}
