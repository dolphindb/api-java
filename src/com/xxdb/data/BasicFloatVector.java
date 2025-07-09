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
 * Corresponds to DolphinDB float vector
 *
 */

public class BasicFloatVector extends AbstractVector{
	private float[] values;
	private int size;
	private int capaticy;
	
	public BasicFloatVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicFloatVector(List<Float> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new float[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i] = -Float.MAX_VALUE;
				}
			}
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	public BasicFloatVector(float[] array){
		this(array, true);
	}
	
	public BasicFloatVector(float[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicFloatVector(DATA_FORM df, int size){
		super(df);
		values = new float[size];

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicFloatVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		values = new float[rows];
		long totalBytes = (long)rows * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = (int)(off / 4), end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getFloat(i * 4);
			off += len;
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		long totalBytes = (long)count * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byte[] buf = new byte[4096];
		while (off < totalBytes) {
			int len = (int)Math.min(4096, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getFloat(i * 4);
			off += len;
			start += end;
		}

		this.size = values.length;
		capaticy = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeFloat(values[start + i]);
		}
	}

	@Override
	public int getUnitLength(){
		return 4;
	}


	public void add(float value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}


	public void addRange(float[] valueList) {
		int requiredCapacity = size + valueList.length;
		checkCapacity(requiredCapacity);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().floatValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicFloatVector)value).getdataArray());
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

	public float[] getdataArray(){
		float[] data = new float[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	public Entity get(int index){
		return new BasicFloat(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		float[] sub = new float[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicFloatVector(sub, false);
	}
	
	public float getFloat(int index){
		return values[index];
	}
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = -Float.MAX_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().floatValue();
		}

	}

	public void setFloat(int index, float value){
		values[index] = value;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicFloatVector v = (BasicFloatVector)vector;
		int newSize = this.rows() + v.rows();
		float[] newValue = new float[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicFloatVector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == -Float.MAX_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = -Float.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_FLOAT;
	}

	@Override
	public int rows() {
		return size;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicFloat.class;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		float[] data = new float[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeFloatArray(data);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		float[] data = new float[size];
		System.arraycopy(values, 0, data, 0, size);
		for (float val: data) {
			buffer.putFloat(val);
		}
		return buffer;
	}
	
	@Override
	public int asof(Scalar value) {
		float target;
		try{
			target = value.getNumber().floatValue();
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
			out.putFloat(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 4;
	}

	public float[] getValues() {
		return values;
	}
}
