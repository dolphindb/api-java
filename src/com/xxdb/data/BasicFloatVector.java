package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
	}
	
	protected BasicFloatVector(DATA_FORM df, int size){
		super(df);
		values = new float[size];
	}
	
	protected BasicFloatVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new float[size];
		int totalBytes = size * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 4, end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getFloat(i * 4);
			off += len;
		}
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 4, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 4;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getFloat(i * 4);
			off += len;
			start += end;
		}
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

	public Scalar get(int index){
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
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = -Float.MAX_VALUE;
		}else{
			values[index] = value.getNumber().floatValue();
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
		return values.length;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicFloat.class;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeFloatArray(values);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (float val: values) {
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
			out.putFloat(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 4;
	}
}
