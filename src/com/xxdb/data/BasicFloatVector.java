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
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicFloatVector(float[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
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
	
	public Scalar get(int index){
		return new BasicFloat(values[index]);
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
}
