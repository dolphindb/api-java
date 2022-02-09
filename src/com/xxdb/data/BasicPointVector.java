package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicPointVector extends AbstractVector{
	protected Double2[] values;
	
	public BasicPointVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicPointVector(List<Double2> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new Double2[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null) {
					values[i] = list.get(i);
				}else{
					values[i]=new Double2(-Double.MAX_VALUE, -Double.MAX_VALUE);
				}
			}
		}
	}
	
	public BasicPointVector(Double2[] array){
		this(array, true);
	}
	
	protected BasicPointVector(Double2[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
		for(int i = 0; i < values.length; i++){
			if(values[i] == null){
				values[i]=new Double2(-Double.MAX_VALUE, -Double.MAX_VALUE);
			}
		}
	}
	
	protected BasicPointVector(DATA_FORM df, int size){
		super(df);
		values = new Double2[size];
		for(int i=0; i<size; ++i)
			values[i] = new Double2(0, 0);
	}
	
	protected BasicPointVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new Double2[size];
		int totalBytes = size * 16, off = 0;
		boolean littleEndian = in.isLittleEndian();
		ByteOrder bo = littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 16, end = len / 16;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++){
				double x = byteBuffer.getDouble(i * 16);
				double y = byteBuffer.getDouble(i * 16 + 8);
				values[i + start] = new Double2(x, y);
			}
			off += len;
		}
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 16, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
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
	}
	
	public Scalar get(int index){
		return new BasicPoint(values[index].x, values[index].y);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		Double2[] sub = new Double2[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicPointVector(sub, false);
	}
	
	protected Double2[] getSubArray(int[] indices){
		int length = indices.length;
		Double2[] sub = new Double2[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return sub;
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index].x = ((BasicPoint)value).getX();
		values[index].y = ((BasicPoint)value).getY();
	}
	
	public void setPoint(int index, double x, double y){
		values[index].x = x;
		values[index].y = y;
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return values[index].hashBucket(buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicPointVector v = (BasicPointVector)vector;
		int newSize = this.rows() + v.rows();
		Double2[] newValue = new Double2[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicPointVector(newValue);
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
		return Entity.DATA_TYPE.DT_POINT;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicPoint.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeDouble2Array(values);
	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicPointVector.asof not supported.");
	}

	@Override
	protected ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (Double2 val: values) {
			buffer.putDouble(val.x);
			buffer.putDouble(val.y);
		}
		return buffer;
	}
}