package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;

public class BasicPointVector extends AbstractVector{
	protected Double2[] values;
	protected int size;
	protected int capaticy;
	
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

		this.size = values.length;
		capaticy = values.length;
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

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicPointVector(DATA_FORM df, int size){
		super(df);
		values = new Double2[size];
		for(int i=0; i<size; ++i)
			values[i] = new Double2(0, 0);

		this.size = values.length;;
		capaticy = values.length;
	}
	
	protected BasicPointVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new Double2[size];
		long totalBytes = (long)size * 16, off = 0;
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
		capaticy = values.length;
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
		capaticy = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeDouble2(values[start + i]);
		}
	}

	public Entity get(int index){
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
	
	public void set(int index, Entity value) throws Exception {
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
	public int getUnitLength() {
		return 16;
	}


	public void add(Double2 value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}


	public void addRange(Double2[] valueList) {
		values = Arrays.copyOf(values, valueList.length + values.length);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
		capaticy = values.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(new Double2(((BasicPoint)value).getX(), ((BasicPoint)value).getY()));
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicPointVector)value).getdataArray());
	}

	public Double2[] getdataArray(){
		Double2[] data = new Double2[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
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
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		Double2[] data = new Double2[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeDouble2Array(data);
	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicPointVector.asof not supported.");
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
}
