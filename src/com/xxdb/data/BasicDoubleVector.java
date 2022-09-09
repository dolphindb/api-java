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
 * Corresponds to DolphinDB double vector
 *
 */

public class BasicDoubleVector extends AbstractVector{
	private double[] values;
	private int size;
	private int capaticy;
	
	public BasicDoubleVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicDoubleVector(List<Double> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new double[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null)
					values[i] = list.get(i);
				else
					values[i]= -Double.MAX_VALUE;
			}
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	public BasicDoubleVector(double[] array){
		this(array, true);
	}
	
	public BasicDoubleVector(double[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;

		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicDoubleVector(DATA_FORM df, int size){
		super(df);
		values = new double[size];

		this.size = values.length;
		capaticy = values.length;
	}

	protected BasicDoubleVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new double[size];
		int totalBytes = size * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int start = off / 8, end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getDouble(i * 8);
			off += len;
		}

		this.size = values.length;
		capaticy = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		int totalBytes = count * 8, off = 0;
		ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		while (off < totalBytes) {
			int len = Math.min(BUF_SIZE, totalBytes - off);
			in.readFully(buf, 0, len);
			int end = len / 8;
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
			for (int i = 0; i < end; i++)
				values[i + start] = byteBuffer.getDouble(i * 8);
			off += len;
			start += end;
		}

		this.size = values.length;
		capaticy = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeDouble(values[start + i]);
		}
	}

	public Entity get(int index){
		return new BasicDouble(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		double[] sub = new double[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicDoubleVector(sub, false);
	}
	
	public double getDouble(int index){
		return values[index];
	}
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = -Double.MAX_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().doubleValue();
		}

	}

	public void setDouble(int index, double value){
		values[index] = value;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicDoubleVector v = (BasicDoubleVector)vector;
		int newSize = this.rows() + v.rows();
		double[] newValue = new double[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicDoubleVector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == -Double.MAX_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = -Double.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DOUBLE;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDouble.class;
	}

	@Override
	public int rows() {
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		double[] data = new double[size];
		System.arraycopy(values, 0, data, 0, size);
		out.writeDoubleArray(data);
	}

	@Override
	public int getUnitLength(){
		return 8;
	}


	public void add(double value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}


	public void addRange(double[] valueList) {
		values = Arrays.copyOf(values, valueList.length + values.length);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
		capaticy = values.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().doubleValue());
	}

	@Override
	public void Append(Vector value) {
		addRange(((BasicDoubleVector)value).getdataArray());
	}

	public double[] getdataArray(){
		double[] data = new double[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		double[] data = new double[size];
		System.arraycopy(values, 0, data, 0, size);
		for (double val: data) {
			buffer.putDouble(val);
		}
		return buffer;
	}

	@Override
	public int asof(Scalar value) {
		double target;
		try{
			target = value.getNumber().doubleValue();
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
			out.putDouble(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement * 8;
	}
}
