package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
	
	public BasicDoubleVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicDoubleVector(List<Double> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new double[list.size()];
			for (int i=0; i<list.size(); ++i)
				values[i] = list.get(i);
		}
	}
	
	public BasicDoubleVector(double[] array){
		super(DATA_FORM.DF_VECTOR);
		values = array.clone();
	}
	
	protected BasicDoubleVector(DATA_FORM df, int size){
		super(df);
		values = new double[size];
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
	}
	
	public Scalar get(int index){
		return new BasicDouble(values[index]);
	}
	
	public double getDouble(int index){
		return values[index];
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value.isNull()){
			values[index] = -Double.MAX_VALUE;
		}else{
			values[index] = value.getNumber().doubleValue();
		}

	}
	
	public void setDouble(int index, double value){
		values[index] = value;
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
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeDoubleArray(values);
	}

}
