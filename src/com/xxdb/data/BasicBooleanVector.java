package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB bool vector
 *
 */

public class BasicBooleanVector extends AbstractVector{
	private byte[] values;
	
	public BasicBooleanVector(int size){
		this(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicBooleanVector(List<Byte> list){
		super(DATA_FORM.DF_VECTOR);
		if (list != null) {
			values = new byte[list.size()];
			for (int i=0; i<list.size(); ++i) {
				if(list.get(i) != null)
					values[i] = list.get(i);
				else
					values[i] = Byte.MIN_VALUE;
			}
		}
	}
	
	public BasicBooleanVector(byte[] array){
		this(array, true);
	}

	static byte[] convert(boolean[] b){
		byte[] ret = new byte[]{};
		for (int i = 0;i < b.length;i++) {
			ret[i] = (byte)(b[i] ? 0x01 : 0x00);
		}
		return ret;
	}

	public BasicBooleanVector(boolean[] b){
		this(convert(b), true);
	}
	
	protected BasicBooleanVector(byte[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
	}
	
	protected BasicBooleanVector(DATA_FORM df, int size){
		super(df);
		values = new byte[size];
	}
	
	protected BasicBooleanVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new byte[size];
		int off = 0;
		while (off < size) {
			int len = Math.min(BUF_SIZE, size - off);
			in.readFully(values, off, len);
			off += len;
		}
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		in.readFully(values, start, count);
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeByte(values[start + i]);//todo:Have question
		}
	}

	public Scalar get(int index){
		return new BasicBoolean(values[index]);
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		byte[] sub = new byte[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicBooleanVector(sub, false);
	}
	
	public boolean getBoolean(int index){
		return values[index] != 0;
	}
	
	public void set(int index, Scalar value) throws Exception {
		if(value == null){
			values[index] = Byte.MIN_VALUE;
		}else{
			values[index] = value.getNumber().byteValue();
		}
	}

	public void setBoolean(int index, boolean value){
		values[index] = value ? (byte)1 : (byte)0;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicBooleanVector v = (BasicBooleanVector)vector;
		int newSize = this.rows() + v.rows();
		byte[] newValue = new byte[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicBooleanVector(newValue);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == Byte.MIN_VALUE;
	}

	@Override
	public void setNull(int index) {
		values[index] = Byte.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LOGICAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_BOOL;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicBoolean.class;
	}

	@Override
	public int rows() {
		return values.length;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		out.write(values);
	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicBooleanVector.asof not supported.");
	}

	@Override
	public int getUnitLength(){
		return 1;
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		for (byte val: values) {
			buffer.put(val);
		}
		return buffer;
	}
}
