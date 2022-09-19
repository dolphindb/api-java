package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import org.apache.commons.lang3.ArrayUtils;

/**
 * 
 * Corresponds to DolphinDB bool vector
 *
 */

public class BasicBooleanVector extends AbstractVector{
	private byte[] values;
	private int size;
	private int capaticy;
	
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
		size = values.length;
		capaticy = values.length;
	}
	
	public BasicBooleanVector(byte[] array){
		this(array, true);
	}

	static byte[] convert(boolean[] b){
		byte[] ret = new byte[b.length];
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

		size = values.length;
		capaticy = values.length;
	}
	
	protected BasicBooleanVector(DATA_FORM df, int size){
		super(df);
		values = new byte[size];
		this.size = values.length;
		capaticy = values.length;
	}
	
	protected BasicBooleanVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df);
		int rows = in.readInt();
		int cols = in.readInt(); 
		int size = rows * cols;
		values = new byte[size];
		int off = 0;
		while (off < size) {
			int len = Math.min(4096, size - off);
			in.readFully(values, off, len);
			off += len;
		}
		this.size = values.length;
		capaticy = values.length;
	}
	
	@Override
	public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
		in.readFully(values, start, count);
		size = values.length;
		capaticy = values.length;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeByte(values[start + i]);
		}
	}

	public Entity get(int index){
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
	
	public void set(int index, Entity value) throws Exception {
		if(((Scalar)value).isNull()){
			values[index] = Byte.MIN_VALUE;
		}else{
			values[index] = ((Scalar)value).getNumber().byteValue();
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
		return size;
	}
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		out.write(data);
	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicBooleanVector.asof not supported.");
	}

	@Override
	public int getUnitLength(){
		return 1;
	}

	public void add(byte value) {
		if (size + 1 > capaticy && values.length > 0){
			values = Arrays.copyOf(values, values.length * 2);
		}else if (values.length <= 0){
			values = Arrays.copyOf(values, values.length + 1);
		}
		capaticy = values.length;
		values[size] = value;
		size++;
	}

	public void addRange(byte[] valueList) {
		values = Arrays.copyOf(values, valueList.length + values.length);
		System.arraycopy(valueList, 0, values, size, valueList.length);
		size += valueList.length;
		capaticy = values.length;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().byteValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicBooleanVector)value).getdataArray());
	}

	public byte[] getdataArray(){
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		return data;
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		byte[] data = new byte[size];
		System.arraycopy(values, 0, data, 0, size);
		for (byte val: data) {
			buffer.put(val);
		}
		return buffer;
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
		for (int i = 0; i < targetNumElement; ++i)
		{
			out.put(values[indexStart + i]);
		}
		numElementAndPartial.numElement = targetNumElement;
		numElementAndPartial.partial = 0;
		return targetNumElement;
	}
}
