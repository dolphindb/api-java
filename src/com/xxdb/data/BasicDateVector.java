package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB date vector
 *
 */

public class BasicDateVector extends BasicIntVector{

	public BasicDateVector(int size) {
		super(size);
	}
	
	public BasicDateVector(List<Integer> list){
		super(list);
	}
	
	public BasicDateVector(int[] array){
		super(array);
	}
	
	protected BasicDateVector(int[] array, boolean copy){
		super(array, copy);
	}
	
	protected BasicDateVector(DATA_FORM df, int size){
		super(df,size);
	}

	protected BasicDateVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DATE;
	}
	
	public Entity get(int index){
		return new BasicDate(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicDateVector(getSubArray(indices), false);
	}
	
	public LocalDate getDate(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseDate(getInt(index));
	}
 
	public void setDate(int index, LocalDate date){
		setInt(index,Utils.countDays(date));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDate.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicDateVector v = (BasicDateVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicDateVector(newValue);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		for (int val: data) {
			buffer.putInt(val);
		}
		return buffer;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicDateVector)value).getdataArray());
	}
}
