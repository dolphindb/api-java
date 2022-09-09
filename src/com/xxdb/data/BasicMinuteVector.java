package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB minute vector
 *
 */

public class BasicMinuteVector extends BasicIntVector{
	
	public BasicMinuteVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicMinuteVector(List<Integer> list){
		super(list);
	}
	
	public BasicMinuteVector(int[] array){
		super(array);
	}
	
	public BasicMinuteVector(int[] array, boolean copy){
		super(array, copy);
	}
	
	protected BasicMinuteVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicMinuteVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_MINUTE;
	}
	
	public Entity get(int index){
		return new BasicMinute(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicMinuteVector(getSubArray(indices), false);
	}
	
	public LocalTime getMinute(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseMinute(getInt(index));
	}
	
	public void setMinute(int index, LocalTime time){
		setInt(index, Utils.countMinutes(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicMinute.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicMinuteVector v = (BasicMinuteVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicMinuteVector(newValue);
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicMinuteVector)value).getdataArray());
	}
}
