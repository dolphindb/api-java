package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.List;
import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB time vector
 *
 */

public class BasicTimeVector extends BasicIntVector{
	
	public BasicTimeVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}

	public BasicTimeVector(int size, int capacity) {
		super(size, capacity);
	}
	
	public BasicTimeVector(List<Integer> list){
		super(list);
	}
	
	public BasicTimeVector(int[] array){
		super(array);
	}
	
	protected BasicTimeVector(int[] array, boolean copy){
		super(array, copy);
	}
	
	protected BasicTimeVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicTimeVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_TIME;
	}
	
	public Entity get(int index){
		return new BasicTime(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicTimeVector(getSubArray(indices), false);
	}
	
	public LocalTime getTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseTime(getInt(index));
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Integer) {
			setInt(index, (int) value);
		} else if (value instanceof LocalTime) {
			setTime(index, (LocalTime) value);
		} else if (value instanceof Calendar) {
			setInt(index, Utils.countMilliseconds((Calendar) value));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only LocalTime, Calendar, Integer or null is supported.");
		}
	}
	
	public void setTime(int index, LocalTime time){
		setInt(index, Utils.countMilliseconds(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicTime.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicTimeVector v = (BasicTimeVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicTimeVector(newValue);
	}

	@Override
	public void add(Object value) {
		if (value == null) {
			add(Integer.MIN_VALUE);
		} else if (value instanceof Integer) {
			add((int) value);
		} else if (value instanceof LocalTime) {
			add(Utils.countMilliseconds((LocalTime) value));
		} else if (value instanceof Calendar) {
			add(Utils.countMilliseconds((Calendar) value));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only LocalTime, Calendar, Integer or null is supported.");
		}
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicTimeVector)value).getdataArray());
	}
}
