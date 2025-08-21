package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

/**
 * 
 * Corresponds to DolphinDB nanotime vector
 *
 */

public class BasicNanoTimeVector extends BasicLongVector{

	public BasicNanoTimeVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}

	public BasicNanoTimeVector(List<Long> list){
		super(list);
	}

	public BasicNanoTimeVector(long[] array){
		super(array);
	}
	
	protected BasicNanoTimeVector(long[] array, boolean copy){
		super(array, copy);
	}

	protected BasicNanoTimeVector(DATA_FORM df, int size){
		super(df, size);
	}

	protected BasicNanoTimeVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIME;
	}
	
	public Entity get(int index){
		return new BasicNanoTime(getLong(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicNanoTimeVector(getSubArray(indices), false);
	}
	
	public LocalTime getNanoTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseNanoTime(getLong(index));
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Long) {
			setLong(index, (long) value);
		} else if (value instanceof LocalTime) {
			setNanoTime(index, (LocalTime) value);
		} else if (value instanceof LocalDateTime) {
			setNanoTime(index, ((LocalDateTime) value).toLocalTime());
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only LocalTime, LocalDateTime, Long or null is supported.");
		}
	}
	
	public void setNanoTime(int index, LocalTime time){
		setLong(index, Utils.countNanoseconds(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTime.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicNanoTimeVector v = (BasicNanoTimeVector)vector;
		int newSize = this.rows() + v.rows();
		long[] newValue = new long[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicNanoTimeVector(newValue);
	}

	@Override
	public void add(Object value) {
		if (value == null) {
			add(Long.MIN_VALUE);
		} else if (value instanceof Long) {
			add((long) value);
		} else if (value instanceof LocalTime) {
			add(Utils.countNanoseconds((LocalTime) value));
		} else if (value instanceof LocalDateTime) {
			add(Utils.countNanoseconds(((LocalDateTime) value).toLocalTime()));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only LocalTime, LocalDateTime, Long or null is supported.");
		}
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().longValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicNanoTimeVector)value).getdataArray());
	}
}
