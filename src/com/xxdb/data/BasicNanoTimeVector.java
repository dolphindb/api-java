package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
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
	
	public Scalar get(int index){
		return new BasicNanoTime(getLong(index));
	}
	
	public LocalTime getNanoTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseNanoTime(getLong(index));
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
}
