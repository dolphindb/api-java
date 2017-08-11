package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

/**
 * 
 * Corresponds to DolphinDB TIME VECTOR
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
	
	public LocalTime getTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseNanoTime(getLong(index));
	}
	
	public void setTime(int index, LocalTime time){
		setLong(index, Utils.countNanoseconds(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTime.class;
	}
}
