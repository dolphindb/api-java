package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB SECOND VECTOR
 *
 */

public class BasicSecondVector extends BasicIntVector{
	
	public BasicSecondVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicSecondVector(List<Integer> list){
		super(list);
	}
	
	public BasicSecondVector(int[] array){
		super(array);
	}
	
	protected BasicSecondVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicSecondVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_SECOND;
	}
	
	public Scalar get(int index){
		return new BasicSecond(getInt(index));
	}
	
	public LocalTime getSecond(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseSecond(getInt(index));
	}
	
	public void setSecond(int index, LocalTime time){
		setInt(index, Utils.countSeconds(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicSecond.class;
	}
}
