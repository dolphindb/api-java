package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;
import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB TIME VECTOR
 *
 */

public class BasicTimeVector extends BasicIntVector{
	
	public BasicTimeVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicTimeVector(List<Integer> list){
		super(list);
	}
	
	public BasicTimeVector(int[] array){
		super(array);
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
	
	public Scalar get(int index){
		return new BasicTime(getInt(index));
	}
	
	public LocalTime getTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseTime(getInt(index));
	}
	
	public void setTime(int index, LocalTime time){
		setInt(index, Utils.countMilliseconds(time));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicTime.class;
	}
}
