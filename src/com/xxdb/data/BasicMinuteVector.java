package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

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
	
	public Scalar get(int index){
		return new BasicMinute(getInt(index));
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

}
