package com.xxdb.streaming.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.streaming.io.ExtendedDataInput;

public class BasicDateTimeVector extends BasicIntVector{

	public BasicDateTimeVector(int size) {
		super(size);
	}
	
	public BasicDateTimeVector(List<Integer> list){
		super(list);
	}
	
	public BasicDateTimeVector(int[] array){
		super(array);
	}
	
	protected BasicDateTimeVector(DATA_FORM df, int size){
		super(df,size);
	}

	protected BasicDateTimeVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DATETIME;
	}
	
	public Scalar get(int index){
		return new BasicDateTime(getInt(index));
	}
	
	public LocalDateTime getDateTime(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseDateTime(getInt(index));
	}
	
	public void setDateTime(int index, LocalDateTime dt){
		setInt(index,Utils.countSeconds(dt));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDateTime.class;
	}

}
