package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

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
	
	public Scalar get(int index){
		return new BasicDate(getInt(index));
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
}
