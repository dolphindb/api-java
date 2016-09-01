package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

public class BasicTimestampVector extends BasicLongVector{

	public BasicTimestampVector(int size){
		super(size);
	}
	
	public BasicTimestampVector(List<Long> list){
		super(list);
	}
	
	public BasicTimestampVector(long[] array){
		super(array);
	}
	
	protected BasicTimestampVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicTimestampVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_TIMESTAMP;
	}
	
	public Scalar get(int index){
		return new BasicTimestamp(getLong(index));
	}
	
	public LocalDateTime getTimestamp(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseTimestamp(getLong(index));
	}
	
	public void setTimestamp(int index, LocalDateTime dt){
		setLong(index, Utils.countMilliseconds(dt));
	}
}
