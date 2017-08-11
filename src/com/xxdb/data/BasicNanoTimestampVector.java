package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 
 * Corresponds to DolphinDB nanotimestamp vector
 *
 */

public class BasicNanoTimestampVector extends BasicLongVector{

	public BasicNanoTimestampVector(int size){
		super(size);
	}

	public BasicNanoTimestampVector(List<Long> list){
		super(list);
	}

	public BasicNanoTimestampVector(long[] array){
		super(array);
	}

	protected BasicNanoTimestampVector(DATA_FORM df, int size){
		super(df, size);
	}

	protected BasicNanoTimestampVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIMESTAMP;
	}
	
	public Scalar get(int index){
		return new BasicNanoTimestamp(getLong(index));
	}
	
	public LocalDateTime getTimestamp(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseNanoTimestamp(getLong(index));
	}
	
	public void setTimestamp(int index, LocalDateTime dt){
		setLong(index, Utils.countNanoseconds(dt));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTimestamp.class;
	}
}
