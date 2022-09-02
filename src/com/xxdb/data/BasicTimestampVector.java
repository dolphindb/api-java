package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB timestamp vector
 *
 */

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
	
	public BasicTimestampVector(long[] array, boolean copy){
		super(array, copy);
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
	
	public Entity get(int index){
		return new BasicTimestamp(getLong(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicTimestampVector(getSubArray(indices), false);
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
	
	@Override
	public Class<?> getElementClass(){
		return BasicTimestamp.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicTimestampVector v = (BasicTimestampVector)vector;
		int newSize = this.rows() + v.rows();
		long[] newValue = new long[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicTimestampVector(newValue);
	}
}
