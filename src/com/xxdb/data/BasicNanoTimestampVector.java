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
	
	protected BasicNanoTimestampVector(long[] array, boolean copy){
		super(array, copy);
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
	
	public Entity get(int index){
		return new BasicNanoTimestamp(getLong(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicNanoTimestampVector(getSubArray(indices), false);
	}
	
	public LocalDateTime getNanoTimestamp(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseNanoTimestamp(getLong(index));
	}
	
	public void setNanoTimestamp(int index, LocalDateTime dt){
		setLong(index, Utils.countDTNanoseconds(dt));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicNanoTimestamp.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicNanoTimestampVector v = (BasicNanoTimestampVector)vector;
		int newSize = this.rows() + v.rows();
		long[] newValue = new long[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicNanoTimestampVector(newValue);
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().longValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicNanoTimestampVector)value).getdataArray());
	}
}
