package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB datetime vector
 *
 */

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
	
	protected BasicDateTimeVector(int[] array, boolean copy){
		super(array, copy);
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
	
	public Entity get(int index){
		return new BasicDateTime(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicDateTimeVector(getSubArray(indices), false);
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

	@Override
	public Vector combine(Vector vector) {
		BasicDateTimeVector v = (BasicDateTimeVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicDateTimeVector(newValue);
	}

	@Override
	public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
		int[] data = new int[size];
		System.arraycopy(values, 0, data, 0, size);
		for (int val: data) {
			buffer.putInt(val);
		}
		return buffer;
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicDateTimeVector)value).getdataArray());
	}
}
