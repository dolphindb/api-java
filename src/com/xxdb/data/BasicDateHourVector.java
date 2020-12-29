package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB datehour vector
 *
 */

public class BasicDateHourVector extends BasicIntVector {
	public BasicDateHourVector(int size) {
		super(size);
	}
	
	public BasicDateHourVector(List<Integer> list){
		super(list);
	}
	
	public BasicDateHourVector(int[] array){
		super(array);
	}
	
	protected BasicDateHourVector(int[] array, boolean copy){
		super(array, copy);
	}
	
	protected BasicDateHourVector(DATA_FORM df, int size){
		super(df,size);
	}

	protected BasicDateHourVector(DATA_FORM df, ExtendedDataInput in) throws IOException{
		super(df, in);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DATEHOUR;
	}
	
	public Scalar get(int index){
		return new BasicDateHour(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicDateHourVector(getSubArray(indices), false);
	}
	
	public LocalDateTime getDateHour(int index){
		if(isNull(index))
			return null;
		else
			return Utils.parseDateHour(getInt(index));
	}
	
	public void setDateHour(int index, LocalDateTime dt){
		setInt(index,Utils.countHours(dt));
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicDateHour.class;
	}

	@Override
	public Vector combine(Vector vector) {
		BasicDateHourVector v = (BasicDateHourVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicDateHourVector(newValue);
	}
}
