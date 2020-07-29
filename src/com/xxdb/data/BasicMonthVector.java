package com.xxdb.data;

import java.io.IOException;
import java.time.YearMonth;
import java.util.List;

import com.xxdb.io.ExtendedDataInput;


/**
 * 
 * Corresponds to DolphinDB month vector
 *
 */

public class BasicMonthVector extends BasicIntVector{

	public BasicMonthVector(int size){
		super(DATA_FORM.DF_VECTOR, size);
	}
	
	public BasicMonthVector(List<Integer> list){
		super(list);
	}
	
	public BasicMonthVector(int[] array){
		super(array);
	}
	
	protected BasicMonthVector(DATA_FORM df, int size){
		super(df, size);
	}
	
	protected BasicMonthVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df, in);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_MONTH;
	}
	
	public Scalar get(int index){
		return new BasicMonth(getInt(index));
	}
	
	public YearMonth getMonth(int index){
		return Utils.parseMonth(getInt(index));
	}
	
	public void setMonth(int index, YearMonth month){
		setInt(index, Utils.countMonths(month));
	}
	
	@Override
	public Class<?> getElementClass(){
		return YearMonth.class;
	}


	@Override
	public Vector combine(Vector vector) {
		BasicMonthVector v = (BasicMonthVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		return new BasicMonthVector(newValue);
	}
}
