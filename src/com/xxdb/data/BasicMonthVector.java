package com.xxdb.data;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Calendar;
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

	public BasicMonthVector(List<Integer> list) {
		super(list);
	}

	public BasicMonthVector(int[] array) {
		super(array);
	}

	protected BasicMonthVector(int[] array, boolean copy) {
		super(array, copy);
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
	
	public Entity get(int index){
		return new BasicMonth(getInt(index));
	}
	
	public Vector getSubVector(int[] indices){
		return new BasicMonthVector(getSubArray(indices), false);
	}
	
	public YearMonth getMonth(int index){
		return Utils.parseMonth(getInt(index));
	}

	@Override
	public void set(int index, Object value) {
		if (value == null) {
			setNull(index);
		} else if (value instanceof Integer) {
			setInt(index, (int) value);
		} else if (value instanceof YearMonth) {
			setMonth(index, (YearMonth) value);
		} else if (value instanceof Calendar) {
			setInt(index, ((Calendar) value).get(Calendar.YEAR) * 12 + ((Calendar)value).get(Calendar.MONTH));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only YearMonth, Calendar, Integer or null is supported.");
		}
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

	@Override
	public void add(Object value) {
		if (value == null) {
			add(Integer.MIN_VALUE);
		} else if (value instanceof Integer) {
			add((int) value);
		} else if (value instanceof YearMonth) {
			add(Utils.countMonths((YearMonth) value));
		} else if (value instanceof Calendar) {
			add(((Calendar) value).get(Calendar.YEAR) * 12 + ((Calendar) value).get(Calendar.MONTH));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only YearMonth, Calendar, Integer or null is supported.");
		}
	}

	@Override
	public void Append(Scalar value) throws Exception{
		add(value.getNumber().intValue());
	}

	@Override
	public void Append(Vector value) throws Exception{
		addRange(((BasicMonthVector)value).getdataArray());
	}
}
