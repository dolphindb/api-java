package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;

public class BasicDate extends BasicInt{

	public BasicDate(LocalDate value){
		super(Utils.countDays(value));
	}
	
	public BasicDate(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicDate(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DATE;
	}
	
	public LocalDate getDate(){
		if(isNull())
			return null;
		else
			return Utils.parseDate(getInt());
	}

	@Override
	public Temporal getTemporal() throws Exception {
		return getDate();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getDate().toString();
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDate) || o == null)
			return false;
		else
			return getInt() == ((BasicDate)o).getInt();
	}
}
