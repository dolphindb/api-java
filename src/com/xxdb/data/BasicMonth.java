package com.xxdb.data;

import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB month scalar
 *
 */

public class BasicMonth extends BasicInt{
	private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy.MM'M'");

	public BasicMonth(int year, Month month){
		super(year * 12 + month.getValue());
	}
	public BasicMonth(Calendar calendar){
		super((calendar.get(Calendar.YEAR)-1900) * 12 + calendar.get(Calendar.MONTH));
	}
	public BasicMonth(YearMonth value){
		super(value.getYear() * 12 + value.getMonthValue() - 1);
	}
	
	public BasicMonth(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicMonth(int value){
		super(value);
	}
	
	public YearMonth getMonth(){
		if(isNull())
			return null;
		else
			return Utils.parseMonth(getInt());
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_MONTH;
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getMonth();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getMonth().format(format);
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicMonth) || o == null)
			return false;
		else
			return getInt() == ((BasicMonth)o).getInt();
	}
}
