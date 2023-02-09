package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

/**
 * 
 * Corresponds to DolphinDB month scalar
 *
 */

public class BasicMonth extends BasicInt{

	public BasicMonth(int year, Month month){
		super(year * 12 + month.getValue()-1);
	}
	public BasicMonth(Calendar calendar){
		super((calendar.get(Calendar.YEAR)) * 12 + calendar.get(Calendar.MONTH));
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
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_MONTH;
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getMonth();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else{
			YearMonth yearMonth = getMonth();
			if(yearMonth.getYear() == 0){
				return ("0000" + getMonth().format(DateTimeFormatter.ofPattern(".MM'M'")));
			}else{
				return getMonth().format(DateTimeFormatter.ofPattern("yyyy.MM'M'"));
			}
		}
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
