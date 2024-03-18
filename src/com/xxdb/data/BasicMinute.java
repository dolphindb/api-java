package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

/**
 * 
 * Corresponds to DolphinDB minute scalar
 *
 */

public class BasicMinute extends BasicInt{

	public BasicMinute(LocalTime value){
		super(Utils.countMinutes(value));
	}

	public BasicMinute(Calendar value){
		super(Utils.countMinutes(value.get(Calendar.HOUR_OF_DAY),value.get(Calendar.MINUTE)));
	}
	
	public BasicMinute(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	public BasicMinute(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_MINUTE;
	}
	
	public LocalTime getMinute(){
		if(isNull())
			return null;
		else
			return Utils.parseMinute(getInt());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getMinute();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getMinute().format(DateTimeFormatter.ofPattern("HH:mm'm'"));
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicMinute) || o == null)
			return false;
		else
			return getInt() == ((BasicMinute)o).getInt();
	}
}
