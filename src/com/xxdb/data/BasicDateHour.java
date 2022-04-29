package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB datehour scalar
 *
 */

public class BasicDateHour extends BasicInt {
	private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH");

	public BasicDateHour(LocalDateTime value){
		super(Utils.countHours(value));
	}

	public BasicDateHour(Calendar value){
		super(Utils.countHours(value));
	}
	
	public BasicDateHour(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicDateHour(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DATEHOUR;
	}
	
	public LocalDateTime getDateHour() {
		if(isNull())
			return null;
		else
			return Utils.parseDateHour(getInt());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getDateHour();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getDateHour().format(format);
	}

	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDateHour) || o == null)
			return false;
		else
			return getInt() == ((BasicDateHour)o).getInt();
	}
}
