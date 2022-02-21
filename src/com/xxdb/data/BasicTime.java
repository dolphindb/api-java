package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB time scalar.
 *
 */

public class BasicTime extends BasicInt{
	private static DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
	
	public BasicTime(LocalTime value){
		super(Utils.countMilliseconds(value));
	}
	public BasicTime(Calendar value){
		super(Utils.countMilliseconds(value));
	}
	
	public BasicTime(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicTime(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_TIME;
	}
	
	public LocalTime getTime(){
		if(isNull())
			return null;
		else
			return Utils.parseTime(getInt());
	}

	@Override
	public Temporal getTemporal() throws Exception {
		return getTime();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getTime().format(format);
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicTime) || o == null)
			return false;
		else
			return getInt() == ((BasicTime)o).getInt();
	}
}
