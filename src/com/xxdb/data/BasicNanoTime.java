package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

/**
 * Corresponds to DolphinDB nanotime scalar.
 *
 */

public class BasicNanoTime extends BasicLong{

	public BasicNanoTime(LocalDateTime value){
		super(Utils.countNanoseconds(value.toLocalTime()));
	}

	public BasicNanoTime(LocalTime value){
		super(Utils.countNanoseconds(value));
	}

	public BasicNanoTime(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public BasicNanoTime(long value){
		super(value);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIME;
	}
	
	public LocalTime getNanoTime(){
		if(isNull())
			return null;
		else
			return Utils.parseNanoTime(getLong());
	}

	@Override
	public Temporal getTemporal() throws Exception {
		return getNanoTime();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getNanoTime().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS"));
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicNanoTime) || o == null)
			return false;
		else
			return getLong() == ((BasicNanoTime)o).getLong();
	}
}
