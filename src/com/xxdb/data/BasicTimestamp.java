package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

/**
 * 
 * Corresponds to DolphinDB timestamp scalar
 *
 */

public class BasicTimestamp extends BasicLong{
	
	public BasicTimestamp(LocalDateTime value){
		super(Utils.countMilliseconds(value));
	}
	public BasicTimestamp(Calendar value){
		super(Utils.countDateMilliseconds(value));
	}
	
	public BasicTimestamp(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public BasicTimestamp(long value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_TIMESTAMP;
	}
	
	public LocalDateTime getTimestamp(){
		if(isNull())
			return null;
		else
			return Utils.parseTimestamp(getLong());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getTimestamp();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getTimestamp().format(DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSS"));
	}

	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicTimestamp) || o == null)
			return false;
		else
			return getLong() == ((BasicTimestamp)o).getLong();
	}
}
