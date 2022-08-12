package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

/**
 * 
 * Corresponds to DolphinDB datetime scalar
 *
 */

public class BasicDateTime extends BasicInt{

	public BasicDateTime(LocalDateTime value){
		super(Utils.countSeconds(value));
	}

	public BasicDateTime(Calendar value){
		super(Utils.countDTSeconds(value));
	}
	
	public BasicDateTime(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicDateTime(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DATETIME;
	}
	
	public LocalDateTime getDateTime(){
		if(isNull())
			return null;
		else
			return Utils.parseDateTime(getInt());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getDateTime();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getDateTime().format(DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss"));
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDateTime) || o == null)
			return false;
		else
			return getInt() == ((BasicDateTime)o).getInt();
	}
}
