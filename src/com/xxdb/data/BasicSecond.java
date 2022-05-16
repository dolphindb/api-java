package com.xxdb.data;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Calendar;

import com.xxdb.io.ExtendedDataInput;


/**
 * 
 * Corresponds to DolphinDB second scalar
 *
 */

public class BasicSecond extends BasicInt{

	public BasicSecond(LocalTime value){
		super(Utils.countSeconds(value));
	}

	public BasicSecond(Calendar value){
		super(Utils.countSeconds(value));
	}

	public BasicSecond(ExtendedDataInput in) throws IOException {
		super(in);
	}

	//TODO: change to public
	public BasicSecond(int value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_SECOND;
	}
	
	public LocalTime getSecond(){
		if(isNull())
			return null;
		else
			return Utils.parseSecond(getInt());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getSecond();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getSecond().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicSecond) || o == null)
			return false;
		else
			return getInt() == ((BasicSecond)o).getInt();
	}
}
