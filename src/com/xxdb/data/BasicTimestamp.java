package com.xxdb.data;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;

/**
 * 
 * Corresponds to DolphinDB timestamp scalar
 *
 */

public class BasicTimestamp extends BasicLong{

	public BasicTimestamp(LocalDateTime value){
		super(Utils.countMilliseconds(value));
	}
	
	public BasicTimestamp(ExtendedDataInput in) throws IOException {
		super(in);
	}
	
	protected BasicTimestamp(long value){
		super(value);
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_TIMESTAMP;
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
			return getTimestamp().toString();
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicTimestamp) || o == null)
			return false;
		else
			return getLong() == ((BasicTimestamp)o).getLong();
	}
}
