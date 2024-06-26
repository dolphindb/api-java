package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

/**
 * 
 * Corresponds to DolphinDB nanotimestamp scalar
 *
 */

public class BasicNanoTimestamp extends BasicLong{

	public BasicNanoTimestamp(LocalDateTime value){
		super(Utils.countDTNanoseconds(value));
	}

	public BasicNanoTimestamp(ExtendedDataInput in) throws IOException {
		super(in);
	}

	public BasicNanoTimestamp(long value){
		super(value);
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.TEMPORAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_NANOTIMESTAMP;
	}
	
	public LocalDateTime getNanoTimestamp(){
		if(isNull())
			return null;
		else
			return Utils.parseNanoTimestamp(getLong());
	}
	
	@Override
	public Temporal getTemporal() throws Exception {
		return getNanoTimestamp();
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return getNanoTimestamp().format(DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS"));
	}
	@Override
	public String getJsonString() {
		if (isNull()) return "null";
		return "\"" + getString() + "\"";
	}
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicNanoTimestamp) || o == null)
			return false;
		else
			return getLong() == ((BasicNanoTimestamp)o).getLong();
	}
}
