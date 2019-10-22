package com.xxdb.data;

import java.io.IOException;
import java.util.UUID;

import com.xxdb.io.ExtendedDataInput;

public class BasicUuid extends BasicInt128 {

	public BasicUuid(long high, long low){
		super(high, low);
	}
	
	BasicUuid(ExtendedDataInput in) throws IOException{
		super(in);
	}
	
	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_UUID;
	}

	@Override
	public String getString() {
		return new UUID(value.high, value.low).toString();
	}
	
	public static BasicUuid fromString(String name){
		UUID uuid = UUID.fromString(name);
		return new BasicUuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
	}
}
