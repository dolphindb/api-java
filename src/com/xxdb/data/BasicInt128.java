package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.xxdb.io.Long2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicInt128 extends AbstractScalar {
	protected Long2 value;
	
	public BasicInt128(long high, long low){
		value = new Long2(high, low);
	}
	
	public BasicInt128(ExtendedDataInput in) throws IOException{
		value = in.readLong2();
	}
	
	public Long2 getLong2(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return value.isNull();
	}

	@Override
	public void setNull() {
		value.setNull();
	}
	
	public long getMostSignicantBits() {
		return value.high;
	}
	
	public long getLeastSignicantBits(){
		return value.low;
	}

	@Override
	public Number getNumber() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.BINARY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_INT128;
	}

	@Override
	public String getString() {
		return String.format("%016x%016x", value.high, value.low);
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicInt128) || o == null)
			return false;
		else
			return value.equals(((BasicInt128)o).value);
	}
	
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		return value.hashBucket(buckets);
	}

	@Override
	public String getJsonString() {
		return "\"" + getString() + "\"";
	}

	@Override
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeLong2(value);
	}
	
	public static BasicInt128 fromString(String num){
		if(num.length() != 32)
			throw new NumberFormatException("Invalid int128 string.");
		long high = Long.parseUnsignedLong(num.substring(0, 16), 16);
		long low = Long.parseUnsignedLong(num.substring(16), 16);
		return new BasicInt128(high, low);
	}

}
