package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphindB long scalar
 *
 */

public class BasicLong extends AbstractScalar implements Comparable<BasicLong>{
	private long value;

	public BasicLong(long value){
		this.value = value;
	}
	
	public BasicLong(ExtendedDataInput in) throws IOException{
		value = in.readLong();
	}
	
	public long getLong(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return  value == Long.MIN_VALUE;
	}
	
	@Override
	public void setNull() {
		value = Long.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_LONG;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return Long.MIN_VALUE;
		else
			return value;
	}

	public Long longValue() throws Exception {
		if (isNull()) {
			return null;
		} else {
			return value;
		}
	}

	@JsonIgnore
	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else
			return String.valueOf(value);
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicLong) || o == null)
			return false;
		else
			return value == ((BasicLong)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Long(value).hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		if(value >= 0)
			return (int)(value % buckets);
		else if(value == Long.MIN_VALUE)
			return -1;
		else{
			return (int)(((Long.MAX_VALUE % buckets) +2 + ((Long.MAX_VALUE + value) % buckets)) % buckets);
		}
	}

	@Override
	public int getScale(){
		return 0;
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return getString();
	}

	public void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeLong(value);
	}

	@Override
	public int compareTo(BasicLong o) {
		return Long.compare(value, o.value);
	}
}
