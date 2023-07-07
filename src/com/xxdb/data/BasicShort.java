package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB short scalar
 *
 */

public class BasicShort extends AbstractScalar implements Comparable<BasicShort>{
	private short value;

	public BasicShort(short value){
		this.value = value;
	}
	
	public BasicShort(ExtendedDataInput in) throws IOException{
		value = in.readShort();
	}
	
	public short getShort(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return  value == Short.MIN_VALUE;
	}
	
	@Override
	public void setNull() {
		value = Short.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_SHORT;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return Short.MIN_VALUE;
		else
			return value;
	}

	public short shortValue() throws Exception {
		return this.getNumber().shortValue();
	}

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
		if(! (o instanceof BasicShort) || o == null)
			return false;
		else
			return value == ((BasicShort)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Short(value).hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		if(value >= 0)
			return value % buckets;
		else if(value == Short.MIN_VALUE)
			return -1;
		else{
			return (int)((4294967296l + value) % buckets);
		}
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return getString();
	}

	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeShort(value);
	}

	@Override
	public int compareTo(BasicShort o) {
		return Short.compare(value, o.value);
	}
}
