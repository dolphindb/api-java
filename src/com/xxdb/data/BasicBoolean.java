package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB bool scalar
 *
 */

public class BasicBoolean extends AbstractScalar implements Comparable<BasicBoolean>{
	private byte value;

	public BasicBoolean(boolean value){
		this.value = value ? (byte)1 : (byte)0;
	}
	
	public BasicBoolean(ExtendedDataInput in) throws IOException{
		value = in.readByte();
	}
	
	protected BasicBoolean(byte value){
		this.value = value;
	}
	
	public boolean getBoolean(){
		return value != 0;
	}

	public byte getValue() {
		return value;
	}

	@JsonIgnore
	@Override
	public boolean isNull() {
		return  value == Byte.MIN_VALUE;
	}
	
	@Override
	public void setNull() {
		value = Byte.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LOGICAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_BOOL;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return Byte.MIN_VALUE;
		else
			return value;
	}

	public Boolean booleanValue() throws Exception {
		if (isNull()) {
			return null;
		} else {
			return this.getNumber().byteValue() == 1;
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
			return String.valueOf(getBoolean());
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicBoolean) || o == null)
			return false;
		else
			return value == ((BasicBoolean)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Byte(value).hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		return -1;
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return getString();
	}

	public void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeByte(value);
	}

	@Override
	public int compareTo(BasicBoolean o) {
		return  Byte.compare(value, o.value);
	}

	@JsonIgnore
	@Override
	public int getScale() {
		return super.getScale();
	}
}
