package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

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
			return null;
		else
			return new Short(value);
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
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeShort(value);
	}

	@Override
	public int compareTo(BasicShort o) {
		return Short.compare(value, o.value);
	}
}
