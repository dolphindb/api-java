package com.xxdb.streaming.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.streaming.io.ExtendedDataInput;
import com.xxdb.streaming.io.ExtendedDataOutput;

public class BasicInt extends AbstractScalar implements Comparable<BasicInt>{
	private int value;

	public BasicInt(int value){
		this.value = value;
	}
	
	public BasicInt(ExtendedDataInput in) throws IOException{
		value = in.readInt();
	}
	
	public int getInt(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return  value == Integer.MIN_VALUE;
	}
	
	@Override
	public void setNull() {
		value = Integer.MIN_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_INT;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return null;
		else
			return new Integer(value);
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
		if(! (o instanceof BasicInt) || o == null)
			return false;
		else
			return value == ((BasicInt)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Integer(value).hashCode();
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeInt(value);
	}

	@Override
	public int compareTo(BasicInt o) {
		return Integer.compare(value, o.value);
	}
}
