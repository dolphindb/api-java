package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB string scalar
 *
 */

public class BasicString extends AbstractScalar implements Comparable<BasicString>{
	private String value;

	public BasicString(String value){
		this.value = value;
	}
	
	public BasicString(ExtendedDataInput in) throws IOException{
		value = in.readString();
	}
	
	public String getString(){
		return value;
	}
	
	protected void setString(String value){
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return  value.isEmpty();
	}
	
	@Override
	public void setNull() {
		value = "";
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_STRING;
	}
	
	public Number getNumber() throws Exception{
		throw new Exception("Imcompatible data type");
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicString) || o == null)
			return false;
		else
			return value.equals(((BasicString)o).value);
	}
	
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeString(value);
	}

	@Override
	public int compareTo(BasicString o) {
		return value.compareTo(o.value);
	}
}
