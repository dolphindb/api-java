package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicString extends AbstractScalar{
	private String value;

	public BasicString(String value){
		this.value = value;
	}
	
	public BasicString(ExtendedDataInput in) throws IOException{
		value = in.readString();
	}
	
	public BasicString(ExtendedDataInput in, DATA_TYPE type) throws IOException{
		if(type==DATA_TYPE.DT_FUNCTIONDEF)
			in.readByte();
		value = in.readString();
	}
	
	public String getString(){
		return value;
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
}
