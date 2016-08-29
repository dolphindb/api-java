package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicFloat extends AbstractScalar{
	private float value;

	public BasicFloat(float value){
		this.value = value;
	}
	
	public BasicFloat(ExtendedDataInput in) throws IOException{
		value = in.readFloat();
	}
	
	public float getFloat(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return  value == -Float.MAX_VALUE;
	}
	
	@Override
	public void setNull() {
		value = -Float.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_FLOAT;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return null;
		else
			return new Float(value);
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
		if(! (o instanceof BasicFloat) || o == null)
			return false;
		else
			return value == ((BasicFloat)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Float(value).hashCode();
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeFloat(value);
	}
}
