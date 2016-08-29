package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDouble extends AbstractScalar{
	private double value;

	public BasicDouble(double value){
		this.value = value;
	}
	
	public BasicDouble(ExtendedDataInput in) throws IOException{
		value = in.readDouble();
	}
	
	public double getDouble(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return  value == -Double.MAX_VALUE;
	}
	
	@Override
	public void setNull() {
		value = -Double.MAX_VALUE;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_DOUBLE;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return null;
		else
			return new Double(value);
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
		if(! (o instanceof BasicDouble) || o == null)
			return false;
		else
			return value == ((BasicDouble)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Double(value).hashCode();
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeDouble(value);
	}
}
