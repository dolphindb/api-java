package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataOutput;

public class Void extends AbstractScalar{

	@Override
	public boolean isNull() {
		return true;
	}

	@Override
	public void setNull() {
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.NOTHING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_VOID;
	}
	
	public Number getNumber() throws Exception{
		throw new Exception("Imcompatible data type");
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}
	
	@Override
	public String getString() {
		return "";
	}
	
	@Override
	public boolean equals(Object o){
		if(! (o instanceof Void) || o == null)
			return false;
		else
			return true;
	}
	
	@Override
	public int hashCode(){
		return 0;
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeBoolean(true); //explicit null value
	}
}
