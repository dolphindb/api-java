package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataOutput;

public abstract class AbstractScalar extends AbstractEntity implements Scalar{
	
	public abstract void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException;
	
	@Override
	public DATA_FORM getDataForm() {
		return Entity.DATA_FORM.DF_SCALAR;
	}
	
	@Override
	public int rows() {
		return 1;
	}
	@Override
	public int columns() {
		return 1;
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (DATA_FORM.DF_SCALAR.ordinal() << 8) + getDataType().getValue();
		out.writeShort(flag);
		writeScalarToOutputStream(out);
	}

	@Override
	public int getScale(){
		throw new RuntimeException("Not suppprt");
	}
	
	@Override
	public String toString(){
		return getString();
	}
}
