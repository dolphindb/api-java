package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicSystemEntity extends BasicString {
	private DATA_TYPE type;

	public BasicSystemEntity(ExtendedDataInput in, DATA_TYPE type) throws IOException{
		super("");
		this.type = type;
		if(type==DATA_TYPE.DT_FUNCTIONDEF)
			in.readByte();
		setString(in.readString());
	}
	
	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.SYSTEM;
	}

	@Override
	public DATA_TYPE getDataType() {
		return type;
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		throw new IOException("System entity is not supposed to serialize.");
	}
}
