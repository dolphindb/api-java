
package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB BYTE object
 *
 */

public class BasicByte extends AbstractScalar implements Comparable<BasicByte>{
	private byte value;

	public BasicByte(byte value){
		this.value = value;
	}
	
	public BasicByte(ExtendedDataInput in) throws IOException{
		value = in.readByte();
	}
	
	public byte getByte(){
		return value;
	}
	
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
		return Entity.DATA_CATEGORY.INTEGRAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_BYTE;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return null;
		else
			return new Byte(value);
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
		if(! (o instanceof BasicByte) || o == null)
			return false;
		else
			return value == ((BasicByte)o).value;
	}
	
	@Override
	public int hashCode(){
		return new Byte(value).hashCode();
	}
	
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeByte(value);
	}

	@Override
	public int compareTo(BasicByte o) {
		return Byte.compare(value, o.value);
	}
}
