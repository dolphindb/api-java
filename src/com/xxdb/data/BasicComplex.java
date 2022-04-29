package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Double2;

public class BasicComplex extends AbstractScalar{
	protected Double2 value;
	
	public BasicComplex(double real, double image){
		value = new Double2(real, image);
	}
	
	public BasicComplex(ExtendedDataInput in) throws IOException{
		value = in.readDouble2();
	}
	
	public Double2 getDouble2(){
		return value;
	}
	
	@Override
	public boolean isNull() {
		return value.isNull();
	}

	@Override
	public void setNull() {
		value.setNull();
	}
	
	public double getReal() {
		return value.x;
	}
	
	public double getImage(){
		return value.y;
	}

	@Override
	public Number getNumber() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.BINARY;
	}

	@Override
	public DATA_TYPE getDataType() {
		return Entity.DATA_TYPE.DT_COMPLEX;
	}

	@Override
	public String getString() {
		if(isNull())
			return "";
		StringBuilder sb = new StringBuilder();
		sb.append(value.x);
		if(value.y >= 0)
			sb.append('+');
		sb.append(value.y);
		sb.append('i');
		return sb.toString();
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicComplex) || o == null)
			return false;
		else
			return value.equals(((BasicComplex)o).value);
	}
	
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	
	@Override
	public int hashBucket(int buckets){
		return value.hashBucket(buckets);
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return "\"" + getString() + "\"";
	}

	@Override
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeDouble2(value);
	}
}
