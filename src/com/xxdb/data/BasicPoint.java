package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicPoint extends AbstractScalar{
	protected Double2 value;
	
	public BasicPoint(double real, double image){
		value = new Double2(real, image);
	}
	
	public BasicPoint(ExtendedDataInput in) throws IOException{
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
	
	public double getX() {
		return value.x;
	}
	
	public double getY(){
		return value.y;
	}

	@JsonIgnore
	@Override
	public Number getNumber() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@JsonIgnore
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
		return Entity.DATA_TYPE.DT_POINT;
	}

	@Override
	public String getString() {
		if(isNull())
			return "(,)";
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		sb.append(value.x);
		sb.append(", ");
		sb.append(value.y);
		sb.append(')');
		return sb.toString();
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicPoint) || o == null)
			return false;
		else
			return value.equals(((BasicPoint)o).value);
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

	@JsonIgnore
	@Override
	public int getScale() {
		return super.getScale();
	}
}
