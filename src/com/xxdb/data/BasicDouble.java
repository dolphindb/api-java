package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.temporal.Temporal;

/**
 * 
 * Corresponds to DolphinDB double scalar
 *
 */

public class BasicDouble extends AbstractScalar implements Comparable<BasicDouble>{
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
		return DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DOUBLE;
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
		else if(Double.isNaN(value) || Double.isInfinite(value))
			return String.valueOf(value);
		else{
			double absVal = Math.abs(value);
			if((absVal>0 && absVal<=0.000001) || absVal>=1000000.0)
				return new DecimalFormat("0.######E0").format(value);
			else
				return new DecimalFormat("0.######").format(value);
		}
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
	
	@Override
	public int hashBucket(int buckets){
		return -1;
	}

	@Override
	public String getJsonString() {
		if(isNull()) return "null";
		return getString();
	}

	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException{
		out.writeDouble(value);
	}

	@Override
	public int compareTo(BasicDouble o) {
		return Double.compare(value, o.value);
	}
}
