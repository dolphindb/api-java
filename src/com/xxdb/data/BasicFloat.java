package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.temporal.Temporal;

import static com.xxdb.data.Utils.SCALE;

/**
 * 
 * Corresponds to DolphinDB float scalar
 *
 */

public class BasicFloat extends AbstractScalar implements Comparable<BasicFloat>{
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
		return DATA_CATEGORY.FLOATING;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_FLOAT;
	}
	
	public Number getNumber() throws Exception{
		if(isNull())
			return -Float.MAX_VALUE;
		else
			return value;
	}

	public Float floatValue() throws Exception {
		if (isNull()) {
			return null;
		} else {
			return value;
		}
	}

	@JsonIgnore
	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}
	
	@Override
	public String getString() {
		if(isNull())
			return "";
		else if(Float.isNaN(value) || Float.isInfinite(value))
			return String.valueOf(value);
		else{
			if (SCALE < 0) {
				//AJ-279
				return new DecimalFormat("0.########").format(value);
				//float absVal = Math.abs(value);
				//if ((absVal > 0 && absVal <= 0.000001) || absVal >= 1000000.0)
				//	return new DecimalFormat("0.######E0").format(value);
				//else
				//	return new DecimalFormat("0.######").format(value);
			}else {
				BigDecimal bd;
				if(value >= 10000000.0){
					bd = new BigDecimal(value);
					return bd.setScale(SCALE, RoundingMode.DOWN).toString();
				}else if (value == 0){
					bd = new BigDecimal(value + "");
					return String.valueOf(0);
				}else {
					bd = new BigDecimal(value + "");
					return bd.setScale(SCALE, RoundingMode.DOWN).toString();
				}
			}
		}
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
		out.writeFloat(value);
	}
	
	@Override
	public int compareTo(BasicFloat o) {
		return Float.compare(value, o.value);
	}

	@JsonIgnore
	@Override
	public int getScale() {
		return super.getScale();
	}
}
