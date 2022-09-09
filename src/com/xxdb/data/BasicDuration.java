package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDuration extends AbstractScalar implements Comparable<BasicDuration>{
	private static final String[] unitSyms = {"ns", "us", "ms", "s", "m", "H", "d", "w", "M", "y", "B"};
	private int value;
	private DURATION unit;

	public BasicDuration(DURATION unit, int value){
		this.value = value;
		this.unit = unit;
	}

	public BasicDuration(ExtendedDataInput in) throws IOException{
		value = in.readInt();
		unit = DURATION.values()[in.readInt()];
	}

	public int getDuration() {
		return value;
	}

	public DURATION getUnit() {
		return unit;
	}

	@Override
	public boolean isNull() {
		return value == Integer.MIN_VALUE;
	}

	@Override
	public void setNull() {
		value = Integer.MIN_VALUE;
	}

	@Override
	public Number getNumber() throws Exception {
		return value;
	}

	@Override
	public Temporal getTemporal() throws Exception {
		throw new Exception("Imcompatible data type");
	}

	@Override
	public int hashBucket(int buckets) {
		return 0;
	}

	@Override
	public String getJsonString() {
		return getString();
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.SYSTEM;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DURATION;
	}

	@Override
	public String getString() {
		if(value == Integer.MIN_VALUE)
			return "";
		else
			return String.valueOf(value) + unitSyms[unit.ordinal()];
	}

	@Override
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeInt(value);
		out.writeInt(unit.ordinal());
	}

	@Override
	public int compareTo(BasicDuration o) {
		if(unit==o.unit)
			return Integer.compare(((BasicDuration)o).value,value);
		else
			return -1;
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDuration) || o == null)
			return false;
		else
			return value == ((BasicDuration)o).value && unit == ((BasicDuration)o).unit;
	}
}
