package com.xxdb.data;

import java.io.IOException;
import java.time.temporal.Temporal;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicDuration extends AbstractScalar implements Comparable<BasicDuration>{
	private static final String[] unitSyms = {"ns", "us", "ms", "s", "m", "H", "d", "w", "M", "y", "B"};
	private int value;
	private DURATION unit;
	private int exchange;

	public BasicDuration(DURATION unit, int value){
		this.value = value;
		if (unit == DURATION.TDAY)
			throw new RuntimeException("the exchange unit should be given as String when use exchange calendar.");
		this.unit = unit;
		this.exchange = unit.ordinal();
	}

	public BasicDuration(String unit, int value) {
		this.value = value;
		int codeNumber = getCodeNumber(unit);
		if (codeNumber >= 0 && codeNumber <= 10)
			this.unit = DURATION.values()[codeNumber];
		else
			this.unit = DURATION.TDAY;
		this.exchange = codeNumber;
	}

	protected BasicDuration(int exchange, int value) {
		this.value = value;
		if (exchange >= 0 && exchange <= 10)
			this.unit = DURATION.values()[exchange];
		else
			this.unit = DURATION.TDAY;
		this.exchange = exchange;
	}

	public BasicDuration(ExtendedDataInput in) throws IOException{
		value = in.readInt();
		int codeNumber = in.readInt();
		if (codeNumber >= 0 && codeNumber <= 10)
			unit = DURATION.values()[codeNumber];
		else
			unit = DURATION.TDAY;
		this.exchange = codeNumber;
	}

	public int getDuration() {
		return value;
	}

	public DURATION getUnit() {
		return unit;
	}

	public int getExchangeInt() {
		return this.exchange;
	}

	public String getExchangeName() {
		if (this.exchange >= 0 && this.exchange <= 10)
			return this.unitSyms[this.exchange];

		char[] codes = new char[4];
		codes[0] = (char) ((this.exchange >> 24) & 255);
		codes[1] = (char) ((this.exchange >> 16) & 255);
		codes[2] = (char) ((this.exchange >> 8) & 255);
		codes[3] = (char) (this.exchange & 255);
		return String.valueOf(codes);
	}

	@Override
	public boolean isNull() {
		return this.value == Integer.MIN_VALUE;
	}

	@Override
	public void setNull() {
		this.value = Integer.MIN_VALUE;
	}

	@Override
	public Number getNumber() throws Exception {
		return this.value;
	}

	@JsonIgnore
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
		if (this.value == Integer.MIN_VALUE)
			return "";
		else
			return this.value + getExchangeName();
	}

	@Override
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeInt(this.value);
		out.writeInt(this.exchange);
	}

	@Override
	public int compareTo(BasicDuration o) {
		if (this.unit == o.unit && this.exchange == o.exchange)
			return Integer.compare(o.value, this.value);
		else
			return -1;
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDuration) || o == null)
			return false;
		else
			return this.value == ((BasicDuration) o).value && this.unit == ((BasicDuration) o).unit && this.exchange == ((BasicDuration) o).exchange;
	}

	@JsonIgnore
	@Override
	public int getScale(){
		return super.getScale();
	}

	private int getCodeNumber(String unit) {
		if (unit.length() == 1 || unit.length() == 2) {
			switch (unit) {
				case "ns":
					return 0;
				case "us":
					return 1;
				case "ms":
					return 2;
				case "s":
					return 3;
				case "m":
					return 4;
				case "H":
					return 5;
				case "d":
					return 6;
				case "w":
					return 7;
				case "M":
					return 8;
				case "y":
					return 9;
				case "B":
					return 10;
				default:
					throw new RuntimeException("error unit: " + unit);
			}
		} else if (unit.length() == 4) {
			int[] codes = new int[4];
			for (int i = 0; i < 4; i++) {
				if (!Character.isUpperCase(unit.charAt(i)))
					throw new RuntimeException("The value of unit must duration enum type or contain four consecutive uppercase letters.");
				codes[i] = unit.charAt(i);
			}
			return (codes[0] << 24) + (codes[1] << 16) + (codes[2] << 8) + codes[3];
		} else {
			throw new RuntimeException("The value of unit must duration enum type or contain four consecutive uppercase letters.");
		}
	}
}
