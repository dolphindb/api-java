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
	private int exchange_;

	public BasicDuration(DURATION unit, int value){
		this.value = value;
		if (unit == DURATION.TDAY)
			throw new RuntimeException("the exchange unit should be given when use exchange calendar.");
		this.unit = unit;
		this.exchange_ = unit.ordinal();
	}

	public BasicDuration(DURATION unit, int value, String exchangeUnit) {
		this.value = value;
		this.unit = unit;
		if (this.unit == DURATION.TDAY)
			this.exchange_ = getCodeNumber(exchangeUnit);
		else
			this.exchange_ = this.unit.ordinal();
	}

	public BasicDuration(String unit, int value) {
		this.value = value;
		int codeNumber = getCodeNumber(unit);
		if (codeNumber >= 0 && codeNumber <= 10)
			this.unit = DURATION.values()[codeNumber];
		else
			this.unit = DURATION.TDAY;
		this.exchange_ = codeNumber;
	}

	protected BasicDuration(int exchange_, int value) {
		this.value = value;
		if (exchange_ >= 0 && exchange_ <= 10)
			this.unit = DURATION.values()[exchange_];
		else
			this.unit = DURATION.TDAY;
		this.exchange_ = exchange_;
	}

	public BasicDuration(ExtendedDataInput in) throws IOException{
		value = in.readInt();
		int codeNumber = in.readInt();
		if (codeNumber >= 0 && codeNumber <= 10)
			unit = DURATION.values()[codeNumber];
		else
			unit = DURATION.TDAY;
		exchange_ = codeNumber;
	}

	public int getDuration() {
		return value;
	}

	public DURATION getUnit() {
		return unit;
	}

	public int getExchangeInt() {
		return exchange_;
	}

	public String getExchangeName() {
		if (exchange_ >= 0 && exchange_ <= 10) {
			return unitSyms[exchange_];
		}
		char[] codes = new char[4];
		codes[0] = (char) ((exchange_ >> 24) & 255);
		codes[1] = (char) ((exchange_ >> 16) & 255);
		codes[2] = (char) ((exchange_ >> 8) & 255);
		codes[3] = (char) (exchange_ & 255);
		return String.valueOf(codes);
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
		if(value == Integer.MIN_VALUE)
			return "";
		else
			return value + getExchangeName();
	}

	@Override
	protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
		out.writeInt(value);
		out.writeInt(exchange_);
	}

	@Override
	public int compareTo(BasicDuration o) {
		if (unit == o.unit && exchange_ == o.exchange_)
			return Integer.compare(o.value, value);
		else
			return -1;
	}

	@Override
	public boolean equals(Object o){
		if(! (o instanceof BasicDuration) || o == null)
			return false;
		else
			return value == ((BasicDuration) o).value && unit == ((BasicDuration) o).unit && exchange_ == ((BasicDuration) o).exchange_;
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
					throw new RuntimeException("except upper letter, got " + unit);
				codes[i] = unit.charAt(i);
			}
			return (codes[0] << 24) + (codes[1] << 16) + (codes[2] << 8) + codes[3];
		} else {
			throw new RuntimeException(String.format("except duration enum type or length 4, got %s and length %d", unit, unit.length()));
		}
	}
}
