package com.xxdb.io;

import java.io.IOException;
import java.io.InputStream;

public class LittleEndianDataInputStream  extends AbstractExtendedDataInputStream {

	public LittleEndianDataInputStream(InputStream in) {
		super(in);
	}

	@Override
	public int readInt() throws IOException {
		byte b1 = readAndCheckByte();
		byte b2 = readAndCheckByte();
		byte b3 = readAndCheckByte();
		byte b4 = readAndCheckByte();
		return fromBytes( b4, b3, b2, b1);
	}

	@Override
	public long readLong() throws IOException {
		byte b1 = readAndCheckByte();
		byte b2 = readAndCheckByte();
		byte b3 = readAndCheckByte();
		byte b4 = readAndCheckByte();
		byte b5 = readAndCheckByte();
		byte b6 = readAndCheckByte();
		byte b7 = readAndCheckByte();
		byte b8 = readAndCheckByte();
		return fromBytes( b8, b7, b6, b5, b4, b3, b2, b1);
	}
	
	public Long2 readLong2() throws IOException {
		long low = readLong();
		long high = readLong();
		return new Long2(high, low);
	}
	
	public Double2 readDouble2() throws IOException {
		double x = readDouble();
		double y = readDouble();
		return new Double2(x, y);
	}

	@Override
	public int readUnsignedShort() throws IOException {
		byte b1 = readAndCheckByte();
		byte b2 = readAndCheckByte();
		return fromBytes((byte)0, (byte)0, b2, b1);
	}

	@Override
	public boolean isLittleEndian() {
		return true;
	}

}
