package com.xxdb.io;

import java.io.IOException;
import java.io.InputStream;

public class BigEndianDataInputStream extends AbstractExtendedDataInputStream{

	public BigEndianDataInputStream(InputStream in) {
		super(in);
	}

	@Override
	public int readInt() throws IOException {
		byte b1 = readAndCheckByte();
		byte b2 = readAndCheckByte();
		byte b3 = readAndCheckByte();
		byte b4 = readAndCheckByte();
		return fromBytes(b1, b2, b3, b4);
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
		return fromBytes(b1, b2, b3, b4, b5, b6, b7, b8);
	}
	
	
	public Long2 readLong2() throws IOException {
		long high = readLong();
		long low = readLong();
		return new Long2(high, low);
	}

	@Override
	public int readUnsignedShort() throws IOException {
		byte b1 = readAndCheckByte();
		byte b2 = readAndCheckByte();
		return fromBytes(b1, b2, (byte)0, (byte)0);
	}

	@Override
	public boolean isLittleEndian() {
		return false;
	}
}
