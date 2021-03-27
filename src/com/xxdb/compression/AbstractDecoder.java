package com.xxdb.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractDecoder implements Decoder {

	protected ByteBuffer createColumnVector(int rows, int unitLength, boolean isLittleEndian){
		ByteBuffer out = ByteBuffer.allocate(rows * unitLength + 8).order(isLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
		out.putInt(rows);
		out.putInt(1);
		return out;
	}
}
