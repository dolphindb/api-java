package com.xxdb.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractDecoder implements Decoder {

	protected ByteBuffer createColumnVector(int rows, int unitLength, boolean isLittleEndian, int minSize, int extra){
		ByteBuffer out = ByteBuffer.allocate(Math.max(rows * unitLength + 8, minSize)).order(isLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
		out.putInt(rows);
		out.putInt(extra);
		return out;
	}

	protected ByteBuffer createLZ4ColumnVector(int rows, boolean isLittleEndian, int extra){
		ByteBuffer out = ByteBuffer.allocate(8).order(isLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
		out.putInt(rows);
		out.putInt(extra);
		return out;
	}
}
