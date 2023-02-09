package com.xxdb.compression;

import com.xxdb.data.Entity;
import com.xxdb.data.Utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractDecoder implements Decoder {

	protected ByteBuffer createColumnVector(int rows, int unitLength, boolean isLittleEndian, int minSize, int extra, int type, short scale){
		ByteBuffer out = ByteBuffer.allocate(Math.max(rows * unitLength + 16, minSize)).order(isLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
		out.putInt(rows);
		out.putInt(extra);
		Entity.DATA_TYPE dataType = Entity.DATA_TYPE.valueOf(type);
		if(Utils.getCategory(dataType) == Entity.DATA_CATEGORY.DENARY || dataType == Entity.DATA_TYPE.DT_DECIMAL32_ARRAY || dataType == Entity.DATA_TYPE.DT_DECIMAL64_ARRAY || dataType == Entity.DATA_TYPE.DT_DECIMAL128_ARRAY){
			out.putInt(scale);
		}
		return out;
	}

	protected ByteBuffer createLZ4ColumnVector(int rows, boolean isLittleEndian, int extra, int type, short scale){
		Entity.DATA_TYPE dataType = Entity.DATA_TYPE.valueOf(type);
		boolean decimalType = Utils.getCategory(dataType) == Entity.DATA_CATEGORY.DENARY || dataType == Entity.DATA_TYPE.DT_DECIMAL32_ARRAY || dataType == Entity.DATA_TYPE.DT_DECIMAL64_ARRAY || dataType == Entity.DATA_TYPE.DT_DECIMAL128_ARRAY;
		int size = decimalType ? 12 : 8;
		ByteBuffer out = ByteBuffer.allocate(size).order(isLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
		out.putInt(rows);
		out.putInt(extra);
		if(decimalType){
			out.putInt(scale);
		}
		return out;
	}
}
