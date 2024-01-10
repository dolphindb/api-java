package com.xxdb.compression;

import com.xxdb.data.BasicDecimal32Vector;
import com.xxdb.data.BasicDecimal64Vector;
import com.xxdb.data.Vector;

import java.io.IOException;

import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.data.EntityFactory;
import com.xxdb.io.ExtendedDataInput;

public class VectorDecompressor {
	
	public Vector decompress(EntityFactory factory, ExtendedDataInput in, boolean extended, boolean isLittleEndian) throws IOException{
		int compressedBytes = in.readInt();
		in.skipBytes(7);
		int compression = in.readByte();
		int dataType = in.readByte();
		int unitLength = in.readByte();
		short reserved = in.readShort();
		int extra = 0;
		extra = in.readInt();
		int elementCount = in.readInt();
		//read checkSum
		in.readInt();
		int tmp = extra;
		if (dataType < DATA_TYPE.DT_BOOL_ARRAY.getValue())
			extra=1;
		ExtendedDataInput decompressedIn = DecoderFactory.get(compression).	decompress(in, compressedBytes - 20, unitLength, elementCount, isLittleEndian, extra, dataType, reserved);
		DATA_TYPE dt = DATA_TYPE.valueOf(dataType);
		if (dt == DATA_TYPE.DT_DECIMAL32)
			return new BasicDecimal32Vector(DATA_FORM.DF_VECTOR, decompressedIn, tmp);
		else if (dt == DATA_TYPE.DT_DECIMAL64)
			return new BasicDecimal64Vector(DATA_FORM.DF_VECTOR, decompressedIn, tmp);
		else
			return (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, dt, decompressedIn, extended, -1); // Haotian - TODO: this function `decompress` may need to be updated
	}

}
