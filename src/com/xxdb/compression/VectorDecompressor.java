package com.xxdb.compression;

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
		in.skipBytes(6);
		int elementCount = in.readInt();
		//read checkSum
		in.readInt();
		
		ExtendedDataInput decompressedIn = DecoderFactory.get(compression).	decompress(in, compressedBytes - 20, unitLength, elementCount, isLittleEndian);
		DATA_TYPE dt = DATA_TYPE.values()[dataType];
		return (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, dt, decompressedIn, extended);
	}

}
