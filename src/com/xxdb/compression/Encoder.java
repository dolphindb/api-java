package com.xxdb.compression;

import com.xxdb.io.ExtendedDataOutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface Encoder {

    int compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException;

}
