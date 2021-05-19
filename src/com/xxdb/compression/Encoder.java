package com.xxdb.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Encoder {

    int compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException;

}
