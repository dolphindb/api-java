package com.xxdb.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface Encoder {

    List<Object> compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException;

}
