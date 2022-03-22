package com.xxdb.compression;

import com.xxdb.data.AbstractVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface Encoder {

    int compress(AbstractVector input, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException;

}
