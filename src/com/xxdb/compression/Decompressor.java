package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;
import com.xxdb.io.ExtendedDataInput;

import java.nio.ByteBuffer;

public class Decompressor {
    Decoder decoder;

    public Decompressor(ExtendedDataInput in, int inLength, int dataSize, int unitLength, int compression) {
        if (!(in instanceof AbstractExtendedDataInputStream)) {
            throw new RuntimeException("Cannot decompress the input: not a class of AbstractExtendedInputStream");
        }
        ByteBuffer dest = ByteBuffer.allocate(unitLength * dataSize + 20);
        if (compression == 1) {
            DecoderUtil.createColumnHeader(dest, dataSize, 1, true);
            decoder = DecoderFactory.createLZ4Decoder();
        } else {
            DecoderUtil.createColumnHeader(dest, dataSize, 1, false);
            decoder = DecoderFactory.createDeltaOfDeltaDecoder();
        }
        byte[] out = dest.array();
        decoder.decompress((AbstractExtendedDataInputStream) in, inLength, out, 8, dataSize * unitLength, unitLength);
    }

    public ExtendedDataInput decompress() {
        return decoder.getInputStream();
    }
}
