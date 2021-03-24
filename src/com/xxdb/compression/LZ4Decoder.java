package com.xxdb.compression;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class LZ4Decoder {
    byte[] dest = null;
    public LZ4Decoder(byte[] src, int blockSize, int dataSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        dest = new byte[dataSize * Long.BYTES];
        int count = 0;
        while ((count + 1) * blockSize <= dataSize) {
            decompressor.decompress(src, count * blockSize, dest, 20 + count * blockSize, blockSize); //FIXME: dest cannot directly use blockSize
            count++;
        }
        if (dataSize - count * blockSize > 0)
            decompressor.decompress(src, count * blockSize, dest, 20 + count * blockSize, dataSize - count * blockSize);
    }

    public byte[] decompress() {
        return dest;
    }
}
