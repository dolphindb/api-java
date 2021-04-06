package com.xxdb.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.*;
import java.nio.ByteBuffer;

public class LZ4Encoder extends AbstractEncoder{
    LZ4Compressor compressor;
    private static final int DEFAULT_BLOCK_SIZE = 65536;

    @Override
    public int compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException {
        if(compressor == null){
            LZ4Factory factory = LZ4Factory.fastestInstance();
            compressor = factory.fastCompressor();
        }
        int count = 0;
        while (elementCount > 0 && count < maxCompressedLength) {
            int blockSize = Math.min(DEFAULT_BLOCK_SIZE, elementCount * unitLength);
            byte[] src = new byte[blockSize];
            byte[] dest = new byte[maxCompressedLength];
            in.get(src);
            int compressedLength = compressor.compress(src, dest);
            //write
            out.putInt(compressedLength);
            out.put(dest, 0, compressedLength);
            count+=Integer.BYTES + compressedLength;
            elementCount -= blockSize / unitLength;
        }
        return count;
    }

    public ByteBuffer compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength) throws IOException {
        if(compressor == null){
            LZ4Factory factory = LZ4Factory.fastestInstance();
            compressor = factory.fastCompressor();
        }
        ByteBuffer out = ByteBuffer.allocate(maxCompressedLength);
        int count = 0;
        while (elementCount > 0 && count < maxCompressedLength) {
            int blockSize = Math.min(DEFAULT_BLOCK_SIZE, elementCount * unitLength);
            byte[] src = new byte[blockSize];
            byte[] dest = new byte[blockSize];
            in.get(src);
            int compressedLength = compressor.compress(src, 0, blockSize, dest, 0);
            //write
            out.putInt(compressedLength);
            out.put(dest, 0, compressedLength);
            count+=Integer.BYTES;
            count += compressedLength;
            elementCount -= blockSize / unitLength;
        }
        out.flip();
        return out;
    }
}
