package com.xxdb.compression;

import com.xxdb.data.Utils;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LZ4Encoder implements Encoder {
    LZ4Compressor compressor;
    private static final int DEFAULT_BLOCK_SIZE = 65536;

    @Override
    public List<Object> compress(ByteBuffer in, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException {
        if(compressor == null){
            LZ4Factory factory = LZ4Factory.fastestInstance();
            compressor = factory.fastCompressor();
        }
        int count = 0;
        while (in.limit() - in.position() > 0) {
            int blockSize = Math.min(DEFAULT_BLOCK_SIZE, in.limit() - in.position());
            byte[] srcBytes = new byte[blockSize];
            in.get(srcBytes);
            byte[] destBuff = compressor.compress(srcBytes, 0, blockSize);
            int compressedLength = destBuff.length;
            while(destBuff.length + 4 + out.position() > out.capacity()){
                out = Utils.reAllocByteBuffer(out, Math.max(65536, out.capacity() * 2));
            }
            out.putInt(compressedLength);
            out.put(destBuff, 0, compressedLength);
            count += Integer.BYTES + compressedLength;
        }
        List<Object> ret = new ArrayList<>();
        ret.add(count);
        ret.add(out);
        return ret;
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
