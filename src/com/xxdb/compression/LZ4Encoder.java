package com.xxdb.compression;

import com.xxdb.data.AbstractVector;

import com.xxdb.data.BasicStringVector;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


import static com.xxdb.data.Utils.reAllocByteBuffer;

public class LZ4Encoder implements Encoder {
    LZ4Compressor compressor;
    private static final int DEFAULT_BLOCK_SIZE = 65536;

    @Override
    public ByteBuffer compress(AbstractVector input, int elementCount, int unitLength, int maxCompressedLength, ByteBuffer out) throws IOException {
        if(compressor == null){
            LZ4Factory factory = LZ4Factory.fastestInstance();
            compressor = factory.fastCompressor();
        }
        int byteCount = 0;
        int dataCount = input.rows();
        int dataIndex = 0;
        ByteBuffer dataBufer = ByteBuffer.allocate(DEFAULT_BLOCK_SIZE);
        boolean isLittleEndian = out.order() == ByteOrder.LITTLE_ENDIAN;
        if (isLittleEndian)
            dataBufer.order(ByteOrder.LITTLE_ENDIAN);
        else
            dataBufer.order(ByteOrder.BIG_ENDIAN);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(0, 0);
        while (dataCount > dataIndex)
        {
            int readBytes = input.serialize(dataIndex, numElementAndPartial.partial, dataCount-dataIndex, numElementAndPartial, dataBufer);
            while(readBytes > 0)
            {
                int blockSize = Math.min(DEFAULT_BLOCK_SIZE, dataBufer.position());
                byte[] srcBuf = new byte[blockSize];
                dataBufer.flip();
                dataBufer.get(srcBuf, 0, blockSize);
                byte[] ret = compressor.compress(srcBuf);
                if(ret.length + Integer.BYTES > out.remaining())
                {
                    out = reAllocByteBuffer(out, (out.position() + ret.length + Integer.BYTES) * 2);
                }
                out.putInt(ret.length);
                out.put(ret);
                byteCount += ret.length + Integer.BYTES * 8;
                readBytes -= blockSize;
            }
            dataIndex += numElementAndPartial.numElement;
            dataBufer.clear();
        }
        return out;
    }
}
