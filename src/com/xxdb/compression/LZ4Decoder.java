package com.xxdb.compression;
import com.xxdb.io.AbstractExtendedDataInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LZ4Decoder {
    byte[] dest;

    public LZ4Decoder(AbstractExtendedDataInputStream src, int srcSize, int dataSize, int unitLength) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4SafeDecompressor decompressor = factory.safeDecompressor();
        dest = new byte[dataSize * unitLength];
        int blockSize = 0;
        int count = 0;
        int length = 0;
        while (count < srcSize) {
            try {
                blockSize = src.readInt();
                count+=Integer.BYTES;
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (blockSize == 0) break;
            blockSize = Math.min(blockSize, srcSize - count);
            try {
                byte[] srcArray = new byte[blockSize];
                src.readFully(srcArray);
                length+=decompressor.decompress(srcArray, 0, blockSize, dest, length);
            } catch (Exception e) {
                e.printStackTrace();
            }

            count+=blockSize;
        }
    }

    public byte[] decompress() {
        return dest;
    }
}
