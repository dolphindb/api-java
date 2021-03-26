package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;
import com.xxdb.io.LittleEndianDataInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.ByteArrayInputStream;
import java.io.IOException;


public class LZ4Decoder implements Decoder {

    LZ4SafeDecompressor decompressor;
    byte[] out;

    @Override
    public int decompress(AbstractExtendedDataInputStream in, int inLength, byte[] out, int outOff, int outLength, int unitLength) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        this.decompressor = factory.safeDecompressor();
        this.out = out;
        int count = 0;
        while (inLength > 0 && count < outLength) {
            int blockSize;
            try {
                blockSize = in.readInt();
            } catch (IOException e) {
                e.printStackTrace();
                return count;
            }
            inLength -= Integer.BYTES;
            blockSize = Math.min(blockSize, inLength);
            if (blockSize == 0) return count;
            byte[] src = new byte[blockSize];
            try {
                in.readFully(src);
            } catch (IOException e) {
                e.printStackTrace();
                return count;
            }
            count += decompress(src, this.out, blockSize, count + outOff);
            inLength -= blockSize;
        }
        return count;
    }

    @Override
    public AbstractExtendedDataInputStream getInputStream() {
        return new LittleEndianDataInputStream(new ByteArrayInputStream(out));
    }

    protected int decompress(byte[] src, byte[] dest, int blockSize, int destOff) {
        return decompressor.decompress(src, 0, blockSize, dest, destOff);
    }


}
