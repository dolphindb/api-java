package com.xxdb.compression;

import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

public class LZ4Decoder extends AbstractDecoder {
    private LZ4SafeDecompressor decompressor = null;

    @Override
    public ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount, boolean isLittleEndian, int extra) throws IOException{
    	if(decompressor == null){
	        LZ4Factory factory = LZ4Factory.fastestInstance();
	        decompressor = factory.safeDecompressor();
    	}
        int offset = 8;
        byte[] out = createColumnVector(elementCount, unitLength, isLittleEndian, 65536, extra).array();
        while (length > 0) {
            int blockSize = in.readInt();
            if(blockSize < 0){
            	blockSize = blockSize & 2147483647;
            }
            length -= Integer.BYTES;
            blockSize = Math.min(blockSize, length);
            if (blockSize == 0) break;
            
            byte[] src = new byte[blockSize];
            in.readFully(src);
            byte[] ret = decompressor.decompress(src, 1<<16);
            if(offset + ret.length > out.length){
                long longLength = ((long) out.length) * 2 - 1;
                if (longLength >= Integer.MAX_VALUE){
                    out = Arrays.copyOf(out, offset+ret.length);
                }else {
                    out = Arrays.copyOf(out, out.length * 2);
                }
            }
            System.arraycopy(ret, 0, out, offset, ret.length);
            offset += ret.length;
            length -= blockSize;
        }
        return isLittleEndian ?
                new LittleEndianDataInputStream(new ByteArrayInputStream(out, 0, offset)) :
                new BigEndianDataInputStream(new ByteArrayInputStream(out, 0, offset));
    }
}
