package com.xxdb.compression;

import com.xxdb.data.Entity;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.DdbByteArrayInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

public class LZ4Decoder extends AbstractDecoder {
    private LZ4SafeDecompressor decompressor = null;

    @Override
    public ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount, boolean isLittleEndian, int extra, int type, short scale) throws IOException{
    	if(decompressor == null){
	        LZ4Factory factory = LZ4Factory.fastestInstance();
	        decompressor = factory.safeDecompressor();
    	}
        byte[] lengthMsg = createLZ4ColumnVector(elementCount, isLittleEndian, extra, type, scale).array();
        LinkedList<byte[]> buffers = new LinkedList<>();
        buffers.add(lengthMsg);
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
            buffers.add(ret);
            length -= blockSize;
        }
        return isLittleEndian ?
                new LittleEndianDataInputStream(new DdbByteArrayInputStream(buffers)) :
                new BigEndianDataInputStream(new DdbByteArrayInputStream(buffers));
    }
}
