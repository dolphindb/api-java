package com.xxdb.compression;

import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LZ4Decoder extends AbstractDecoder {
    private LZ4SafeDecompressor decompressor = null;

    @Override
    public ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount, boolean isLittleEndian) throws IOException{
    	if(decompressor == null){
	        LZ4Factory factory = LZ4Factory.fastestInstance();
	        decompressor = factory.safeDecompressor();
    	}
        int offset = 8;
      	ByteBuffer dest = createColumnVector(elementCount, unitLength, isLittleEndian);
    	byte[] out = dest.array();
        int outLength = out.length - offset;
        int count = 0;
        
        while (length > 0 && count < outLength) {
            int blockSize = in.readInt();
            if(blockSize < 0){
            	blockSize = blockSize & 2147483647;
            }
            length -= Integer.BYTES;
            blockSize = Math.min(blockSize, length);
            if (blockSize == 0) break;
            
            byte[] src = new byte[blockSize];
            in.readFully(src);
            count += decompressor.decompress(src, 0, blockSize, out, count + offset);
            length -= blockSize;
        }
        return isLittleEndian ?
                new LittleEndianDataInputStream(new ByteArrayInputStream(out, 0, offset + count)) :
                new BigEndianDataInputStream(new ByteArrayInputStream(out, 0, offset + count));
    }
}
