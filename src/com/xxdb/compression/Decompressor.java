package com.xxdb.compression;

import com.xxdb.io.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Decompressor {
    AbstractExtendedDataInputStream in;
    int dataSize;
    int srcSize;
    int compression;
    int unitLength;

    public Decompressor(ExtendedDataInput in, int srcSize, int dataSize, int unitLength, int compression) {
        if (!(in instanceof AbstractExtendedDataInputStream)) {
            throw new RuntimeException("Cannot decompress the input: not a class of AbstractExtendedInputStream");
        }
        this.in = (AbstractExtendedDataInputStream) in;
        this.srcSize = srcSize;
        this.dataSize = dataSize;
        this.compression = compression;
        this.unitLength = unitLength;
    }

    public ExtendedDataInput decompress() {
        AbstractExtendedDataInputStream out = null;
        if (compression == 2) {
            ByteBuffer dest = ByteBuffer.allocate(unitLength * dataSize + 20);
            dest.putInt(dataSize);
            dest.putInt(1);
            DeltaOfDeltaDecoder decoder = new DeltaOfDeltaDecoder(this.in, dest, srcSize, dataSize, unitLength);
            out = new BigEndianDataInputStream(new ByteArrayInputStream(decoder.decompress()));
        } else if (compression == 1) {
            ByteBuffer dest = ByteBuffer.allocate(unitLength * dataSize + 20);
            ByteBuffer header = ByteBuffer.allocate(8);
            header.putInt(dataSize);
            header.putInt(1);
            header.flip();
            LittleEndianDataInputStream bigIn = new LittleEndianDataInputStream(new ByteArrayInputStream(header.array()));
            try {
                dest.putInt(bigIn.readInt());
                dest.putInt(bigIn.readInt());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Fail to generater column header after decompressing");
            }
            LZ4Decoder decoder = new LZ4Decoder(in, srcSize, dataSize, unitLength);
            dest.put(decoder.decompress());
            out = new LittleEndianDataInputStream(new ByteArrayInputStream(dest.array()));
        }
        return out;
    }
}
