package com.xxdb.compression;

import com.xxdb.data.Entity;
import com.xxdb.io.AbstractExtendedDataInputStream;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;

//TODO: get data size
public class Decompressor {
    AbstractExtendedDataInputStream in;
    int dataSize;

    public Decompressor(ExtendedDataInput in) {
        if (!(in instanceof AbstractExtendedDataInputStream)) {
            throw new RuntimeException("Cannot decompress the input: not a class of AbstractExtendedInputStream");
        }
        this.in = (AbstractExtendedDataInputStream) in;
        try {
            this.in.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.dataSize = dataSize;
    }
    public ExtendedDataInput decompress() {
        AbstractExtendedDataInputStream out = null;
        try {
            int decoder_type = this.in.readShort(); //TODO: which bit to use?
            if (decoder_type == 1) {
                DeltaOfDeltaDecoder decoder = new DeltaOfDeltaDecoder(this.in, 1<<16, dataSize);
            } else if (decoder_type == 2) {
                byte[] src = new byte[Long.BYTES * 1000];
                this.in.readFully(src);
                LZ4Decoder decoder = new LZ4Decoder(src, 1<<16, dataSize);
                out = new BigEndianDataInputStream(new ByteArrayInputStream(decoder.decompress()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out;
    }
}
