package com.xxdb.compression;

import com.xxdb.io.ExtendedDataInput;

import java.io.DataInput;
import java.io.IOException;

public class LZ4Encoder extends AbstractEncoder{

    @Override
    public ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount) throws IOException {
        return null;
    }
}
