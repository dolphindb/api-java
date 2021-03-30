package com.xxdb.compression;

import com.xxdb.io.ExtendedDataOutput;

import java.io.DataInput;
import java.io.IOException;

public interface Encoder {

    ExtendedDataOutput compress(DataInput in, int length, int unitLength, int elementCount) throws IOException;
}
