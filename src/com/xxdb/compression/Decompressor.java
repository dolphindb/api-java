package com.xxdb.compression;

import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataInput;

import java.io.DataOutput;

public interface Decompressor {
    public ExtendedDataInput decompress(Entity.DATA_FORM df, ExtendedDataInput in);
}
