package com.xxdb.compression;

import java.io.DataInput;
import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;

public interface Decoder {
    /**
     * This method takes in an input stream with many blocks and decompresses them all into a byte array
     * <p>
     * Form of input:
     * 1. 4 bytes - block size
     * 2. contents
     * 3. 4 bytes - block size
     * 4. contents
     * ...
     *
     * @param in         input stream
     * @param length     input length, continue from current position of input stream
     * @param unitLength The length of an element in bytes. It is zero if the element is a string object.
     * @return
     */
    ExtendedDataInput decompress(DataInput in, int length, int unitLength, int elementCount, boolean isLittleEndian, int extra) throws IOException;
}
