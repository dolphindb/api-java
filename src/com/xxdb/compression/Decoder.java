package com.xxdb.compression;

import com.xxdb.io.AbstractExtendedDataInputStream;

public interface Decoder {

    default int decompress(AbstractExtendedDataInputStream in, int inLength, byte[] out, int unitLength) {
        return decompress(in, inLength, out, 0, out.length, unitLength);
    }

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
     * @param inLength   input length, continue from current position of input stream
     * @param out        byte array of output
     * @param outOff     offset of output
     * @param outLength  the exact number of bytes you want (e.g. size of out - out offset )
     * @param unitLength how many bytes does each unit contain
     * @return number of bytes decompressed
     */
    int decompress(AbstractExtendedDataInputStream in, int inLength, byte[] out, int outOff, int outLength, int unitLength);

    AbstractExtendedDataInputStream getInputStream();

}
