package com.xxdb.compression;

import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class AbstractEncoder implements Encoder {

//    protected ByteBuffer createColumnVector(int maxCompressedLength, boolean isLittleEndian) throws IOException {
//        ByteBuffer out = isLittleEndian ?
//                ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.LITTLE_ENDIAN) :
//                ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.BIG_ENDIAN);
//        out.putInt(0); //space for element count
//        out.putInt(1);
//    }

//    protected ExtendedDataOutput createColumnVector(int length, ByteBuffer buffer, boolean isLittleEndian) throws IOException {
//        ExtendedDataOutput out = isLittleEndian ?
//                new LittleEndianDataOutputStream(new FileOutputStream("/home/ydli/Documents/Compression/clientCompressed")) :
//                new BigEndianDataOutputStream(new FileOutputStream("/home/ydli/Documents/Compression/clientCompressed"));
//        out.writeInt(length);
//        out.writeInt(1);
//        out.write(buffer.array(), 0, length);
//        return out;
//    }

}
