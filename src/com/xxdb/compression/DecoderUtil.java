package com.xxdb.compression;

import com.xxdb.io.LittleEndianDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DecoderUtil {


    public static void createColumnHeader(ByteBuffer dest, int rows, int cols, boolean isLittleEndian) {
        if (isLittleEndian) {
            ByteBuffer header = ByteBuffer.allocate(8);
            header.putInt(rows);
            header.putInt(cols);
            header.flip();
            LittleEndianDataInputStream bigIn = new LittleEndianDataInputStream(new ByteArrayInputStream(header.array()));
            try {
                dest.putInt(bigIn.readInt());
                dest.putInt(bigIn.readInt());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Fail to generater column header.");
            }
        } else {
            dest.putInt(rows);
            dest.putInt(cols);
        }

    }

}
