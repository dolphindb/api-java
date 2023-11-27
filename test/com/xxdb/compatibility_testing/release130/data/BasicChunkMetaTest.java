package com.xxdb.compatibility_testing.release130.data;

import com.xxdb.io.*;
import com.xxdb.data.*;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.OutputStream;

public class BasicChunkMetaTest {
    @Test
    public void test_BasicChunkMeta_Basic() throws IOException {
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return "/home/usr/local/bin/DolphinDB";
            }

            @Override
            public Long2 readLong2() throws IOException {
                return new Long2(9820L,7420L);
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 15;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 1;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 3;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        BasicChunkMeta bcm = new BasicChunkMeta(in);
        assertEquals(1,bcm.rows());
        assertEquals(0,bcm.columns());
        assertEquals("/home/usr/local/bin/DolphinDB",bcm.getPath());
        assertEquals(new BasicUuid(9820L,7420L),bcm.getId());
        assertEquals(Entity.DATA_FORM.DF_CHUNK,bcm.getDataForm());
        assertEquals(bcm.size(),bcm.getVersion());
        assertFalse(bcm.isFileBlock());
        assertFalse(bcm.isTablet());
        assertFalse(bcm.isSplittable());
        assertEquals(15,bcm.getCopyCount());
        assertNull(bcm.getDataCategory());
        assertNull(bcm.getDataType());
        assertEquals("FileBlock[/home/usr/local/bin/DolphinDB, " +
                "00000000-0000-265c-0000-000000001cfc, " +
                "{/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB, /home/usr/local/bin/DolphinDB, " +
                "/home/usr/local/bin/DolphinDB}, v3, 3]",bcm.getString());
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bcm.write(out);
    }
}
