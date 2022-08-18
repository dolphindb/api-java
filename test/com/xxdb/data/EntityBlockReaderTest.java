package com.xxdb.data;

import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;

public class EntityBlockReaderTest {
    @Test
    public void test_EntityBlockReader_Basic() throws IOException {
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return null;
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
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
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 385;
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
                return 2;
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
        EntityBlockReader ebr = new EntityBlockReader(in);
        assertEquals(Entity.DATA_FORM.DF_TABLE,ebr.getDataForm());
        assertEquals(Entity.DATA_CATEGORY.MIXED,ebr.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_ANY,ebr.getDataType());
        assertEquals(0,ebr.rows());
        assertEquals(0,ebr.columns());
        assertNull(ebr.getString());
        assertFalse(ebr.isScalar());
        assertFalse(ebr.isVector());
        assertFalse(ebr.isPair());
        assertTrue(ebr.isTable());
        assertFalse(ebr.isMatrix());
        assertFalse(ebr.isDictionary());
        assertFalse(ebr.isChart());
        assertFalse(ebr.isChunk());
        ebr.write(null);
    }
}
