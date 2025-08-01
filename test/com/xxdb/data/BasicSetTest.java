package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ResourceBundle;

public class BasicSetTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test(expected = IllegalArgumentException.class)
    public void test_BasicSet_KeyType_error(){
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_SYMBOL);
    }

    @Test
    public void test_BasicSet_KeyType(){
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_INT,4);
        assertTrue(bs.add(new BasicInt(2)));
        assertTrue(bs.add(new BasicInt(3)));
        assertTrue(bs.add(new BasicInt(4)));
        assertTrue(bs.add(new BasicInt(5)));
        assertEquals(4,bs.rows());
        assertEquals(1,bs.columns());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bs.getDataCategory());
        assertTrue(bs.contains(new BasicInt(4)));
        assertFalse(bs.contains(new BasicInt(1)));
        assertEquals("[2,3,4,5]",bs.getString());
        assertTrue(bs.add(new BasicInt(7)));
        assertFalse(bs.add(new BasicDateTime(LocalDateTime.now())));
        assertEquals("[]",new BasicSet(Entity.DATA_TYPE.DT_TIMESTAMP,2).keys().getString());
    }
    @Test
    public void test_BasicSet_KeyType1(){
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_MINUTE,4);
        assertTrue(bs.add(new BasicMinute(-2147483648)));
        assertTrue(bs.add(new BasicMinute(-2)));
        assertTrue(bs.add(new BasicMinute(0)));
        assertTrue(bs.add(new BasicMinute(21)));
        System.out.println(bs.getString());
    }

    @Test(expected = IOException.class)
    public void test_BasicSet_ExtendedDataInput_notVector() throws IOException {
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
                return 641;
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
                return 0;
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
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_INT,in);
    }

    @Test(expected = IOException.class)
    public void test_BasicSet_ExtendedDataInput_invalidKeyType() throws IOException {
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
                return 425;
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
                return 0;
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
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_TIMESTAMP,in);
    }
    @Test
    public void test_BasicSet_toJSONString(){
        BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_INT,4);
        assertTrue(bs.add(new BasicInt(2)));
        assertTrue(bs.add(new BasicInt(3)));
        assertTrue(bs.add(new BasicInt(4)));
        assertTrue(bs.add(new BasicInt(5)));
        String re = JSONObject.toJSONString(bs);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SET\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[2,3,4,5]\",\"table\":false,\"vector\":false}", re);
    }
}
