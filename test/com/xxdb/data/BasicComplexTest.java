package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.Vector;

import static org.junit.Assert.*;

public class BasicComplexTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test
    public void test_complex_function(){
        BasicComplex bc = new BasicComplex(25.14,42.33);
        assertEquals(25.14,bc.getDouble2().x,0);
        assertEquals(42.33,bc.getDouble2().y,0);
        assertFalse(bc.isNull());
        assertEquals(Entity.DATA_CATEGORY.BINARY,bc.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_COMPLEX,bc.getDataType());
        System.out.println(bc.hashCode());
        System.out.println(bc.hashBucket(1));
        System.out.println(bc.getJsonString());
        System.out.println(bc.getString());
        assertFalse(bc.equals(null));
        bc.setNull();
        assertTrue(bc.isNull());
        assertEquals("",bc.getString());
    }

    @Test
    public void test_BasicComplex() throws IOException {
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
                return new Double2(17.17,21.21);
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
                return 0;
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
        BasicComplex bc = new BasicComplex(in);
        BasicComplex bc2 = new BasicComplex(17.17,21.21);
        assertTrue(bc.equals(bc2));
        ExtendedDataOutput out = new ExtendedDataOutput() {
            @Override
            public void writeString(String str) throws IOException {

            }

            @Override
            public void writeBlob(byte[] v) throws IOException {

            }

            @Override
            public void writeLong2(Long2 v) throws IOException {

            }

            @Override
            public void writeDouble2(Double2 v) throws IOException {
                System.out.println(v.x+" "+v.y);
            }

            @Override
            public void flush() throws IOException {

            }

            @Override
            public void writeShortArray(short[] A) throws IOException {

            }

            @Override
            public void writeShortArray(short[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeIntArray(int[] A) throws IOException {

            }

            @Override
            public void writeIntArray(int[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeLongArray(long[] A) throws IOException {

            }

            @Override
            public void writeLongArray(long[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeDoubleArray(double[] A) throws IOException {

            }

            @Override
            public void writeDoubleArray(double[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeFloatArray(float[] A) throws IOException {

            }

            @Override
            public void writeFloatArray(float[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeStringArray(String[] A) throws IOException {

            }

            @Override
            public void writeStringArray(String[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeLong2Array(Long2[] A) throws IOException {

            }

            @Override
            public void writeLong2Array(Long2[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeDouble2Array(Double2[] A) throws IOException {

            }

            @Override
            public void writeDouble2Array(Double2[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void writeBigIntArray(byte[] A, int startIdx, int len) throws IOException {

            }

            @Override
            public void write(int b) throws IOException {

            }

            @Override
            public void write(byte[] b) throws IOException {

            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public void writeBoolean(boolean v) throws IOException {

            }

            @Override
            public void writeByte(int v) throws IOException {

            }

            @Override
            public void writeShort(int v) throws IOException {

            }

            @Override
            public void writeChar(int v) throws IOException {

            }

            @Override
            public void writeInt(int v) throws IOException {

            }

            @Override
            public void writeLong(long v) throws IOException {

            }

            @Override
            public void writeFloat(float v) throws IOException {

            }

            @Override
            public void writeDouble(double v) throws IOException {

            }

            @Override
            public void writeBytes(String s) throws IOException {

            }

            @Override
            public void writeChars(String s) throws IOException {

            }

            @Override
            public void writeUTF(String s) throws IOException {

            }
        };
        bc.writeScalarToOutputStream(out);
    }

    @Test(expected = Exception.class)
    public void test_BasicComplex_getNumber() throws Exception {
        BasicComplex bc = new BasicComplex(2.2,4);
        bc.getNumber();
    }

    @Test(expected = Exception.class)
    public void test_BasicComplex_getTemporal() throws Exception {
        BasicComplex bc = new BasicComplex(1,9);
        assertEquals(1,bc.getReal(),0);
        assertEquals(9,bc.getImage(),0);
        bc.getTemporal();
    }

    @Test
    public void test_BasicComplexMatrix_function() {
        BasicComplexMatrix bcm = new BasicComplexMatrix(2, 2);
        bcm.setComplex(0,0,1.1,2.2);
        bcm.setComplex(0,1,3.3,4.4);
        bcm.setComplex(1,0,5.5,6.6);
        bcm.setComplex(1,1,7.7,8.8);
        assertEquals(new BasicComplex(3.3,4.4),bcm.get(0,1));
        assertFalse(bcm.isNull(1,0));
        assertEquals(new Double2(3.3,4.4),bcm.getDouble(0,1));
        assertEquals(Entity.DATA_CATEGORY.BINARY,bcm.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_COMPLEX,bcm.getDataType());
        assertEquals(BasicComplex.class,bcm.getElementClass());
        bcm.setNull(1,1);
        assertTrue(bcm.isNull(1,1));
    }

    @Test(expected= Exception.class)
    public void test_BasicComplexMatrix_nullList() throws Exception {
        BasicComplexMatrix bcm = new BasicComplexMatrix(1,1,null);
    }

    @Test(expected= Exception.class)
    public void test_BasicComplexMatrix_lessRows() throws Exception {
        List<Double2[]> list = new ArrayList<>();
        Double2[] double1 = new Double2[]{new Double2(1,9.2)};
        Double2[] double2 = new Double2[]{new Double2(3.8,7.4)};
        list.add(double1);
        list.add(double2);
        BasicComplexMatrix bcm = new BasicComplexMatrix(2,2,list);
    }

    @Test
    public void test_BasicComplexMatrix_normal() throws Exception {
        List<Double2[]> list = new ArrayList<>();
        Double2[] double1 = new Double2[]{new Double2(1,9.2),new Double2(5.5,6.4)};
        Double2[] double2 = new Double2[]{new Double2(3.8,7.4),new Double2(4.7,8.3)};
        list.add(double1);
        list.add(double2);
        BasicComplexMatrix bcm = new BasicComplexMatrix(2,2,list);
        assertEquals(4.7,bcm.getDouble(1,1).x,0);
        assertEquals(8.3,bcm.getDouble(1,1).y,0);
        Map<String,Entity> map = new HashMap<>();
        map.put("complexMatrix",bcm);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.upload(map);
        assertTrue(conn.run("complexMatrix;").isMatrix());
        assertEquals(Entity.DATA_TYPE.DT_COMPLEX,conn.run("complexMatrix").getDataType());
    }

    @Test
    public void test_BasicComplex_toJSONString() {
        BasicComplex bc = new BasicComplex(25.14,42.33);
        String re = JSONObject.toJSONString(bc);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_COMPLEX\",\"dictionary\":false,\"double2\":{\"null\":false,\"x\":25.14,\"y\":42.33},\"image\":42.33,\"jsonString\":\"\\\"25.14+42.33i\\\"\",\"matrix\":false,\"null\":false,\"pair\":false,\"real\":25.14,\"scalar\":true,\"string\":\"25.14+42.33i\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicComplexMatrix_toJSONString() {
        BasicComplexMatrix bcm = new BasicComplexMatrix(2, 2);
        bcm.setComplex(0,0,1.1,2.2);
        bcm.setComplex(0,1,3.3,4.4);
        bcm.setComplex(1,0,5.5,6.6);
        bcm.setComplex(1,1,7.7,8.8);
        assertEquals(new BasicComplex(3.3,4.4),bcm.get(0,1));
        String re = JSONObject.toJSONString(bcm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_COMPLEX\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicComplex\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0       #1      \\n1.1+2.2i 3.3+4.4i\\n5.5+6.6i 7.7+8.8i\\n\",\"table\":false,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicComplexMartix_getScale() throws Exception {
        BasicComplexMatrix bcm = new BasicComplexMatrix(2, 2);
        bcm.setComplex(0,0,1.1,2.2);
        bcm.setComplex(0,1,3.3,4.4);
        bcm.setComplex(1,0,5.5,6.6);
        bcm.setComplex(1,1,7.7,8.8);
        bcm.getScale();
    }
}
