package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class BasicByteTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(HOST,PORT,"admin","123456")){
                throw new IOException("Failed to connect to 2xdb server");
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
    private int getServerHash(Scalar s, int bucket) throws IOException{
        List<Entity> args = new ArrayList<>();
        args.add(s);
        args.add(new BasicInt(bucket));
        BasicInt re = (BasicInt)conn.run("hashBucket",args);
        return re.getInt();
    }

    @Test
    public void test_getHash() throws  IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicByteVector v = new BasicByteVector(6);
        v.setByte(0,(byte)127);
        v.setByte(1,(byte)-127);
        v.setByte(2,(byte)12);
        v.setByte(3,(byte)0);
        v.setByte(4,(byte)-128);
        v.setNull(5);
        for(int b : num){
            for(int i=0;i<v.rows();i++){
                int expected = getServerHash(((Scalar)v.get(i)),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, ((Scalar)v.get(i)).hashBucket(b));
            }
        }
    }

    @Test
    public void test_BasicByte() throws Exception {
        BasicByte bb = new BasicByte(Byte.MIN_VALUE);
        assertTrue(bb.isNull());
        assertFalse(bb.equals(6));
    }

    @Test
    public void test_BasicByteMatrix(){
        BasicByteMatrix bbm = new BasicByteMatrix(2,2);
        bbm.setInt(0,0, (byte)24);
        bbm.setInt(0,1, (byte) 33);
        bbm.setInt(1,0, (byte) 48);
        bbm.setInt(1,1, (byte) 72);
        assertEquals((byte)48,bbm.getByte(1,0));
        assertFalse(bbm.isNull(0,1));
        bbm.setNull(1,0);
        assertTrue(bbm.isNull(1,0));
        assertEquals(Entity.DATA_TYPE.DT_BYTE,bbm.getDataType());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bbm.getDataCategory());
        assertEquals(BasicByte.class,bbm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicByteMatrix_list_null() throws Exception {
        BasicByteMatrix bbm = new BasicByteMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicByteMatrix_rows() throws Exception {
        List<byte[]> list = new ArrayList<>();
        list.add(new byte[]{75,33,69});
        list.add(new byte[]{42,88,23});
        BasicByteMatrix bbm = new BasicByteMatrix(4,2,list);
    }

    @Test
    public void test_BasicByteMatrix_wvtos() throws Exception {
        List<byte[]> list = new ArrayList<>();
        list.add(new byte[]{75,33,69});
        list.add(new byte[]{42,88,23});
        BasicByteMatrix bbm = new BasicByteMatrix(3,2,list);
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
                System.out.println((byte)v);
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
        bbm.writeVectorToOutputStream(out);
    }

    @Test
    public void test_byteValue() throws Exception {
        BasicByte bb = new BasicByte(Byte.MIN_VALUE);
        assertEquals(null,bb.byteValue());
        BasicByte bb1 = new BasicByte((byte) 117);
        assertEquals(true,bb1.byteValue().equals((byte) 117));
    }
    @Test
    public void test_BasicByte_toJsonString() throws Exception {
        BasicByte bb1 = new BasicByte((byte) 117);
        String re = JSONObject.toJSONString(bb1);
        System.out.println(re);
        assertEquals("{\"byte\":117,\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_BYTE\",\"dictionary\":false,\"jsonString\":\"'u'\",\"matrix\":false,\"null\":false,\"number\":117,\"pair\":false,\"scalar\":true,\"string\":\"'u'\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicByte_toJsonString_null() throws Exception {
        BasicByte bb = new BasicByte(Byte.MIN_VALUE);
        String re = JSONObject.toJSONString(bb);
        System.out.println(re);
        assertEquals("{\"byte\":-128,\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_BYTE\",\"dictionary\":false,\"jsonString\":\"null\",\"matrix\":false,\"null\":true,\"number\":-128,\"pair\":false,\"scalar\":true,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicByteMatrix_toJsonString(){
        BasicByteMatrix bbm = new BasicByteMatrix(2,2);
        bbm.setInt(0,0, (byte)24);
        String re = JSONObject.toJSONString(bbm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_BYTE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicByte\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1\\n24 0 \\n0  0 \\n\",\"table\":false,\"vector\":false}", re);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicByteMartix_getScale() throws Exception {
        byte [] a={1,2,3};
        byte [] b={1,2,3};
        BasicByteMatrix bm=new BasicByteMatrix(3,2,Arrays.asList(a,b));
        bm.getScale();
    }
}
