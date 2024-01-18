package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.ResourceBundle;

import static org.junit.Assert.*;
public class BasicDurationTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test
    public void test_BasicDuration(){
        BasicDuration bd = new BasicDuration(Entity.DURATION.MS,2);
        assertEquals(2,bd.getDuration());
        assertEquals(Entity.DURATION.MS,bd.getUnit());
        assertEquals("2ms",bd.getJsonString());
        assertEquals(-1,bd.compareTo(new BasicDuration(Entity.DURATION.NS,Integer.MIN_VALUE)));
        assertEquals(-1,bd.compareTo(new BasicDuration(Entity.DURATION.MS,Integer.MIN_VALUE)));
        assertFalse(bd.equals(null));
        assertEquals("",new BasicDuration(Entity.DURATION.US,Integer.MIN_VALUE).getString());
    }
    @Test
    public void test_BasicDuration_unit_string() throws IOException {
        BasicDuration bd1 = new BasicDuration("ns",3);
        assertEquals("3ns",bd1.getJsonString());
        BasicDuration bd2 = new BasicDuration("us",3);
        assertEquals("3us",bd2.getJsonString());
        BasicDuration bd3 = new BasicDuration("ms",3);
        assertEquals("3ms",bd3.getJsonString());
        BasicDuration bd4 = new BasicDuration("s",3);
        assertEquals("3s",bd4.getJsonString());
        BasicDuration bd5 = new BasicDuration("m",3);
        assertEquals("3m",bd5.getJsonString());
        BasicDuration bd6 = new BasicDuration("H",3);
        assertEquals("3H",bd6.getJsonString());
        BasicDuration bd7 = new BasicDuration("d",3);
        assertEquals("3d",bd7.getJsonString());
        BasicDuration bd8 = new BasicDuration("w",3);
        assertEquals("3w",bd8.getJsonString());
        BasicDuration bd9 = new BasicDuration("M",3);
        assertEquals("3M",bd9.getJsonString());
        BasicDuration bd10 = new BasicDuration("y",3);
        assertEquals("3y",bd10.getJsonString());
        BasicDuration bd11 = new BasicDuration("B",3);
        assertEquals("3B",bd11.getJsonString());
        String re = null;
        try{
            BasicDuration bd12 = new BasicDuration("b",3);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("error unit: b",re);
    }
    @Test
    public void test_BasicDuration_write() throws IOException {
        BasicDuration bd = new BasicDuration(Entity.DURATION.MONTH,3);
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

            }

            @Override
            public void writeShort(int v) throws IOException {

            }

            @Override
            public void writeChar(int v) throws IOException {

            }

            @Override
            public void writeInt(int v) throws IOException {
                System.out.println(v);
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
        bd.writeScalarToOutputStream(out);
    }

    @Test
    public void test_BasicDurationVector() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(3);
        bdv.setNull(0);
        bdv.setNull(1);
        bdv.setNull(2);
        assertEquals(-2147483648,((Scalar)bdv.get(1)).getNumber());
        assertEquals(0,bdv.hashBucket(0,1));
        assertEquals(4,bdv.getUnitLength());
    }

    @Test
    public void test_BasicDurationVector_wvtb() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration(Entity.DURATION.WEEK,5));
        bdv.set(1,new BasicDuration(Entity.DURATION.HOUR,2));
        ByteBuffer bb = bdv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals("[0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 5]",Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicDurationVector_Append() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration(Entity.DURATION.WEEK,5));
        bdv.set(1,new BasicDuration(Entity.DURATION.HOUR,2));
        int size = bdv.rows();
        System.out.println(size);
        bdv.Append(new BasicDuration(Entity.DURATION.NS,7));
        bdv.Append(new BasicDuration(Entity.DURATION.NS,8));
        assertEquals(size+2,bdv.rows());
        BasicDurationVector bdv2 = new BasicDurationVector(2);
        bdv2.set(0,new BasicDuration(Entity.DURATION.MONTH,6));
        bdv2.set(1,new BasicDuration(Entity.DURATION.SECOND,15));
        bdv.Append(bdv2);
        System.out.println(bdv.getString());
        assertEquals(size+4,bdv.rows());
    }
//    @Test
//    public void test_BasicDurationVector_Append_1() throws Exception {
//        BasicDurationVector bdv = new BasicDurationVector(0);
//        BasicDuration bdv2 = new BasicDuration(Entity.DURATION.MONTH,6);
//        bdv2.setNull();
//        bdv.Append(bdv2);
//        System.out.println(bdv.getString());
//    }

    @Test
    public void test_BasicDuration_serialize() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(6);
        bdv.set(0,new BasicDuration(Entity.DURATION.WEEK,5));
        bdv.set(1,new BasicDuration(Entity.DURATION.HOUR,2));
        bdv.set(2,new BasicDuration(Entity.DURATION.SECOND,7));
        bdv.set(3,new BasicDuration(Entity.DURATION.MINUTE,4));
        bdv.set(4,new BasicDuration(Entity.DURATION.MONTH,11));
        bdv.set(5,new BasicDuration(Entity.DURATION.US,3));
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

            }

            @Override
            public void writeShort(int v) throws IOException {

            }

            @Override
            public void writeChar(int v) throws IOException {

            }

            @Override
            public void writeInt(int v) throws IOException {
                System.out.println(v);
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
        bdv.serialize(1,4,out);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(6,2);
        assertEquals(16,bdv.serialize(1,0,4,numElementAndPartial,ByteBuffer.allocate(56)));
    }
    @Test
    public void test_BasicDuration_toJSONString(){
        BasicDuration bd = new BasicDuration(Entity.DURATION.MS,2);
        String re = JSONObject.toJSONString(bd);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"SYSTEM\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DURATION\",\"dictionary\":false,\"duration\":2,\"exchangeInt\":2,\"exchangeName\":\"ms\",\"jsonString\":\"2ms\",\"matrix\":false,\"null\":false,\"number\":2,\"pair\":false,\"scalar\":true,\"string\":\"2ms\",\"table\":false,\"unit\":\"MS\",\"vector\":false}", re);
    }
    @Test
    public void test_BasicDuration_1() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicDuration bd = (BasicDuration)conn.run("duration(3XNYS)");
        assertEquals(3,bd.getDuration());
        assertEquals("3XNYS",bd.getString());
        BasicDuration bd1 = (BasicDuration)conn.run("duration(-3XNYS)");
        assertEquals(-3,bd1.getDuration());
        assertEquals("-3XNYS",bd1.getString());
        BasicDuration bd2 = (BasicDuration)conn.run("duration(0XNYS)");
        assertEquals(0,bd2.getDuration());
        assertEquals("0XNYS",bd2.getString());
        BasicDuration bd3 = (BasicDuration)conn.run("duration(-9999999XNYS)");
        assertEquals(-9999999,bd3.getDuration());
        assertEquals("-9999999XNYS",bd3.getString());
        conn.close();
    }
    @Test
    public void test_BasicDuration_2() throws IOException {
        BasicDuration bd = new BasicDuration("XNYS",3);
        assertEquals(3,bd.getDuration());
        assertEquals("3XNYS",bd.getString());
        BasicDuration bd1 = new BasicDuration("XNYS",-300);
        assertEquals(-300,bd1.getDuration());
        assertEquals("-300XNYS",bd1.getString());
        BasicDuration bd2 = new BasicDuration("XNYS",0);
        assertEquals(0,bd2.getDuration());
        assertEquals("0XNYS",bd2.getString());
        BasicDuration bd3 = new BasicDuration("XNYS",-9999999);
        assertEquals(-9999999,bd3.getDuration());
        assertEquals("-9999999XNYS",bd3.getString());
        String ex = null;
        try{
            BasicDuration bd4 = new BasicDuration("1111",-9999999);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("The value of unit must duration enum type or contain four consecutive uppercase letters.",ex);
        try{
            BasicDuration bd4 = new BasicDuration("aaaa",-9999999);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("The value of unit must duration enum type or contain four consecutive uppercase letters.",ex);
        try{
            BasicDuration bd4 = new BasicDuration("aaaaaa",-9999999);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("The value of unit must duration enum type or contain four consecutive uppercase letters.",ex);

    }
    @Test
    public void test_BasicDuration_not_support() throws IOException {
        String ex = null;
        try{
            BasicDuration bd = new BasicDuration(Entity.DURATION.TDAY,3);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("the exchange unit should be given as String when use exchange calendar.",ex);
    }
    @Test
    public void test_BasicDurationVector_1() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicAnyVector bdv = (BasicAnyVector)conn.run("[duration(3XNYS),duration(3XNYS)]");
        System.out.println(bdv.getString());
        assertEquals("(3XNYS,3XNYS)",bdv.getString());
        System.out.println(((BasicDuration)bdv.get(0)).getDuration());
        assertEquals(3,((BasicDuration)bdv.get(0)).getDuration());
        conn.close();
    }
    @Test
    public void test_BasicDurationVector_2() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration("XNYS",3));
        bdv.set(1,new BasicDuration("XNYS",0));
        bdv.Append(new BasicDuration("XNYS",-999999999));
        assertEquals("[3XNYS,0XNYS,-999999999XNYS]",bdv.getString());
        assertEquals("3XNYS",bdv.get(0).getString());
    }
    @Test
    public void test_BasicDurationVector_3() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicDurationVector Vector1 = (BasicDurationVector) conn.run("-5AAAA:0AAAA");
        BasicDurationVector Vector2 = (BasicDurationVector) conn.run("pair(-5AAAA,0AAAA)");
        assertEquals("[-5AAAA,0AAAA]",Vector1.getString());
        assertEquals("[-5AAAA,0AAAA]",Vector2.getString());
        conn.close();
    }
}
