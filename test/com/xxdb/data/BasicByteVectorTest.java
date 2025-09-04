package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicByteVectorTest {
    @Test
    public void TestCombineByteVector() throws Exception {
        Byte[] data = {'4', '5', '3', '6'};
        BasicByteVector v = new BasicByteVector(Arrays.asList(data));
        Byte[] data2 = { '2', '5', '1'};
        BasicByteVector vector2 = new BasicByteVector(Arrays.asList(data2));
        BasicByteVector res= (BasicByteVector) v.combine(vector2);
        Byte[] datas = {'4', '5', '3', '6', '2', '5', '1'};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicByteVector_basic() throws IOException {
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 99);
        list.add(1,(byte) 122);
        list.add(2,null);
        list.add(3,(byte) 71);
        list.add(4,(byte) 7);
        BasicByteVector bbv = new BasicByteVector(list);
        assertEquals("[7,'z','G','c']",bbv.getSubVector(new int[]{4,1,3,0}).getString());
        assertTrue(bbv.isNull(2));
        assertEquals("",bbv.get(2).getString());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bbv.getDataCategory());
        assertEquals(BasicByte.class,bbv.getElementClass());
        assertEquals((byte)71,bbv.getByte(3));
        ByteBuffer bb = bbv.writeVectorToBuffer(ByteBuffer.allocate(10));
        assertEquals((byte)122,bb.get(1));
    }

    @Test
    public void test_BasicByteVector_capacity_lt_size() throws Exception {
        BasicByteVector bbv = new BasicByteVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(2, (byte) 99);
        bbv.set(3, (byte) 122);
        bbv.set(4, (byte) 71);
        bbv.set(5, (byte) 7);
        Assert.assertEquals("[,0,'c','z','G',7]", bbv.getString());
    }

    @Test
    public void test_BasicByteVector_size_capacity_set() throws Exception {
        BasicByteVector bbv = new BasicByteVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,new Byte((byte)99));
        bbv.set(3,new Byte((byte)122));
        bbv.set(4, (byte) 71);
        bbv.set(5, (byte) 7);
        Assert.assertEquals("[,,'c','z','G',7]", bbv.getString());
    }

    @Test
    public void test_BasicByteVector_size_capacity_add() throws Exception {
        BasicByteVector bbv = new BasicByteVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(new Byte((byte)99));
        bbv.add(new Byte((byte)122));
        bbv.add((byte) 71);
        bbv.add((byte) 7);
        Assert.assertEquals("[,,'c','z','G',7]", bbv.getString());
    }

    @Test
    public void test_BasicByteVector_set_type_not_match() throws Exception {
        BasicByteVector bbv = new BasicByteVector(0,6);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Byte or null is supported.", re);
    }

    @Test
    public void test_BasicByteVector_add_type_not_match() throws Exception {
        BasicByteVector bbv = new BasicByteVector(0,6);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Byte or null is supported.", re);
    }


    @Test
    public void test_BasicByteVector_asof(){
        List<Byte> list = new ArrayList<>();
        list.add(0,null);
        list.add(1,(byte) 7);
        list.add(2,(byte) 71);
        list.add(3,(byte) 99);
        list.add(4,(byte) 122);
        BasicByteVector bbv = new BasicByteVector(list);
        Scalar value= new BasicByte((byte) 117);
        assertEquals(3,bbv.asof(value));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicByteVector_asof_error(){
        List<Byte> list = new ArrayList<>();
        list.add(0,null);
        list.add(1,(byte) 7);
        list.add(2,(byte) 71);
        list.add(3,(byte) 99);
        list.add(4,(byte) 122);
        BasicByteVector bbv = new BasicByteVector(list);
        assertEquals(3,bbv.asof(new BasicComplex(8.3,4.2)));
    }

    @Test
    public void test_BasicByteVector_serialize() throws IOException {
        List<Byte> list = new ArrayList<>();
        list.add(0,null);
        list.add(1,(byte) 7);
        list.add(2,(byte) 71);
        list.add(3,(byte) 99);
        list.add(4,(byte) 122);
        BasicByteVector bbv = new BasicByteVector(list);
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
        bbv.serialize(0,3,out);
        ByteBuffer bb = ByteBuffer.allocate(20);
        System.out.println(bb.remaining());
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(30,2);
        int m = bbv.serialize(0,0,4,numElementAndPartial,bb);
        assertEquals(4,m);
        System.out.println(bb.get(3));
    }

    @Test
    public void test_BasicByteVector_Append() throws Exception {
        BasicByteVector bbv = new BasicByteVector(new byte[]{'d','o','l','p','h','i','n'});
        int size = bbv.rows();
        bbv.Append(new BasicByte((byte) 'd'));
        bbv.Append(new BasicByte((byte) 'b'));
        assertEquals(size+2,bbv.rows());
        bbv.Append(new BasicByteVector(new byte[]{'t','i','m','i','n','g'}));
        assertEquals(size+8,bbv.rows());
    }

    @Test
    public void test_BasicByteVector_toJsonString(){
        BasicByteVector bbv = new BasicByteVector(new byte[]{'d','o','l','p','h','i','n'});
        String re = JSONObject.toJSONString(bbv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[100,111,108,112,104,105,110],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_BYTE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicByte\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"['d','o','l','p','h','i','n']\",\"table\":false,\"unitLength\":1,\"values\":[100,111,108,112,104,105,110],\"vector\":true}", re);
    }
}
