package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

public class BasicPointVectorTest {

    @Test
    public void test_BasicPointVector_list(){
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(null);
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv = new BasicPointVector(list);
        assertEquals("[(3.8, 7.4),(5.6, 6.5),(1.0, 9.2)]",bpv.getSubVector(new int[]{1,3,0}).getString());
        assertTrue(bpv.isNull(2));
        assertEquals(new BasicPoint(1.0,9.2),bpv.get(0));
        assertEquals(Entity.DATA_CATEGORY.BINARY,bpv.getDataCategory());
        assertEquals(BasicPoint.class,bpv.getElementClass());
    }

    @Test
    public void test_BasicPointVector_array(){
        Double2[] arr = new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),null,new Double2(5.6,6.5)};
        BasicPointVector bpv = new BasicPointVector(arr);
        assertEquals(3.8,bpv.getSubArray(new int[]{1,3,0})[0].x,0);
        assertEquals(9.2,bpv.getSubArray(new int[]{1,3,0})[2].y,0);
        assertEquals(-1,bpv.hashBucket(2,1));
        bpv.setPoint(2,8.3,2.9);
        assertEquals(8.3,bpv.getSubArray(new int[]{2,1,3,0})[0].x,0);
        bpv.setNull(1);
        assertTrue(bpv.isNull(1));
    }

    @Test
    public void test_BasicPointVector_capacity_lt_size() throws Exception {
        BasicPointVector bbv = new BasicPointVector(5,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Double2(1.0,9.2));
        bbv.set(3, new Double2(-1.0,-9.2));
        bbv.set(4, new Double2(Double.MIN_VALUE,Double.MIN_VALUE));
        Assert.assertEquals("[(,),(,),(1.0, 9.2),(-1.0, -9.2),(4.9E-324, 4.9E-324)]", bbv.getString());
    }

    @Test
    public void test_BasicPointVector_size_capacity_set() throws Exception {
        BasicPointVector bbv = new BasicPointVector(5,6);
        Assert.assertEquals("[(0.0, 0.0),(0.0, 0.0),(0.0, 0.0),(0.0, 0.0),(0.0, 0.0)]", bbv.getString());
        bbv.set(0, (Object)null);
        //bbv.set(1, null);
        bbv.set(2, new Double2(1.0,9.2));
        bbv.set(3, new Double2(-1.0,-9.2));
        bbv.set(4, new Double2(Double.MIN_VALUE,Double.MIN_VALUE));
        Assert.assertEquals("[(,),(,),(1.0, 9.2),(-1.0, -9.2),(4.9E-324, 4.9E-324)]", bbv.getString());
    }

    @Test
    public void test_BasicPointVector_size_capacity_add() throws Exception {
        BasicPointVector bbv = new BasicPointVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(new Double2(1.0,9.2));
        bbv.add(new Double2(-1.0,-9.2));
        bbv.add(new Double2(Double.MIN_VALUE,Double.MIN_VALUE));
        Assert.assertEquals("[(,),(,),(1.0, 9.2),(-1.0, -9.2),(4.9E-324, 4.9E-324)]", bbv.getString());
    }

    @Test
    public void test_BasicPointVector_set_type_not_match() throws Exception {
        BasicPointVector bbv = new BasicPointVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Double2 or null is supported.", re);
    }

    @Test
    public void test_BasicPointVector_add_type_not_match() throws Exception {
        BasicPointVector bbv = new BasicPointVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Double2 or null is supported.", re);
    }

    @Test
    public void test_BasicPointVector_input() throws IOException {
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
        BasicPointVector bpv = new BasicPointVector(Entity.DATA_FORM.DF_VECTOR,in);
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
                for(Double2 dou :A){
                    System.out.println(dou.x);
                    System.out.println(dou.y);
                }
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
        bpv.writeVectorToOutputStream(out);
    }

    @Test
    public void test_BasicPointVector_combine(){
        Double2[] arr = new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),new Double2(5.6,6.5)};
        BasicPointVector bpv = new BasicPointVector(arr);
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        assertEquals(bpv.combine(bpv2).getString(), bpv2.combine(bpv).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicPointVector_asof(){
        Double2[] arr = new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),new Double2(5.6,6.5)};
        BasicPointVector bpv = new BasicPointVector(arr);
        bpv.asof(new BasicPoint(1.7,8.9));
    }

    @Test
    public void test_BasicPointVector_serialize() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        ByteBuffer bb1 = ByteBuffer.allocate(48);
        bb1.order(ByteOrder.BIG_ENDIAN);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(3,1);
        assertEquals(48,bpv2.serialize(0,0,3,numElementAndPartial,bb1));
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(48,bpv2.serialize(0,0,3,numElementAndPartial,bb));
        assertNotEquals(Arrays.toString(bb1.array()),Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicPointVector_wvtb() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer bo = bpv2.writeVectorToBuffer(bb);
        ByteBuffer bb1 = ByteBuffer.allocate(48);
        bb1.order(ByteOrder.BIG_ENDIAN);
        ByteBuffer bo1 = bpv2.writeVectorToBuffer(bb1);
        assertNotEquals(Arrays.toString(bo.array()),Arrays.toString(bo1.array()));
    }

    @Test
    public void test_BasicPointVector_Append() throws Exception {
        BasicPointVector bpv = new BasicPointVector(new Double2[]{});
        int size = bpv.size;
        int capacity = bpv.capacity;
        System.out.println(size);
        System.out.println(capacity);
        bpv.Append(new BasicPoint(3.6,0.72));
        assertEquals(size+1,bpv.size);
        System.out.println(bpv.capacity);
        bpv.Append(new BasicPointVector(new Double2[]{new Double2(2.2,0.62),new Double2(0.76,9.25)}));
        assertEquals(size+3,bpv.size);
        assertEquals(capacity+3,bpv.capacity);
    }

    @Test
    public void test_BasicPointVector_toJSONString() throws Exception {
        BasicPointVector bpv = new BasicPointVector(new Double2[]{});
        String re = JSONObject.toJSONString(bpv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_POINT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicPoint\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[]\",\"table\":false,\"unitLength\":16,\"values\":[],\"vector\":true}", re);
    }
}
