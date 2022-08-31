package com.xxdb.data;

import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;

import static org.junit.Assert.*;
public class BasicDurationTest {
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
}
