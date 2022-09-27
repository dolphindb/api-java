package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;
public class BasicSymbolTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));


    @Test(expected = IOException.class)
    public void test_BasicSymbolEntity() throws IOException {
        BasicSystemEntity bse = new BasicSystemEntity(new LittleEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }), Entity.DATA_TYPE.DT_ANY);
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,bse.getDataCategory());
        bse.writeScalarToOutputStream(new LittleEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        }));
    }

    @Test
    public void test_BasicSymbolVector_BasicFunctions() throws Exception {
        SymbolBase base = new SymbolBase(2);
        BasicSymbolVector bsv = new BasicSymbolVector(base,4);
        assertEquals(4,bsv.getUnitLength());
        bsv.setString(0,"GaussDB");
        bsv.setString(1,"GoldenDB");
        bsv.set(2,new BasicInt(7));
        bsv.set(3,new BasicPoint(5.9,7.1));
        System.out.println(bsv.getString());
        assertEquals("7",bsv.get(2).getString());
        assertEquals("[GoldenDB,7,GaussDB]",bsv.getSubVector(new int[]{1,2,0}).getString());
        assertEquals(0,bsv.hashBucket(2,1));
        assertEquals(Entity.DATA_CATEGORY.LITERAL,bsv.getDataCategory());
        assertEquals(BasicString.class,bsv.getElementClass());
    }

    @Test
    public void test_BasicSymbolVector_list(){
        List<String> list = new ArrayList<>();
        list.add("KingBase");
        list.add("vastBase");
        list.add(null);
        list.add("OceanBase");
        BasicSymbolVector bsv = new BasicSymbolVector(list);
        assertEquals("vastBase",bsv.get(1).getString());
        assertFalse(bsv.isNull(1));
        bsv.setNull(2);
        assertTrue(bsv.isNull(2));
    }

    @Test(expected = Exception.class)
    public void test_basicSymbolVector_values(){
        BasicSymbolVector bsv = new BasicSymbolVector(new SymbolBase(5),new int[]{2,9,16,75,34},true);
        System.out.println(bsv.getString());
    }

    @Test
    public void test_BasicSymbolVector_combine() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("KingBase");
        list.add("vastBase");
        list.add(null);
        list.add("OceanBase");
        BasicSymbolVector bsv = new BasicSymbolVector(list);
        SymbolBase base = new SymbolBase(2);
        BasicSymbolVector bsv2 = new BasicSymbolVector(base,4);
        bsv2.setString(0,"GaussDB");
        bsv2.setString(1,"GoldenDB");
        bsv2.set(2,new BasicInt(7));
        bsv2.set(3,new BasicPoint(5.9,7.1));
        BasicSymbolVector bsv3 = new BasicSymbolVector(base,3);
        bsv3.setString(0,"IBM");
        bsv3.setString(1,"AMAZON");
        bsv3.setString(2,"GOOGLE");
        assertEquals("[KingBase,vastBase,,OceanBase,GaussDB,GoldenDB,7,(5.9, 7.1)]",bsv.combine(bsv2).getString());
        assertEquals("[GaussDB,GoldenDB,7,(5.9, 7.1),IBM,AMAZON,GOOGLE]",bsv2.combine(bsv3).getString());
    }

    @Test
    public void test_BasicSymbolVector_write() throws Exception {
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
                System.out.println(Arrays.toString(A));
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
                System.out.println(v);
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
        SymbolBase base = new SymbolBase(2);
        BasicSymbolVector bsv2 = new BasicSymbolVector(base,4);
        bsv2.setString(0,"GaussDB");
        bsv2.setString(1,"GoldenDB");
        bsv2.set(2,new BasicInt(7));
        bsv2.set(3,new BasicPoint(5.9,7.1));
        bsv2.write(out,new SymbolBaseCollection());
    }

    @Test
    public void test_BasicSymbolVector_asof() throws Exception {
        SymbolBase base = new SymbolBase(2);
        BasicSymbolVector bsv2 = new BasicSymbolVector(base,4);
        bsv2.setString(0,"GaussDB");
        bsv2.setString(1,"MangoDB");
        bsv2.setString(2,"OceanBase");
        bsv2.setString(3,"Xcode");
        assertEquals(0,bsv2.asof(new BasicString("KingBase")));
    }

    @Test
    public void test_BasicSymbolVector_serialize() throws Exception {
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
        BasicSymbolVector bsv = new BasicSymbolVector(3);
        bsv.set(1,new BasicInt(2));
        bsv.set(2,new BasicInt(5));
        bsv.set(0,new BasicInt(1));
        bsv.serialize(0,3,out);
        ByteBuffer bb = ByteBuffer.allocate(24);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(3,1);
        assertEquals(12,bsv.serialize(0,0,3,numElementAndPartial,bb));
        System.out.println(Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicSymbolVector_Append(){
        List<String> list = new ArrayList<>();
        list.add("GaussDB");
        list.add("GoldenDB");
        BasicSymbolVector bsv = new BasicSymbolVector(list);
        int size = bsv.rows();
        bsv.Append(new BasicInt(0));
        assertEquals(size+1,bsv.rows());
        System.out.println(bsv.getString());
        try {
            bsv.Append(new BasicShortVector(new short[]{5, 7, 1}));
        }catch(RuntimeException re){
            assertTrue(re.getMessage().contains("SymbolVector does not support append a vector"));
        }
        assertEquals(size+1,bsv.rows());
    }
}
