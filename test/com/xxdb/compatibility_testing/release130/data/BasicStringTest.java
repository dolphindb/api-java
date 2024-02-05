package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

public class BasicStringTest {
    private  DBConnection conn;
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
    public void test_getHash() throws IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicStringVector v = new BasicStringVector(6);
        v.setString(0,"!@#$%^&*()");
        v.setString(1,"我是中文测试内容");
        v.setString(2,"我是!@#$%^中文&*()");
        v.setString(3,"e1281ls.zxl.d.,cxnv./';'sla");
        v.setString(4,"abckdlskdful");
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
    public void TestCombineStringVector() throws Exception {
        String[] data = {"1","-1","3"};
        BasicStringVector v = new BasicStringVector(Arrays.asList(data));
        String[] data2 = {"1","-1","3","9"};
        BasicStringVector vector2 = new BasicStringVector( Arrays.asList(data2));
        BasicStringVector res= (BasicStringVector) v.combine(vector2);
        String[] datas = {"1","-1","3","1","-1","3","9"};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],res.get(i).getString());

        }
        assertEquals(7,res.rows());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicString_blobfalse(){
        byte[] value = new byte[]{12,31,59,86,47};
        BasicString bs = new BasicString(value,false);
        assertFalse(bs.equals(null));
        bs.getBytes();
    }

    @Test
    public void test_BasicString_blobTrue(){
        byte[] value = new byte[]{12,31,59,86,74};
        BasicString bs = new BasicString(value,true);
        System.out.println(bs.hashCode());
        assertFalse(bs.isNull());
        System.out.println(bs.hashCode());
        System.out.println(bs.hashBucket(2));
        assertEquals(8,bs.compareTo(new BasicString("Jmeter")));
    }

    @Test
    public void test_BasicString_equals_False(){
        assertFalse(new BasicString("Junit",true).equals(new BasicString("Jmeter")));
        assertFalse(new BasicString("Junit",true).equals(new BasicString("Jmeter",true)));
        assertFalse(new BasicString("Junit",true).equals(new BasicString("meter",true)));
        assertTrue(new BasicString("Junit",true).equals(new BasicString("Junit",true)));
    }

    @Test
    public void test_BasicString_in_blobfalse() throws IOException {
        ExtendedDataInput edi = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return "Dolphindb";
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
        BasicString bs = new BasicString(edi,false);
        assertEquals("Dolphindb",bs.getString());
    }

    @Test
    public void test_hashBucket(){
        BasicString bs = new BasicString("Ôþ ©÷ò");
        assertEquals(0,bs.hashBucket(3));
    }

    @Test
    public void test_BasicStringMatrix_BasicFunctions(){
        BasicStringMatrix bsm = new BasicStringMatrix(2,2);
        bsm.setString(0,0,"MySQL");
        bsm.setString(0,1,"Oracle");
        bsm.setString(1,0,"DolphinDB");
        bsm.setString(1,1,"SQL Server");
        assertEquals("DolphinDB",bsm.getString(1,0));
        bsm.setNull(1,1);
        assertTrue(bsm.isNull(1,1));
        assertEquals(new BasicString("Oracle"),bsm.get(0,1));
        assertEquals(Entity.DATA_CATEGORY.LITERAL,bsm.getDataCategory());
        assertEquals(BasicString.class,bsm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicStringMatrix_list_Null() throws Exception {
        BasicStringMatrix bsm = new BasicStringMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicStringMatrix_arrat_null() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"MySQL","SQL Server","Oracle"});
        list.add(new String[]{"MangoDB","DolphinDB"});
        list.add(new String[]{"KingBase","VastBase","GaussDB"});
        BasicStringMatrix bsm = new BasicStringMatrix(3,3,list);
    }

    @Test
    public void test_BasicStringVector_BasicFunctions_blobTrue(){
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add("KingBase");
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        System.out.println(bsv.getString());
        assertEquals(1,bsv.getUnitLength());
        assertEquals("GoldenDB",bsv.get(2).getString());
        assertFalse(bsv.isNull(2));
        bsv.setNull(3);
        assertTrue(bsv.isNull(3));
        bsv.setString(2,"OceanBase");
        assertEquals("OceanBase",bsv.getString(2));
        assertEquals(BasicString.class,bsv.getElementClass());
       System.out.println(bsv.getSubVector(new int[]{0,1,2,3,4}).getString());
    }

    @Test
    public void test_BasicStringVector_blobFalse(){
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add(null);
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list);
        assertTrue(bsv.isNull(3));
        BasicStringVector bsv2 = new BasicStringVector(list,false);
        assertEquals("",bsv2.getString(3));
        BasicStringVector bsv3 = new BasicStringVector(list,true);
    }

    @Test(expected = Exception.class)
    public void test_BasicStringVector_set_notBlob() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add("KingBase");
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        bsv.set(2,new BasicPoint(6.8,4.4));
    }

    @Test(expected = Exception.class)
    public void test_BasicStringVector_set_notString() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add("KingBase");
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list);
        bsv.set(1,new BasicComplex(0.6,8.2));
    }

    @Test
    public void test_BasicStringVector_byteArray(){
        byte[][] array = new byte[3][5];
        array[0] = new byte[]{'J','u','n','i','t'};
        array[1] = new byte[]{'m','a','v','e','n'};
        array[2] = new byte[]{'R','e','d','i','s'};
        BasicStringVector bsv = new BasicStringVector(array);
        assertEquals("[Junit,maven,Redis]",bsv.getString());
    }

    @Test
    public void test_BasicStringVector_compare() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add("KingBase");
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list);
        Method method= bsv.getClass().getDeclaredMethod("compare",byte[].class,byte[].class);
        method.setAccessible(true);
        assertEquals(-1,method.invoke(bsv,new byte[]{'J','u','n','i','t'},new byte[]{'c','o','n','s','t'}));
        assertEquals(1,method.invoke(bsv,new byte[]{'d','i','c','t'},new byte[]{'c','o','n','s','t'}));
        assertEquals(1,method.invoke(bsv,new byte[]{'i','n','i','t','a'},new byte[]{'i','n','i','t'}));
        assertEquals(0,method.invoke(bsv,new byte[]{'i','n','i','t'},new byte[]{'i','n','i','t'}));
        assertEquals(-1,method.invoke(bsv,new byte[]{'i','n','i','t','a'},new byte[]{'i','n','i','t','a','b'}));
    }

    @Test
    public void test_BasicStringVector_serialize() throws IOException {
        ExtendedDataOutput out = new ExtendedDataOutput() {
            @Override
            public void writeString(String str) throws IOException {
                System.out.println(str);
            }

            @Override
            public void writeBlob(byte[] v) throws IOException {
                System.out.println(Arrays.toString(v));
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
        List<String> list = new ArrayList<>();
        list.add("MangoDB");
        list.add("GaussDB");
        list.add("GoldenDB");
        list.add("KingBase");
        list.add("DolphinDB");
        BasicStringVector bsv = new BasicStringVector(list);
        bsv.serialize(0,5,out);
        BasicStringVector bsv2 = new BasicStringVector(list,true);
        bsv2.serialize(0,5,out);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicStringVector_asof() throws IOException {
        List<String> list = new ArrayList<>();
        list.add("DolphinDB");
        list.add("GoldenDB");
        list.add("GaussDB");
        list.add("KingBase");
        list.add("MangoDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity value = conn.run("a=blob(\"English\");a;");
        assertEquals(0,bsv.asof((Scalar) value));
        bsv.asof(new BasicString("HikVision"));
    }
    @Test
    public void test_BasicStringVector_wvtb() throws IOException {
        List<String> list = new ArrayList<>();
        list.add("DolphinDB");
        list.add("GoldenDB");
        list.add("GaussDB");
        list.add("KingBase");
        list.add("MangoDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        BasicStringVector bsv2 = new BasicStringVector(list,false);
        ByteBuffer bb = bsv.writeVectorToBuffer(ByteBuffer.allocate(40));
        ByteBuffer bb2 = bsv2.writeVectorToBuffer(ByteBuffer.allocate(40));
        assertNotEquals(Arrays.toString(bb.array()),Arrays.toString(bb2.array()));
    }

    @Test
    public void test_BasicStringVector_serialize_bytebuffer(){
        List<String> list = new ArrayList<>();
        list.add("DolphinDB");
        list.add("GoldenDB");
        list.add("GaussDB");
        list.add("KingBase");
        list.add("MangoDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        BasicStringVector bsv2 = new BasicStringVector(list,false);
        assertEquals(bsv.serialize(new byte[40],40,0,5,new AbstractVector.Offect(1)),
                bsv2.serialize(new byte[40],40,0,5,new AbstractVector.Offect(1)));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicStringVector_serialize_zeroNumElement() throws IOException {
        BasicStringVector bsv = new BasicStringVector(4);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(0,1);
        bsv.serialize(0,1,0,numElementAndPartial,ByteBuffer.allocate(16));
    }

    @Test
    public void test_BasicStringVector_serialize_isBlob() throws IOException {
        List<String> list = new ArrayList<>();
        list.add("DolphinDB");
        list.add("GoldenDB");
        list.add("GaussDB");
        list.add("KingBase");
        list.add("MangoDB");
        BasicStringVector bsv = new BasicStringVector(list,true);
        BasicStringVector bsv2 = new BasicStringVector(list,false);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(0,0);
        assertEquals(36,bsv.serialize(0,0,5,numElementAndPartial,ByteBuffer.allocate(40)));
        assertEquals(36,bsv2.serialize(0,0,5,numElementAndPartial,ByteBuffer.allocate(40)));
    }

    @Test
    public void test_BasicStringVector_array(){
        String[] array = new String[]{"KingBase","vastBase","OceanBase"};
        BasicStringVector bsv = new BasicStringVector(array,true);
        BasicStringVector bsv2 = new BasicStringVector(array,false);
        assertEquals(0,bsv2.asof(new BasicString("iBase")));
    }

    @Test
    public void test_BasicStringVector_array_blob_copy(){
        String[] array = new String[]{"Dolphindb","MongoDB","GaussDB","GoldenDB"};
        BasicStringVector bsv = new BasicStringVector(array,true,true);
        BasicStringVector bsv2 = new BasicStringVector(array,true,false);
        assertEquals(bsv.getString(),bsv2.getString());
        BasicStringVector bsv3 = new BasicStringVector(array,false,true);
        BasicStringVector bsv4 = new BasicStringVector(array,false,false);
        assertEquals(bsv3.getString(),bsv4.getString());
        String[] array2 = new String[]{"a","b",null,"d"};
        BasicStringVector bsv7 = new BasicStringVector(array2,true,false);
        BasicStringVector bsv5 = new BasicStringVector(array2,true,true);
        BasicStringVector bsv6 = new BasicStringVector(array2,false,false);
        assertEquals(bsv5.getString(),bsv6.getString());
        assertEquals(bsv5.getString(),bsv7.getString());
    }

    @Test
    public void test_BasicStringVector_Append(){
        BasicStringVector bsv = new BasicStringVector(new String[]{"MySQL","Oracle"});
        int size = bsv.rows();
        bsv.Append(new BasicString("PortageSQL"));
        assertEquals(size+1,bsv.rows());
        bsv.Append(new BasicStringVector(new String[]{"GaussDB","GoldenDB"}));
        assertEquals(size+3,bsv.rows());
    }
    @Test
    public void test_BasicString_big_blob_data() throws IOException {
        BasicString re1 =(BasicString) conn.run("blob(concat(take(`abcd中文123,100000)))");
        //System.out.println(re1.getString());
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals(data.getString(),re1.getString());
    }
    @Test
    public void test_BasicString_big_string_data() throws IOException {
        BasicString re1 =(BasicString) conn.run("string(concat(take(`abcd中文123,100000)))");
       // System.out.println(re1.getString());
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals(data.getString(),re1.getString());
    }
    @Test
    public void test_BasicStringVector_big_blob_data() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("blob([concat(take(`abcd中文123,100000))])");
        //System.out.println(re1.getString());
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals("["+data.getString()+"]",re1.getString());
    }
    @Test
    public void test_BasicStringVector_big_string_data() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("string([concat(take(`abcd中文123,100000))])");
        System.out.println(re1.getString());
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals("["+data.getString()+"]",re1.getString());
    }
    @Test
    public void test_BasicStringVector_big_symbol_data() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("symbol([concat(take(`abcd中文123,100000))])");
        System.out.println(re1.getString());
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals("["+data.getString()+"]",re1.getString());
    }
    @Test
    public void test_BasicStringVector_run_array_blob_bigdata() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("t = array(BLOB,10).append!(blob(concat(take(`abcd中文123,100000))));t");
        System.out.println(re1.get(10).getString());
        for(int i = 0; i < 10; i++) {
            assertEquals("",re1.get(i).getString());
        }
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals(data.getString(),re1.get(10).getString());
    }
    @Test
    public void test_BasicStringVector_run_array_bigdata() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("a = array(STRING,10).append!(string(concat(take(\"123&#@!^%;d《》中文\",100000))));a");
        System.out.println(re1.get(10).getString());
        for(int i = 0; i < 10; i++) {
            assertEquals("",re1.get(i).getString());
        }
        String d = "123&#@!^%;d《》中文";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals(data.getString(),re1.get(10).getString());
    }
    @Test
    public void test_BasicStringVector_run_array_bigdata_1() throws IOException {
        BasicStringVector re1 =(BasicStringVector) conn.run("t = array(STRING,10).append!(string(concat(take(`abcd中文123,100000))));t");
        System.out.println(re1.get(10).getString());
        for(int i = 0; i < 10; i++) {
            assertEquals("",re1.get(i).getString());
        }
        String d = "abcd中文123";
        String dd = "";
        for(int i = 0; i < 100000; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        assertEquals(data.getString(),re1.get(10).getString());
    }
    @Test
    public void test_BasicStringVector_upload() throws IOException {
        Map<String, Entity> map = new HashMap<String, Entity>();
        BasicStringVector stringv =(BasicStringVector) conn.run("t = array(STRING,10).append!(string(concat(take(`abcd中文123,100000))));t");
        map.put("stringv", stringv);
        String dd = null;
        try{
            conn.upload(map);
        }catch(Exception e){
            dd=e.getMessage();
        }
        assertEquals("Serialized string length must less than 256k bytes.",dd);
    }
    @Test
    public void test_BasicString_toJSONString() throws Exception {
        BasicString bs = new BasicString("stringggggsere123");
        String re = JSONObject.toJSONString(bs);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_STRING\",\"dictionary\":false,\"jsonString\":\"\\\"stringggggsere123\\\"\",\"matrix\":false,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"stringggggsere123\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicStringVector_toJSONString() throws Exception {
        String[] array = new String[]{"Dolphindb","MongoDB","GaussDB","GoldenDB"};
        BasicStringVector bsv = new BasicStringVector(array,false,true);
        String re = JSONObject.toJSONString(bsv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_STRING\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicString\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[Dolphindb,MongoDB,GaussDB,GoldenDB]\",\"table\":false,\"unitLength\":1,\"vector\":true}", re);
    }
    @Test
    public void test_BasicStringMatrix_toJSONString() throws Exception {
        BasicStringMatrix bsm = new BasicStringMatrix(2,2);
        String re = JSONObject.toJSONString(bsm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_SYMBOL\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicString\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicString_BLOB_toJSONString() throws Exception {
        BasicString bs = new BasicString("s123",true);
        String re = JSONObject.toJSONString(bs);
        System.out.println(re);
        assertEquals("{\"blobValue\":[115,49,50,51],\"chart\":false,\"chunk\":false,\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_BLOB\",\"dictionary\":false,\"jsonString\":\"\\\"s123\\\"\",\"matrix\":false,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"s123\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicStringVector_BLOB_toJSONString() throws Exception {
        String[] array = new String[]{"Dolphindb","MongoDB","GaussDB","GoldenDB"};
        BasicStringVector bsv = new BasicStringVector(array,true,true);
        String re = JSONObject.toJSONString(bsv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataByteArray\":[[68,111,108,112,104,105,110,100,98],[77,111,110,103,111,68,66],[71,97,117,115,115,68,66],[71,111,108,100,101,110,68,66]],\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_BLOB\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicString\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[Dolphindb,MongoDB,GaussDB,GoldenDB]\",\"table\":false,\"unitLength\":1,\"vector\":true}", re);
    }
    @Test
    public void test_BasicStringVector_setNull() throws Exception {
        BasicStringVector bsv = new BasicStringVector(1);
        bsv.setNull(0);
        System.out.println(bsv.getString());
        assertEquals("[]",bsv.getString());
    }
    @Test
    public void test_BasicStringVector_setNull_tableInsert() throws IOException {
        conn.run("t = table(1:0,[`col1],[STRING]);share t as test_null\n");
        List<String> colNames = Arrays.asList("col1");
        List<Vector> cols = new ArrayList<>();
        BasicStringVector stringVector = new BasicStringVector(1);
        stringVector.setNull(0);
        cols.add(stringVector);
        List<String> allColNames = new ArrayList<String>();
        allColNames.addAll(colNames);
        BasicTable table = new BasicTable(allColNames, cols);
        List<Entity> list = new ArrayList<>(1);
        list.add(table);
        conn.run("tableInsert{test_null}", list);
        BasicTable re = (BasicTable)conn.run("select * from test_null");
        assertEquals(1,re.rows());
    }
}
