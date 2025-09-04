package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
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
        bs.setString("Junit");
        assertEquals("Junit",bs.getString());
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
    public void test_BasicString_toJSONString() throws Exception {
        BasicString bs = new BasicString("stringggggsere123");
        String re = JSONObject.toJSONString(bs);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"LITERAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_STRING\",\"dictionary\":false,\"jsonString\":\"\\\"stringggggsere123\\\"\",\"matrix\":false,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"stringggggsere123\",\"table\":false,\"vector\":false}", re);
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
}
