package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicShortTest {
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
    private int getServerHash(Scalar s, int bucket) throws IOException {
        List<Entity> args = new ArrayList<>();
        args.add(s);
        args.add(new BasicInt(bucket));
        BasicInt re = (BasicInt)conn.run("hashBucket",args);
        return re.getInt();
    }

    @Test
    public void test_getHash() throws  IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicShortVector v = new BasicShortVector(6);
        v.setShort(0,(short)32767);
        v.setShort(1,(short)-32767);
        v.setShort(2,(short)12);
        v.setShort(3,(short)0);
        v.setShort(4,(short)-12);
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
    public void test_isNull() throws Exception {
        BasicShort bs = new BasicShort(Short.MIN_VALUE);
        assertTrue(bs.isNull());
        assertEquals("",bs.getString());
        assertFalse(new BasicShort(Short.MAX_VALUE).equals(null));
        assertEquals("11",new BasicShort((short) 11).getJsonString());
    }

    @Test
    public void test_BasicShortMatrix_BasicFunctions(){
        BasicShortMatrix bsm = new BasicShortMatrix(2,2);
        bsm.setShort(0,0, (short) 7);
        bsm.setShort(0,1, (short) 3);
        bsm.setShort(1,0, (short) 19);
        bsm.setShort(1,1,Short.MAX_VALUE);
        assertEquals(7,bsm.getShort(0,0));
        assertEquals(new BasicShort(Short.MAX_VALUE),bsm.get(1,1));
        bsm.setNull(1,1);
        assertTrue(bsm.isNull(1,1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bsm.getDataCategory());
        assertEquals(BasicShort.class,bsm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicShortMatrix_listNull() throws Exception {
        BasicShortMatrix bsm = new BasicShortMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicShortMatrix_ArrayNull() throws Exception {
        List<short[]> list = new ArrayList<>();
        list.add(new short[]{3,6,9});
        list.add(new short[]{2,5,8});
        list.add(null);
        BasicShortMatrix bsm = new BasicShortMatrix(3,3,list);
    }

    @Test
    public void test_shortValue() throws Exception {
        BasicShort bb = new BasicShort((short) 88);
        bb.setNull();
        assertEquals(null,bb.shortValue());
        BasicShort bb1 = new BasicShort((short) 88);
        assertEquals("88",bb1.shortValue().toString());
    }
    @Test
    public void test_BasicShort_toJSONString() throws Exception {
        BasicShort bb1 = new BasicShort((short) 88);
        String re = JSONObject.toJSONString(bb1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"jsonString\":\"88\",\"matrix\":false,\"null\":false,\"number\":88,\"pair\":false,\"scalar\":true,\"short\":88,\"string\":\"88\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void test_BasicShortMatrix_toJSONString() throws Exception {
        BasicShortMatrix bsm = new BasicShortMatrix(2,2);
        String re = JSONObject.toJSONString(bsm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicShort\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1\\n0  0 \\n0  0 \\n\",\"table\":false,\"vector\":false}", re);
    }
}
