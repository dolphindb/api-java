package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class BasicLongTest {
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
        BasicLongVector v = new BasicLongVector(6);
        v.setLong(0,9223372036854775807l);
        v.setLong(1,-9223372036854775807l);
        v.setLong(2,12);
        v.setLong(3,0);
        v.setLong(4,-12);
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
    public void test_BasicLong() throws Exception {
        BasicLong bl = new BasicLong(9000L);
        assertEquals("9000",bl.getJsonString());
        assertTrue(new BasicLong(Long.MIN_VALUE).isNull());
        assertEquals("",new BasicLong(Long.MIN_VALUE).getString());
        assertFalse(bl.equals(null));
    }

    @Test
    public void test_BasicLongMatrix(){
        BasicLongMatrix blm = new BasicLongMatrix(2,2);
        blm.setLong(0,0,Long.MAX_VALUE);
        blm.setLong(0,1,7420L);
        blm.setLong(1,0,9820L);
        blm.setLong(1,1,Long.MIN_VALUE);
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,blm.getDataCategory());
        assertEquals(BasicLong.class,blm.getElementClass());
        assertTrue(blm.isNull(1,1));
        assertFalse(blm.isNull(1,0));
        blm.setNull(0,0);
        assertTrue(blm.isNull(0,0));
    }

    @Test(expected = Exception.class)
    public void test_BasicLongMatrix_listNull() throws Exception {
        BasicLongMatrix blm = new BasicLongMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicLongMatrix_arrNull() throws Exception {
        List<long[]> list = new ArrayList<>();
        list.add(new long[]{7420L,9810L,9820L});
        list.add(new long[]{659L,810L,990L});
        list.add(null);
        BasicLongMatrix blm = new BasicLongMatrix(3,3,list);
    }

    @Test
    public void test_longValue() throws Exception {
        BasicLong bb = new BasicLong(860L);
        bb.setNull();
        assertEquals(null,bb.longValue());
        BasicLong bb1 = new BasicLong(860L);
        assertEquals("860",bb1.longValue().toString());
    }
    @Test
    public void test_BasicLong_toJSONString() throws Exception {
        BasicLong bl = new BasicLong(9000L);
        String re = JSONObject.toJSONString(bl);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_LONG\",\"dictionary\":false,\"jsonString\":\"9000\",\"long\":9000,\"matrix\":false,\"null\":false,\"number\":9000,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"9000\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicLongVector_toJSONString() throws Exception {
        BasicLongVector blv = new BasicLongVector(new long[]{600,615,617});
        String re = JSONObject.toJSONString(blv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[600,615,617],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_LONG\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicLong\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[600,615,617]\",\"table\":false,\"unitLength\":16,\"values\":[600,615,617],\"vector\":true}", re);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicLongMatrix_getScale() throws Exception {
        BasicLongMatrix bdm = new BasicLongMatrix(2,2);
        bdm.setLong(0,0, 2);
        bdm.setLong(0,1, 5);
        bdm.setLong(1,0, 2);
        bdm.setLong(1,1, 3);
        bdm.getScale();
    }
}
