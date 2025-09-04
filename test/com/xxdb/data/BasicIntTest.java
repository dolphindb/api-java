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

public class BasicIntTest {
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
        BasicIntVector v = new BasicIntVector(6);
        v.setInt(0,2147483647);
        v.setInt(1,-2147483647);
        v.setInt(2,99);
        v.setInt(3,0);
        v.setInt(4,-12);
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
    public void test_BasicInt() throws Exception {
        BasicInt bi = new BasicInt(Integer.MIN_VALUE);
        assertNotNull(bi.getNumber());
        assertFalse(bi.equals(null));
    }

    @Test(expected = Exception.class)
    public void test_BasicIntMatrix_ListNull() throws Exception {
        BasicIntMatrix bim = new BasicIntMatrix(1,1,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicIntMatrix_arrayNull() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{8,11});
        list.add(null);
        BasicIntMatrix bim = new BasicIntMatrix(2,2,list);
    }

    @Test
    public void test_BasicIntMatrix() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1,4,5});
        list.add(new int[]{Integer.MIN_VALUE,0,Integer.MAX_VALUE});
        list.add(new int[]{5,2,1});
        BasicIntMatrix bim = new BasicIntMatrix(3,3,list);
        assertTrue(bim.isNull(0,1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bim.getDataCategory());
        assertEquals(BasicInt.class,bim.getElementClass());
        bim.setNull(1,1);
        assertTrue(bim.isNull(1,1));
    }

    @Test
    public void test_intValue() throws Exception {
        BasicInt bb = new BasicInt(1234);
        bb.setNull();
        assertEquals(null,bb.intValue());
        BasicInt bb1 = new BasicInt(1234);
        assertEquals("1234",bb1.intValue().toString());
    }
    @Test
    public void test_BasicInt_toJSONString() throws Exception {
        BasicInt bi = new BasicInt(Integer.MIN_VALUE);
        String re = JSONObject.toJSONString(bi);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"int\":-2147483648,\"jsonString\":\"null\",\"matrix\":false,\"null\":true,\"number\":-2147483648,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void test_BasicIntMatrix_toJSONString() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1,4,5});
        list.add(new int[]{Integer.MIN_VALUE,0,Integer.MAX_VALUE});
        list.add(new int[]{5,2,1});
        BasicIntMatrix bim = new BasicIntMatrix(3,3,list);
        String re = JSONObject.toJSONString(bim);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicInt\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1         #2\\n1             5 \\n4  0          2 \\n5  2147483647 1 \\n\",\"table\":false,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicIntMatrix_getScale() throws Exception {
        BasicIntMatrix bdm = new BasicIntMatrix(2,2);
        bdm.setInt(0,0, 2);
        bdm.setInt(0,1, 5);
        bdm.setInt(1,0, 2);
        bdm.setInt(1,1, 3);
        bdm.getScale();
    }

}
