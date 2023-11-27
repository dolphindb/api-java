package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.Scalar;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.xxdb.data.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicInt128Test {
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
    public void test_getHash() throws  Exception{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicInt128Vector v = new BasicInt128Vector(6);

        v.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        v.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        v.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        v.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        v.set(4,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
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
    public void TestCombineInt128Vector() throws Exception {
        BasicInt128Vector v = new BasicInt128Vector(4);
        v.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        v.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        v.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        v.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        BasicInt128Vector vector2 = new BasicInt128Vector(2 );
        vector2.set(0,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        vector2.set(1,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
        BasicInt128Vector res= (BasicInt128Vector) v.combine(vector2);
        BasicInt128Vector res128 = new BasicInt128Vector(6);
        res128.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        res128.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        res128.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        res128.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        res128.set(4,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        res128.set(5,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(6,res.rows());
    }

    @Test
    public void test_BasicInt128(){
        BasicInt128 bi128 = new BasicInt128(75L,10L);
        assertEquals(10L,bi128.getLong2().low);
        assertEquals(75L,bi128.getLong2().high);
        assertFalse(bi128.equals(new BasicDuration(Entity.DURATION.MONTH,Integer.MAX_VALUE)));
    }

    @Test(expected = NumberFormatException.class)
    public void test_BasicInt128_formString_false(){
        BasicInt128.fromString("130d6d5a0536c99ac7f9a0");
    }

    @Test
    public void test_BasicInt128Vector_list(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(980L,5L));
        list.add(null);
        list.add(new Long2(2022L,0L));
        BasicInt128Vector bi128v = new BasicInt128Vector(list);
        assertTrue(bi128v.isNull(1));
        bi128v.setInt128(1,1566L,220L);
        assertEquals("[000000000000061e00000000000000dc" +
                ",00000000000007e60000000000000000," +
                "00000000000003d40000000000000005]",bi128v.getSubVector(new int[]{1,2,0}).getString());
        assertEquals(Entity.DATA_CATEGORY.BINARY,bi128v.getDataCategory());
        assertEquals(BasicInt128.class,bi128v.getElementClass());
    }
    @Test
    public void test_BasicInt128_toJSONString(){
        BasicInt128 bi128 = new BasicInt128(75L,10L);
        String re = JSONObject.toJSONString(bi128);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_INT128\",\"dictionary\":false,\"jsonString\":\"\\\"000000000000004b000000000000000a\\\"\",\"leastSignicantBits\":10,\"long2\":{\"high\":75,\"low\":10,\"null\":false},\"matrix\":false,\"mostSignicantBits\":75,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"000000000000004b000000000000000a\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicInt128Vector_toJSONString(){
        BasicInt128Vector bi128v = new BasicInt128Vector(new Long2[]{new Long2(17L,6L),new Long2(29L,2L)});
        String re = JSONObject.toJSONString(bi128v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[{\"high\":17,\"low\":6,\"null\":false},{\"high\":29,\"low\":2,\"null\":false}],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_INT128\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicInt128\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00000000000000110000000000000006,000000000000001d0000000000000002]\",\"table\":false,\"unitLength\":16,\"vector\":true}", re);
    }
}
