package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void test_BasicInt128_toJSONString(){
        BasicInt128 bi128 = new BasicInt128(75L,10L);
        String re = JSONObject.toJSONString(bi128);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_INT128\",\"dictionary\":false,\"jsonString\":\"\\\"000000000000004b000000000000000a\\\"\",\"leastSignicantBits\":10,\"long2\":{\"high\":75,\"low\":10,\"null\":false},\"matrix\":false,\"mostSignicantBits\":75,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"000000000000004b000000000000000a\",\"table\":false,\"vector\":false}", re);
    }
}
