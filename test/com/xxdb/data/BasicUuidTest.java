package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicUuidTest {
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
        BasicUuidVector v = new BasicUuidVector(6);
        v.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        v.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        v.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        v.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        v.set(4,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        v.setNull(5);
        for(int b : num){
            for(int i=0;i<v.rows();i++){
                int expected = getServerHash(v.get(i),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, v.get(i).hashBucket(b));
            }
        }
    }

    @Test
    public void test_BasicUuid_toJSONString() throws Exception {
        BasicUuid buv = new BasicUuid(1,2);
        String re = JSONObject.toJSONString(buv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_UUID\",\"dictionary\":false,\"jsonString\":\"\\\"00000000-0000-0001-0000-000000000002\\\"\",\"leastSignicantBits\":2,\"long2\":{\"high\":1,\"low\":2,\"null\":false},\"matrix\":false,\"mostSignicantBits\":1,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"00000000-0000-0001-0000-000000000002\",\"table\":false,\"vector\":false}", re);
    }

}
