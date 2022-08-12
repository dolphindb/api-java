package com.xxdb.data;

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
import static org.junit.Assert.assertNull;

public class BasicIPAddrTest {
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
        BasicIPAddrVector v = new BasicIPAddrVector(6);
        v.set(0,BasicIPAddr.fromString("dc70:a4c2:f0f7:81da:334:66e3:b915:a254"));
        v.set(1,BasicIPAddr.fromString("72e1:e064:b242:5386:109:bdcb:639c:9e63"));
        v.set(2,BasicIPAddr.fromString("5d42:fc4f:efb2:6735:e5be:1a5d:ebf8:b987"));
        v.set(3,BasicIPAddr.fromString("fa6b:bf42:cfb4:1bea:3551:1cbc:2c99:9128"));
        v.set(4,BasicIPAddr.fromString("6e01:2a6e:b3b0:323a:745:1527:1537:8019"));
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
    public void TestCombineIpAddrVector() throws Exception {
        BasicIPAddrVector v = new BasicIPAddrVector(4);
        v.set(0,BasicIPAddr.fromString("dc70:a4c2:f0f7:81da:334:66e3:b915:a254"));
        v.set(1,BasicIPAddr.fromString("72e1:e064:b242:5386:109:bdcb:639c:9e63"));
        v.set(2,BasicIPAddr.fromString("5d42:fc4f:efb2:6735:e5be:1a5d:ebf8:b987"));
        v.set(3,BasicIPAddr.fromString("fa6b:bf42:cfb4:1bea:3551:1cbc:2c99:9128"));
        BasicIPAddrVector vector2 = new BasicIPAddrVector(2 );
        vector2.set(0,BasicIPAddr.fromString("fa6b:bf42:cfb4:1bea:3551:1cbc:2c99:9128"));
        vector2.set(1,BasicIPAddr.fromString("6e01:2a6e:b3b0:323a:745:1527:1537:8019"));
        BasicIPAddrVector res= (BasicIPAddrVector) v.combine(vector2);
        BasicIPAddrVector res128 = new BasicIPAddrVector(6);
        res128.set(0,BasicIPAddr.fromString("dc70:a4c2:f0f7:81da:334:66e3:b915:a254"));
        res128.set(1,BasicIPAddr.fromString("72e1:e064:b242:5386:109:bdcb:639c:9e63"));
        res128.set(2,BasicIPAddr.fromString("5d42:fc4f:efb2:6735:e5be:1a5d:ebf8:b987"));
        res128.set(3,BasicIPAddr.fromString("fa6b:bf42:cfb4:1bea:3551:1cbc:2c99:9128"));
        res128.set(4,BasicIPAddr.fromString("fa6b:bf42:cfb4:1bea:3551:1cbc:2c99:9128"));
        res128.set(5,BasicIPAddr.fromString("6e01:2a6e:b3b0:323a:745:1527:1537:8019"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(6,res.rows());
    }
    @Test
    public void TestCombineIpAddr_v4Vector() throws Exception {
        BasicIPAddrVector v = new BasicIPAddrVector(2);
        v.set(0,BasicIPAddr.fromString("192.168.1.13"));
        v.set(1,BasicIPAddr.fromString("192.168.1.142"));
        BasicIPAddrVector vector2 = new BasicIPAddrVector(2 );
        vector2.set(0,BasicIPAddr.fromString("192.168.1.142"));
        vector2.set(1,BasicIPAddr.fromString("192.168.1.142"));
        BasicIPAddrVector res= (BasicIPAddrVector) v.combine(vector2);
        BasicIPAddrVector res128 = new BasicIPAddrVector(4);
        res128.set(0,BasicIPAddr.fromString("192.168.1.13"));
        res128.set(1,BasicIPAddr.fromString("192.168.1.142"));
        res128.set(2,BasicIPAddr.fromString("192.168.1.142"));
        res128.set(3,BasicIPAddr.fromString("192.168.1.142"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(4,res.rows());
    }

    @Test
    public void test_BasicIPAddr_fromString(){
        assertNull(BasicIPAddr.fromString("6e01:1"));
    }

    @Test
    public void test_BasicIPAddr_parseIP4(){
        assertNull(BasicIPAddr.parseIP4("192.168.100.10.5"));
        assertNull(BasicIPAddr.parseIP4("19A.245.12.30"));
        assertNull(BasicIPAddr.parseIP4("192.255.10"));
    }

    @Test
    public void test_BasicIPAddr_ParseIP6(){
        assertNull(BasicIPAddr.parseIP6("6e01:2a6e:b3b0:323a:745:1527:1537:8019:7008"));
        assertNull(BasicIPAddr.parseIP6("6e01:2a6e:b3b0:323a:745:1527:1537"));
        assertNull(BasicIPAddr.parseIP6("6e01:2a6e:b3b0:323a:745:1527:1537:801#"));
        assertEquals("1:2a6e:b3b0:323a:745:1527:1537:801",BasicIPAddr.parseIP6("::01:2a6e:b3b0:323a:745:1527:1537:801").getString());
    }

    @Test
    public void test_BasicIPAddrVector_list(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(473849509537L,2234859305L));
        list.add(new Long2(55887799882L,110044556L));
        BasicIPAddrVector biav = new BasicIPAddrVector(list);
        assertEquals("[0:d:32c:264a::68f:258c,0:6e:53a1:b6a1::8535:3f29,0:d:32c:264a::68f:258c]",biav.getSubVector(new int[]{1,0,1}).getString());
        assertEquals(BasicIPAddr.class,biav.getElementClass());
    }

    @Test
    public void test_BasicIPAddrVector_DF_size(){
        BasicIPAddrVector biav2 = new BasicIPAddrVector(Entity.DATA_FORM.DF_VECTOR,0);
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(473849509537L,2234859305L));
        list.add(new Long2(55887799882L,110044556L));
        BasicIPAddrVector biav = new BasicIPAddrVector(list);
        BasicIPAddrVector biav3 = (BasicIPAddrVector) biav2.combine(biav);
        assertEquals("[0:6e:53a1:b6a1::8535:3f29,0:d:32c:264a::68f:258c]",biav3.getString());

    }
}
