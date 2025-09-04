package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicUuidVectorTest {
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
    @Test
    public void test_BasicUUidVector_Basic(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(9000L,600L));
        list.add(new Long2(888L,200L));
        list.add(new Long2(9000L,10L));
        BasicUuidVector buv = new BasicUuidVector(list);
        assertEquals(BasicUuid.class,buv.getElementClass());
        System.out.println(buv.get(1));
        assertEquals("[00000000-0000-2328-0000-00000000000a,00000000-0000-2328-0000-000000000258,00000000-0000-0378-0000-0000000000c8]",buv.getSubVector(new int[]{2,0,1}).getString());
    }

    @Test
    public void test_BasicUuidVector_capacity_lt_size() throws Exception {
        BasicUuidVector bbv = new BasicUuidVector(5,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Long2(888L,800L));
        bbv.set(3,new Long2(0L,0L));
        bbv.set(4, new Long2(220L,25L));
        Assert.assertEquals("[,,00000000-0000-0378-0000-000000000320,,00000000-0000-00dc-0000-000000000019]", bbv.getString());
    }

    @Test
    public void test_BasicUuidVector_size_capacity_set() throws Exception {
        BasicUuidVector bbv = new BasicUuidVector(5,6);
        Assert.assertEquals("[,,,,]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Long2(888L,800L));
        bbv.set(3,new Long2(0L,0L));
        bbv.set(4, new Long2(220L,25L));
        Assert.assertEquals("[,,00000000-0000-0378-0000-000000000320,,00000000-0000-00dc-0000-000000000019]", bbv.getString());
    }

    @Test
    public void test_BasicUuidVector_size_capacity_add() throws Exception {
        BasicUuidVector bbv = new BasicUuidVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(new Long2(888L,800L));
        bbv.add(new Long2(0L,0L));
        bbv.add(new Long2(220L,25L));
        Assert.assertEquals("[,,00000000-0000-0378-0000-000000000320,,00000000-0000-00dc-0000-000000000019]", bbv.getString());
    }

    @Test
    public void test_BasicUuidVector_set_type_not_match() throws Exception {
        BasicUuidVector bbv = new BasicUuidVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long2 or null is supported.", re);
    }

    @Test
    public void test_BasicUuidVector_add_type_not_match() throws Exception {
        BasicUuidVector bbv = new BasicUuidVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long2 or null is supported.", re);
    }
    
    @Test
    public void test_BasicUUidVector_df(){
        BasicUuidVector buv = new BasicUuidVector(Entity.DATA_FORM.DF_VECTOR,2);
        System.out.println(buv.getString());
    }

    @Test
    public void test_BasicUuidVector_Append() throws Exception {
        BasicUuidVector buv = new BasicUuidVector(new Long2[]{new Long2(35,11)});
        int size = buv.size;
        int capacity = buv.capacity;
        buv.Append(new BasicUuid(19,7));
        assertEquals(capacity*2,buv.capacity);
        buv.Append(new BasicUuid(28,16));
        assertEquals(capacity*4,buv.capacity);
        buv.Append(new BasicUuidVector(new Long2[]{new Long2(46,29),new Long2(28,12)}));
        assertEquals(capacity*4+2,buv.capacity);
        assertEquals(size+4,buv.size);
    }

    @Test
    public void TestCombineUuidVector() throws Exception {
        BasicUuidVector v = new BasicUuidVector(4);
        v.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        v.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        v.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        v.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        BasicUuidVector vector2 = new BasicUuidVector(2 );
        vector2.set(0,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        vector2.set(1,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        BasicUuidVector res= (BasicUuidVector) v.combine(vector2);
        BasicUuidVector res128 = new BasicUuidVector(6);
        res128.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        res128.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        res128.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        res128.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        res128.set(4,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        res128.set(5,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(6,res.rows());
    }

    @Test
    public void test_BasicUuidVector_toJSONString() throws Exception {
        BasicUuidVector buv = new BasicUuidVector(Entity.DATA_FORM.DF_VECTOR,2);
        String re = JSONObject.toJSONString(buv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[{\"high\":0,\"low\":0,\"null\":true},{\"high\":0,\"low\":0,\"null\":true}],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_UUID\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicUuid\",\"jsonString\":\"[,]\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[,]\",\"table\":false,\"unitLength\":16,\"values\":[{\"high\":0,\"low\":0,\"null\":true},{\"high\":0,\"low\":0,\"null\":true}],\"vector\":true}", re);
    }
}
