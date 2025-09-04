package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BasicIPAddrVectorTest {

    @Test
    public void test_BasicIPAddrVector_list(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(473849509537L,2234859305L));
        list.add(new Long2(55887799882L,110044556L));
        BasicIPAddrVector biav = new BasicIPAddrVector(list);
        assertEquals("0:d:32c:264a::68f:258c",biav.get(1).getString());
        assertEquals("[0:d:32c:264a::68f:258c,0:6e:53a1:b6a1::8535:3f29,0:d:32c:264a::68f:258c]",biav.getSubVector(new int[]{1,0,1}).getString());
        assertEquals(BasicIPAddr.class,biav.getElementClass());
    }

    @Test
    public void test_BasicIPAddrVector_capacity_lt_size() throws Exception {
        BasicIPAddrVector bbv = new BasicIPAddrVector(5,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Long2(888L,800L));
        bbv.set(3,new Long2(0L,0L));
        bbv.set(4, new Long2(220L,25L));
        Assert.assertEquals("[0.0.0.0,0.0.0.0,0::378:0:0:0:320,0.0.0.0,0::dc:0:0:0:19]", bbv.getString());
    }

    @Test
    public void test_BasicIPAddrVector_size_capacity_set() throws Exception {
        BasicIPAddrVector bbv = new BasicIPAddrVector(5,6);
        Assert.assertEquals("[0.0.0.0,0.0.0.0,0.0.0.0,0.0.0.0,0.0.0.0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Long2(888L,800L));
        bbv.set(3,new Long2(0L,0L));
        bbv.set(4, new Long2(220L,25L));
        Assert.assertEquals("[0.0.0.0,0.0.0.0,0::378:0:0:0:320,0.0.0.0,0::dc:0:0:0:19]", bbv.getString());
    }

    @Test
    public void test_BasicIPAddrVector_size_capacity_add() throws Exception {
        BasicIPAddrVector bbv = new BasicIPAddrVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(new Long2(888L,800L));
        bbv.add(new Long2(0L,0L));
        bbv.add(new Long2(220L,25L));
        Assert.assertEquals("[0.0.0.0,0.0.0.0,0::378:0:0:0:320,0.0.0.0,0::dc:0:0:0:19]", bbv.getString());
    }

    @Test
    public void test_BasicIPAddrVector_set_type_not_match() throws Exception {
        BasicIPAddrVector bbv = new BasicIPAddrVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long2 or null is supported.", re);
    }

    @Test
    public void test_BasicIPAddrVector_add_type_not_match() throws Exception {
        BasicIPAddrVector bbv = new BasicIPAddrVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long2 or null is supported.", re);
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

    @Test
    public void test_BasicIPAddrVector_Append() throws Exception {
        BasicIPAddrVector bipv = new BasicIPAddrVector(new Long2[]{new Long2(18L,11L),new Long2(9000L,659L)});
        int size = bipv.size;
        int capacity = bipv.capacity;
        bipv.Append(new BasicIPAddr(1300L,800L));
        bipv.Append(new BasicIPAddr(888L,600L));
        assertEquals(size+2,bipv.size);
        assertEquals(capacity*2,bipv.capacity);
        bipv.Append(new BasicIPAddrVector(new Long2[]{new Long2(8100L,1300L),new Long2(820L,710L)}));
        assertEquals(capacity*2+2,bipv.capacity);
        assertEquals(size+4,bipv.rows());
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
    public void test_BasicIPAddrVector_toJSONString() throws Exception {
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(473849509537L,2234859305L));
        list.add(new Long2(55887799882L,110044556L));
        BasicIPAddrVector biav = new BasicIPAddrVector(list);
        String re = JSONObject.toJSONString(biav);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[{\"high\":473849509537,\"low\":2234859305,\"null\":false},{\"high\":55887799882,\"low\":110044556,\"null\":false}],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_IPADDR\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicIPAddr\",\"jsonString\":\"[0:6e:53a1:b6a1::8535:3f29,0:d:32c:264a::68f:258c]\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[0:6e:53a1:b6a1::8535:3f29,0:d:32c:264a::68f:258c]\",\"table\":false,\"unitLength\":16,\"values\":[{\"high\":473849509537,\"low\":2234859305,\"null\":false},{\"high\":55887799882,\"low\":110044556,\"null\":false}],\"vector\":true}", re);
    }
}
