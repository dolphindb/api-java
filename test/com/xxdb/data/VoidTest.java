package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;
public class VoidTest {
    @Test
    public void test_void_basic(){
        Void v = new Void();
        assertEquals(Entity.DATA_CATEGORY.NOTHING,v.getDataCategory());
        assertEquals("",v.getString());
        assertEquals("",v.getJsonString());
        assertEquals(0,v.hashCode());
        assertEquals(-1,v.hashBucket(2));
        assertTrue(v.equals(new Void()));
        assertFalse(v.equals(null));
        v.setNull();
    }

    @Test(expected = Exception.class)
    public void test_void_getNumber() throws Exception {
        Void v = new Void();
        v.getNumber();
    }

    @Test(expected = Exception.class)
    public void test_void_getTemporal() throws Exception {
        Void v = new Void();
        v.getTemporal();
    }
    @Test
    public void test_void_toJSONString(){
        Void v = new Void();
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"NOTHING\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_VOID\",\"dictionary\":false,\"jsonString\":\"\",\"matrix\":false,\"null\":true,\"pair\":false,\"scalar\":true,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }
}
