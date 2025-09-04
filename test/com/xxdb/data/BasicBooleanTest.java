package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class BasicBooleanTest {
    @Test
    public void test_BasicBoolean() throws Exception {
        BasicBoolean bb = new BasicBoolean(Byte.MIN_VALUE);
        assertEquals(1,bb.columns(),0);
        assertEquals(1,bb.rows());
        assertTrue(bb.isNull());
        assertFalse(bb.equals(3));
    }

    @Test(expected = Exception.class)
    public void test_BasicBooleanMatrix_null() throws Exception {
        BasicBooleanMatrix bbm = new BasicBooleanMatrix(2,2);
        bbm.setBoolean(0,1,false);
        bbm.setBoolean(0,0,true);
        bbm.setBoolean(1,0,false);
        bbm.setBoolean(1,1,true);
        System.out.println(bbm.getColumnLabel(1).getString());
        System.out.println(bbm.getColumnLabels());
        System.out.println(bbm.getRowLabels());
        assertFalse(bbm.getBoolean(1,0));
        assertFalse(bbm.isNull(1,1));
        assertEquals("true",bbm.get(0,0).getString());
        bbm.setNull(1,1);
        assertTrue(bbm.isNull(1,1));
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,bbm.getDataCategory());
        assertEquals(BasicBoolean.class,bbm.getElementClass());
        List<byte[]> listofarrays = new ArrayList<>();
        listofarrays.add(new byte[]{0,1,Byte.MIN_VALUE});
        listofarrays.add(new byte[]{Byte.MAX_VALUE,1,0});
        BasicBooleanMatrix bbm2 = new BasicBooleanMatrix(3,2,listofarrays);
        assertTrue(bbm2.isNull(2,0));
        BasicBooleanMatrix bbm3 = new BasicBooleanMatrix(1,1, null);
    }

    @Test(expected = Exception.class)
    public void test_BasicBooleanMatrix_lessRows() throws Exception {
        List<byte[]> listofarrays = new ArrayList<>();
        listofarrays.add(new byte[]{0,1,Byte.MIN_VALUE});
        listofarrays.add(new byte[]{Byte.MAX_VALUE,1,0});
        BasicBooleanMatrix bbm = new BasicBooleanMatrix(4,2,listofarrays);
    }

    @Test
    public void test_basicBooleanMartix(){
        List<byte[]> value1 = new ArrayList<>();
        for (int i = 0; i < 4; i++){
            byte[] data = new byte[4];
            for (int j = 0; j < 4; j++){
                data[j] = (byte) j;
            }
            value1.add(data);
        }
        try {
            BasicBooleanMatrix bm = new BasicBooleanMatrix(4, 4, null);
        }catch (Exception e){
            assertEquals("input list of arrays does not have 4 columns", e.getMessage());
        }

        try {
            BasicBooleanMatrix bm = new BasicBooleanMatrix(4, 3, value1);
        }catch (Exception e){
            assertEquals("input list of arrays does not have 3 columns", e.getMessage());
        }

        List<byte[]> value2 = new ArrayList<>();
        value2.add(null);
        value2.add(new byte[]{1,2,3});

        try {
            BasicBooleanMatrix bm = new BasicBooleanMatrix(3, 2, value2);
        }catch (Exception e){
            assertEquals("The length of array 1 doesn't have 3 elements", e.getMessage());
        }
        BasicBooleanMatrix bm;
        try {
            bm = new BasicBooleanMatrix(4, 4, value1);
            assertTrue(bm.getBoolean(1, 1));
            assertEquals(new BasicBoolean(true).getBoolean(), ((BasicBoolean)bm.get(1, 1)).getBoolean());
            assertFalse(bm.isNull(1, 1));
            bm.setNull(1, 2);
            assertTrue(bm.isNull(1, 2));
            assertEquals(Entity.DATA_CATEGORY.LOGICAL, bm.getDataCategory());
            bm.getClass();
        }catch (Exception e){
        }
    }
    @Test
    public void test_booleanValue() throws Exception {
        BasicBoolean bb = new BasicBoolean(Byte.MIN_VALUE);
        assertEquals(null,bb.booleanValue());
        BasicBoolean bb1 = new BasicBoolean(true);
        assertEquals(true,bb1.booleanValue());
        BasicBoolean bb2 = new BasicBoolean(false);
        assertEquals(false,bb2.booleanValue());
    }
    @Test
    public void test_BasicBoolean_toJsonString() throws Exception {
        BasicBoolean bb1 = new BasicBoolean(true);
        String re = JSONObject.toJSONString(bb1);
        System.out.println(re);
        assertEquals("{\"boolean\":true,\"chart\":false,\"chunk\":false,\"dataCategory\":\"LOGICAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_BOOL\",\"dictionary\":false,\"jsonString\":\"true\",\"matrix\":false,\"number\":1,\"pair\":false,\"scalar\":true,\"string\":\"true\",\"table\":false,\"value\":1,\"vector\":false}", re);
    }
    @Test
    public void test_BasicBoolean_toJsonString_null() throws Exception {
        BasicBoolean bb = new BasicBoolean(Byte.MIN_VALUE);
        String re = JSONObject.toJSONString(bb);
        System.out.println(re);
        assertEquals("{\"boolean\":true,\"chart\":false,\"chunk\":false,\"dataCategory\":\"LOGICAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_BOOL\",\"dictionary\":false,\"jsonString\":\"null\",\"matrix\":false,\"number\":-128,\"pair\":false,\"scalar\":true,\"string\":\"\",\"table\":false,\"value\":-128,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicBooleanMartix_getScale() throws Exception {
        byte [] a={1,2,3};
        byte [] b={1,2,3};
        BasicBooleanMatrix bm=new BasicBooleanMatrix(3,2,Arrays.asList(a,b));
        bm.getScale();

    }
}
