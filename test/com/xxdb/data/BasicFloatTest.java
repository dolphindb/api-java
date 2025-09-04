package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.*;

public class BasicFloatTest {

    @Test
    public void test_BasicFloat() throws Exception {
        BasicFloat bf = new BasicFloat(-Float.MAX_VALUE);
        assertNotNull(bf.getNumber());
        BasicFloat bf2 = new BasicFloat(Float.NEGATIVE_INFINITY);
        assertEquals(String.valueOf(Float.NEGATIVE_INFINITY),bf2.getString());
        assertFalse(bf.equals(new GregorianCalendar()));
        assertTrue(bf.equals(bf));
        assertFalse(bf.equals(null));
        BasicFloat bf3 = new BasicFloat((float) 0.000001);
        assertEquals("0.000001",bf3.getString());
        assertEquals("0.000001",bf3.getJsonString());
        BasicFloat bf1 = new BasicFloat(1);
        assertFalse(bf.equals(bf1));
    }

    @Test
    public void test_BasicFloatMatrix() throws Exception {
        BasicFloatMatrix bfm = new BasicFloatMatrix(3,1);
        bfm.setFloat(0,0, (float) 5.72);
        bfm.setFloat(1,0, (float) 6.32);
        bfm.setFloat(2,0, (float) 4.08);
        assertEquals((float)6.32,bfm.getFloat(1,0),0);
        assertFalse(bfm.isNull(0,0));
        assertEquals((float)5.72,bfm.get(0,0).getNumber());
        bfm.setNull(1,0);
        assertTrue(bfm.isNull(1,0));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bfm.getDataCategory());
        assertEquals(BasicFloat.class,bfm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicFloatMatrix_listnull() throws Exception {
        BasicFloatMatrix bfm = new BasicFloatMatrix(1,1,null);
    }

    @Test(expected = Exception.class)
    public void  test_BasicFloatMatrix_arraynull() throws Exception {
        List<float[]> list = new ArrayList<>();
        list.add(new float[]{(float) 7.89, (float) 7.98});
        list.add(null);
        BasicFloatMatrix bfm = new BasicFloatMatrix(2,2,list);
    }

    @Test
    public void test_floatValue() throws Exception {
        BasicFloat bb = new BasicFloat((float) 9.705);
        bb.setNull();
        assertEquals(null,bb.floatValue());
        BasicFloat bb1 = new BasicFloat((float) 9.705);
        assertEquals("9.705",bb1.floatValue().toString());
    }
    @Test
    public void test_BasicFloat_toJSONString() throws Exception {
        BasicFloat bf = new BasicFloat((float) 0.000001);
        String re = JSONObject.toJSONString(bf);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_FLOAT\",\"dictionary\":false,\"float\":1.0E-6,\"jsonString\":\"0.000001\",\"matrix\":false,\"null\":false,\"number\":1.0E-6,\"pair\":false,\"scalar\":true,\"string\":\"0.000001\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void test_BasicFloatMatrix_toJSONString() throws Exception {
        BasicFloatMatrix bfm = new BasicFloatMatrix(3,1);
        String re = JSONObject.toJSONString(bfm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_FLOAT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicFloat\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0\\n0 \\n0 \\n0 \\n\",\"table\":false,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicFloatMatrix_getScale() throws Exception {
        BasicFloatMatrix bdm = new BasicFloatMatrix(2,2);
        bdm.setFloat(0,0, 2.4F);
        bdm.setFloat(0,1, 5.2F);
        bdm.setFloat(1,0, 2.7F);
        bdm.setFloat(1,1, 3.8F);
        bdm.getScale();
    }
}
