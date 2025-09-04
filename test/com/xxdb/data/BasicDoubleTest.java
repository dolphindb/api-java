package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BasicDoubleTest {

    @Test
    public void testBasicDoubleMatrix_function(){
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(2,2);
        bdm.setDouble(0,0,2.4);
        bdm.setDouble(0,1,5.2);
        bdm.setDouble(1,0,2.7);
        bdm.setDouble(1,1,3.8);
        assertEquals(3.8,bdm.getDouble(1,1),0);
        assertFalse(bdm.isNull(1,0));
        bdm.setNull(1,0);
        assertTrue(bdm.isNull(1,0));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bdm.getDataCategory());
        assertEquals(BasicDouble.class,bdm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicDoubleMatrix_listnull() throws Exception {
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(5,5,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicDoubleMatrix_error() throws Exception {
        List<double[]> list = new ArrayList<>();
        list.add(new double[]{8.2,8.5});
        list.add(new double[]{7.1,7.16});
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(3,2,list);
    }

    @Test
    public void test_BasicDouble_get() throws Exception {
        BasicDouble bd = new BasicDouble(5.73);
        assertFalse(bd.equals(new BasicBoolean(true)));
        assertNotNull(new BasicDouble(-Double.MAX_VALUE).getNumber());
        assertEquals("0.000001",new BasicDouble(0.000001).getString());
        assertEquals("1000000",new BasicDouble(1000000.0).getString());
        assertEquals("1000000",new BasicDouble(1000000.0).getJsonString());
        assertEquals("Infinity",new BasicDouble(Double.POSITIVE_INFINITY).getString());
    }

    @Test
    public void test_doubleValue() throws Exception {
        BasicDouble bb = new BasicDouble(1.243);
        bb.setNull();
        assertEquals(null,bb.doubleValue());
        BasicDouble bb1 = new BasicDouble(1.243);
        assertEquals("1.243",bb1.doubleValue().toString());
    }
    @Test
    public void test_BasicDouble_toJSONString() throws Exception {
        BasicDouble bd = new BasicDouble(5.73);
        String re = JSONObject.toJSONString(bd);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DOUBLE\",\"dictionary\":false,\"double\":5.73,\"jsonString\":\"5.73\",\"matrix\":false,\"null\":false,\"number\":5.73,\"pair\":false,\"scalar\":true,\"string\":\"5.73\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void testBasicDoubleMatrix_toJSONString(){
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(2,2);
        bdm.setDouble(0,0,2.4);
        bdm.setDouble(0,1,5.2);
        bdm.setDouble(1,0,2.7);
        bdm.setDouble(1,1,3.8);
        String re = JSONObject.toJSONString(bdm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_DOUBLE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDouble\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0  #1 \\n2.4 5.2\\n2.7 3.8\\n\",\"table\":false,\"vector\":false}", re);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicDoubleMatrix_getScale() throws Exception {
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(2,2);
        bdm.setDouble(0,0,2.4);
        bdm.setDouble(0,1,5.2);
        bdm.setDouble(1,0,2.7);
        bdm.setDouble(1,1,3.8);
        bdm.getScale();
    }
}
