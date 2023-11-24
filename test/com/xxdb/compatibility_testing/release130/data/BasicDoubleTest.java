package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;
import com.xxdb.data.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BasicDoubleTest {

    @Test
    public void TestCombineDoubleVector() throws Exception {
        Double[] data = {1.1,0.0,1.3};
        BasicDoubleVector v = new BasicDoubleVector(Arrays.asList(data));
        Double[] data2 = {1.1,0.0,1.3};
        BasicDoubleVector vector2 = new BasicDoubleVector( Arrays.asList(data2));
        BasicDoubleVector res= (BasicDoubleVector) v.combine(vector2);
        Double[] datas = {1.1,0.0,1.3,1.1,0.0,1.3};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(6,res.rows());
    }

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
    public void test_BasicDoubleVector_list(){
        List<Double> list = new ArrayList<>();
        list.add(8.14);
        list.add(8.13);
        list.add(null);
        list.add(8.18);
        BasicDoubleVector bdv = new BasicDoubleVector(list);
        assertTrue(bdv.isNull(2));
        assertEquals(new BasicDouble(8.18),bdv.get(3));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bdv.getDataCategory());
        assertEquals(BasicDouble.class,bdv.getElementClass());
        assertEquals("[8.13,8.18,8.14]",bdv.getSubVector(new int[]{1,3,0}).getString());
    }

    @Test
    public void test_BasicDoubleVector_array() throws IOException {
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        ByteBuffer bb = bdv.writeVectorToBuffer(ByteBuffer.allocate(32));
        assertEquals("[64, 24, -52, -52, -52, -52, -52, -51, 64, 25, 30, " +
                "-72, 81, -21, -123, 31, 64, 29, 10, 61, 112, -93, -41, 10, 0, 0, " +
                "0, 0, 0, 0, 0, 0]",Arrays.toString(bb.array()));
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicDoubleVector_asof(){
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        assertEquals(-1,bdv.asof(new BasicDouble(5.7)));
        assertEquals(0,bdv.asof(new BasicDouble(6.25)));
        bdv.asof(new BasicComplex(2.1,4.2));
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
    public void test_BasicDoubleVector_Append() throws Exception {
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{0.61,0.32,0.77});
        int size = bdv.rows();
        bdv.Append(new BasicDouble(1.243));
        assertEquals(size+1,bdv.rows());
        assertEquals("1.243",bdv.get(3).getString());
        bdv.Append(new BasicDoubleVector(new double[]{2.33,2.798}));
        assertEquals(size+3,bdv.rows());
        assertEquals(2.798,bdv.getDouble(5),0);
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
    public void test_BasicDoubleVector_toJSONString() throws Exception {
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        String re = JSONObject.toJSONString(bdv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[6.2,6.28,7.26],\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DOUBLE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDouble\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[6.2,6.28,7.26]\",\"table\":false,\"unitLength\":8,\"vector\":true}", re);
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
}
