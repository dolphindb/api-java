package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicDoubleVectorTest {

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
    @Test
    public void test_BasicDoubleVector_capacity_lt_size() throws Exception {
        BasicDoubleVector bbv = new BasicDoubleVector(6,0);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, -1.33);
        bbv.set(3, 2.99);
        bbv.set(4, Double.MIN_VALUE);

        System.out.println(Double.MIN_VALUE);
        bbv.set(5, -Double.MAX_VALUE);
        Assert.assertEquals("[,,-1.33,2.99,4.9E-324,]", bbv.getString());
    }

    @Test
    public void test_BasicDoubleVector_size_capacity_set() throws Exception {
        BasicDoubleVector bbv = new BasicDoubleVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, -1.33);
        bbv.set(3, 2.99);
        bbv.set(4, 0.0);
        bbv.set(5, -Double.MAX_VALUE);
        Assert.assertEquals("[,,-1.33,2.99,0,]", bbv.getString());
    }

    @Test
    public void test_BasicDoubleVector_size_capacity_add() throws Exception {
        BasicDoubleVector bbv = new BasicDoubleVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(-1.33);
        bbv.add(2.99);
        bbv.add(0.0);
        bbv.add(-Double.MAX_VALUE);
        Assert.assertEquals("[,,-1.33,2.99,0,]", bbv.getString());
    }

    @Test
    public void test_BasicDoubleVector_set_type_not_match() throws Exception {
        BasicDoubleVector bbv = new BasicDoubleVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Double or null is supported.", re);
    }

    @Test
    public void test_BasicDoubleVector_add_type_not_match() throws Exception {
        BasicDoubleVector bbv = new BasicDoubleVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only Double or null is supported.", re);
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
    public void test_BasicDoubleVector_toJSONString() throws Exception {
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        String re = JSONObject.toJSONString(bdv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[6.2,6.28,7.26],\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DOUBLE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDouble\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[6.2,6.28,7.26]\",\"table\":false,\"unitLength\":8,\"values\":[6.2,6.28,7.26],\"vector\":true}", re);
    }
}
