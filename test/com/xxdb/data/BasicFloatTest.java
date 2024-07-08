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
    public void TestCombineFloatVector() throws Exception {
        Float[] data = {1.1f,0.0f,1.3f};
        BasicFloatVector v = new BasicFloatVector(Arrays.asList(data));
        Float[] data2 = {1.1f,0.0f,1.3f};
        BasicFloatVector vector2 = new BasicFloatVector( Arrays.asList(data2));
        BasicFloatVector res= (BasicFloatVector) v.combine(vector2);
        Float[] datas = {1.1f,0.0f,1.3f,1.1f,0.0f,1.3f};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(6,res.rows());
    }
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
    public void test_BasicFloatVector(){
        List<Float> list = new ArrayList<>();
        list.add((float)0.79);
        list.add((float)8.85);
        list.add(null);
        list.add(null);
        BasicFloatVector bfv = new BasicFloatVector(list);
        assertEquals(-Float.MAX_VALUE,bfv.getFloat(2),0);
        assertTrue(bfv.isNull(2));
        assertFalse(bfv.isNull(1));
        assertEquals(bfv.get(0),new BasicFloat((float)0.79));
        bfv.setFloat(2, (float) 15.45);
        bfv.setNull(3);
        assertEquals("[15.44999981,8.85000038,,0.79000002]",bfv.getSubVector(new int[]{2,1,3,0}).getString());
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bfv.getDataCategory());
        assertEquals(BasicFloat.class,bfv.getElementClass());
    }

    @Test
    public void test_BasicFloatVector_wvtb() throws IOException {
        float[] arr = new float[]{(float) 77.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        ByteBuffer bb = bfv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals("[66, -102, -16, -92, 66, 81, -123, 31, 66, -124, -26, 102, 66, -113, -57, -82]",Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicFloatVector_asof(){
        float[] arr = new float[]{(float) 47.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        assertEquals(2,bfv.asof(new BasicFloat((float) 70.44)));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicFloatVector_asof_error(){
        float[] arr = new float[]{(float) 47.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        bfv.asof(new BasicComplex(3.4,5.8));
    }

    @Test
    public void test_BasicFloatVector_serialize() throws IOException {
        float[] arr = new float[]{(float) 47.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(3,1);
        assertEquals(16,bfv.serialize(0,0,4,numElementAndPartial,ByteBuffer.allocate(16)));
    }

    @Test
    public void test_BasicFloatVector_Append() throws Exception {
        BasicFloatVector bfv = new BasicFloatVector(new float[]{(float) 5.1, (float) 0.04, (float) 7.31});
        int size = bfv.rows();
        bfv.Append(new BasicFloat((float) 2.76));
        bfv.Append(new BasicFloat((float) 5.2334));
        assertEquals(size+2,bfv.rows());
        bfv.Append(new BasicFloatVector(new float[]{(float) 3.992, (float) 9.705, (float) 4.441}));
        assertEquals(size+5,bfv.rows());
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
    public void test_BasicFloatVector_toJSONString() throws Exception {
        float[] arr = new float[]{(float) 47.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        String re = JSONObject.toJSONString(bfv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[47.47,52.38,66.45,71.89],\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_FLOAT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicFloat\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[47.47000122,52.38000107,66.44999695,71.88999939]\",\"table\":false,\"unitLength\":4,\"values\":[47.47,52.38,66.45,71.89],\"vector\":true}", re);
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
