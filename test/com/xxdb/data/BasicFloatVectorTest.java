package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BasicFloatVectorTest {

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
    public void test_BasicFloatVector_capacity_lt_size() throws Exception {
        BasicFloatVector bbv = new BasicFloatVector(6,0);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (float)0.1);
        bbv.set(3, (float)-0.1);
        bbv.set(4, Float.MIN_VALUE);
        bbv.set(5, -Float.MAX_VALUE);
        Assert.assertEquals("[,,0.1,-0.1,1.4E-45,]", bbv.getString());
    }

    @Test
    public void test_BasicFloatVector_size_capacity_set() throws Exception {
        BasicFloatVector bbv = new BasicFloatVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (float)0.1);
        bbv.set(3, (float)-0.1);
        bbv.set(4, Float.MIN_VALUE);
        bbv.set(5, -Float.MAX_VALUE);
        Assert.assertEquals("[,,0.1,-0.1,1.4E-45,]", bbv.getString());
    }

    @Test
    public void test_BasicFloatVector_size_capacity_add() throws Exception {
        BasicFloatVector bbv = new BasicFloatVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add((float)0.1);
        bbv.add((float)-0.1);
        bbv.add(Float.MIN_VALUE);
        bbv.add(-Float.MAX_VALUE);
        Assert.assertEquals("[,,0.1,-0.1,1.4E-45,]", bbv.getString());
    }

    @Test
    public void test_BasicFloatVector_set_type_not_match() throws Exception {
        BasicFloatVector bbv = new BasicFloatVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Float or null is supported.", re);
    }

    @Test
    public void test_BasicFloatVector_add_type_not_match() throws Exception {
        BasicFloatVector bbv = new BasicFloatVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only Float or null is supported.", re);
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
    public void test_BasicFloatVector_toJSONString() throws Exception {
        float[] arr = new float[]{(float) 47.47, (float) 52.38, (float) 66.45, (float) 71.89};
        BasicFloatVector bfv = new BasicFloatVector(arr,false);
        String re = JSONObject.toJSONString(bfv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[47.47,52.38,66.45,71.89],\"dataCategory\":\"FLOATING\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_FLOAT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicFloat\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[47.47000122,52.38000107,66.44999695,71.88999939]\",\"table\":false,\"unitLength\":4,\"values\":[47.47,52.38,66.45,71.89],\"vector\":true}", re);
    }
}
