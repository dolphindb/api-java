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

public class BasicShortVectorTest {

    @Test
    public void test_BasicShortVector_basicFunctions(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add(null);
        list.add((short) 4);
        BasicShortVector bsv = new BasicShortVector(list);

        assertEquals("[4,2,7]",bsv.getSubVector(new int[]{3,0,1}).getString());
        assertTrue(bsv.isNull(2));
        assertEquals((short)7,bsv.getShort(1));
        assertEquals(BasicShort.class,bsv.getElementClass());
    }

    @Test
    public void test_BasicShortVector_capacity_lt_size() throws Exception {
        BasicShortVector bbv = new BasicShortVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (short)-1);
        bbv.set(3, (short)1);
        bbv.set(4, Short.MIN_VALUE);
        bbv.set(5,  Short.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,32767]", bbv.getString());
    }

    @Test
    public void test_BasicShortVector_size_capacity_set() throws Exception {
        BasicShortVector bbv = new BasicShortVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (short)-1);
        bbv.set(3, (short)1);
        bbv.set(4, Short.MIN_VALUE);
        bbv.set(5,  Short.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,32767]", bbv.getString());
    }

    @Test
    public void test_BasicShortVector_size_capacity_add() throws Exception {
        BasicShortVector bbv = new BasicShortVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add((short)-1);
        bbv.add((short)1);
        bbv.add(Short.MIN_VALUE);
        bbv.add(Short.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,32767]", bbv.getString());
    }

    @Test
    public void test_BasicShortVector_set_type_not_match() throws Exception {
        BasicShortVector bbv = new BasicShortVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Short or null is supported.", re);
    }

    @Test
    public void test_BasicShortVector_add_type_not_match() throws Exception {
        BasicShortVector bbv = new BasicShortVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Short or null is supported.", re);
    }

    @Test
    public void test_BasicShortVector_wvtb() throws IOException {
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 4);
        BasicShortVector bsv = new BasicShortVector(list);
        ByteBuffer bb = bsv.writeVectorToBuffer(ByteBuffer.allocate(8));
        assertEquals("[0, 2, 0, 7, 0, 16, 0, 4]",Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicShortVector_asof_error(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 24);
        BasicShortVector bsv = new BasicShortVector(list);
        bsv.asof(new BasicPoint(5.9,7.6));
    }

    @Test
    public void test_BasicShortVector_asof(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 24);
        BasicShortVector bsv = new BasicShortVector(list);
        assertEquals(2,bsv.asof(new BasicShort((short) 20)));
    }

    @Test
    public void test_BasicShortVector_Append() throws Exception {
        BasicShortVector bsv = new BasicShortVector(new short[]{55,11,46});
        int size = bsv.rows();
        bsv.Append(new BasicShort((short) 88));
        assertEquals(size+1,bsv.rows());
        bsv.Append(new BasicShortVector(new short[]{16,23,11}));
        assertEquals(size+4,bsv.rows());
    }
    @Test
    public void test_BasicShortVector_setNull() throws Exception {
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        BasicShortVector bsv = new BasicShortVector(list);
        bsv.setNull(0);
        assertEquals(true,bsv.isNull(0));
        //List<Short> list1 = null;
        //BasicShortVector bsv1 = new BasicShortVector(list1);
    }

    @Test
    public void TestCombineShortVector() throws Exception {
        Short[] data = {1,-1,3};
        BasicShortVector v = new BasicShortVector(Arrays.asList(data));
        Short[] data2 = {1,-1,3,9};
        BasicShortVector vector2 = new BasicShortVector( Arrays.asList(data2));
        BasicShortVector res= (BasicShortVector) v.combine(vector2);
        Short[] datas = {1,-1,3,1,-1,3,9};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());
        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicShortVector_toJSONString() throws Exception {
        BasicShortVector bsv = new BasicShortVector(new short[]{55,11,46});
        String re = JSONObject.toJSONString(bsv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[55,11,46],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicShort\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[55,11,46]\",\"table\":false,\"unitLength\":2,\"values\":[55,11,46],\"vector\":true}", re);
    }
}
