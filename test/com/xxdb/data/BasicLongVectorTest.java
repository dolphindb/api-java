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

public class BasicLongVectorTest {
    @Test
    public void test_BasicLongVector(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(null);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        assertEquals("888",blv.get(3).getString());
        assertEquals("[888,865,855]",blv.getSubVector(new int[]{3,1,0}).getString());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,blv.getDataCategory());
        assertEquals(BasicLong.class,blv.getElementClass());
    }

    @Test
    public void test_BasicLongVector_capacity_lt_size() throws Exception {
        BasicLongVector bbv = new BasicLongVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (long)-1);
        bbv.set(3, (long)1);
        bbv.set(4, Long.MIN_VALUE);
        bbv.set(5,  -Long.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-9223372036854775807]", bbv.getString());
    }

    @Test
    public void test_BasicLongVector_size_capacity_set() throws Exception {
        BasicLongVector bbv = new BasicLongVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, (long)-1);
        bbv.set(3, (long)1);
        bbv.set(4, Long.MIN_VALUE);
        bbv.set(5,  -Long.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-9223372036854775807]", bbv.getString());
    }

    @Test
    public void test_BasicLongVector_size_capacity_add() throws Exception {
        BasicLongVector bbv = new BasicLongVector(0, 6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object) null);
        bbv.add(null);
        bbv.add((long)-1);
        bbv.add((long)1);
        bbv.add(Long.MIN_VALUE);
        bbv.add(-Long.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-9223372036854775807]", bbv.getString());
    }

    @Test
    public void test_BasicLongVector_set_type_not_match() throws Exception {
        BasicLongVector bbv = new BasicLongVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long or null is supported.", re);
    }

    @Test
    public void test_BasicLongVector_add_type_not_match() throws Exception {
        BasicLongVector bbv = new BasicLongVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Long or null is supported.", re);
    }

    @Test
    public void test_BasicLongVector_wvtb() throws IOException {
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        ByteBuffer bb = blv.writeVectorToBuffer(ByteBuffer.allocate(24));
        assertEquals("[0, 0, 0, 0, 0, 0, 3, 87, 0, 0, 0, 0, " +
                "0, 0, 3, 97, 0, 0, 0, 0, 0, 0, 3, 120]",Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicLongVector_asof_error(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        blv.asof(new BasicComplex(5.75,7.37));
    }

    @Test
    public void test_BasicLongVector_asof_normal(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        blv.asof(new BasicLong(860L));
    }

    @Test
    public void test_BasicLongVector_Append() throws Exception {
        BasicLongVector blv = new BasicLongVector(new long[]{600,615,617});
        int size = blv.size;
        int capacity = blv.capacity;
        blv.Append(new BasicLong(625));
        assertEquals(size+1,blv.size);
        assertEquals(capacity*2,blv.capacity);
        blv.Append(new BasicLongVector(new long[]{630,632,636}));
        assertEquals(size+4,blv.size);
        assertEquals(capacity*2+3,blv.capacity);
    }

    @Test
    public void TestCombineLongVector() throws Exception {
        Long[] data = {1l,-1l,3l};
        BasicLongVector v = new BasicLongVector(Arrays.asList(data));
        Long[] data2 = {1l,-1l,3l,9l};
        BasicLongVector vector2 = new BasicLongVector( Arrays.asList(data2));
        BasicLongVector res= (BasicLongVector) v.combine(vector2);
        Long[] datas = {1l,-1l,3l,1l,-1l,3l,9l};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicLongMatrix_toJSONString() throws Exception {
        BasicLongMatrix blm = new BasicLongMatrix(2,2);
        String re = JSONObject.toJSONString(blm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_LONG\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicLong\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1\\n0  0 \\n0  0 \\n\",\"table\":false,\"vector\":false}", re);
    }
}
