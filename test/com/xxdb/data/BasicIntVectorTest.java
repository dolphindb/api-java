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

public class BasicIntVectorTest {

    @Test
    public void test_BasicIntVector(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        list.add(null);
        BasicIntVector biv = new BasicIntVector(list);
        assertEquals(BasicInt.class,biv.getElementClass());
        assertEquals(new BasicInt(7),biv.get(1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,biv.getDataCategory());
        assertEquals("[5,7,8,,,5,7,8]",biv.getSubVector(new int[]{0,1,2,3,4,0,1,2}).getString());
    }

    @Test
    public void test_BasicIntVector_capacity_lt_size() throws Exception {
        BasicIntVector bbv = new BasicIntVector(6,0);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, -1);
        bbv.set(3, 1);
        bbv.set(4, Integer.MIN_VALUE);
        bbv.set(5,  -Integer.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-2147483647]", bbv.getString());
    }

    @Test
    public void test_BasicIntVector_size_capacity_set() throws Exception {
        BasicIntVector bbv = new BasicIntVector(6,6);
        Assert.assertEquals("[0,0,0,0,0,0]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, -1);
        bbv.set(3, 1);
        bbv.set(4, Integer.MIN_VALUE);
        bbv.set(5,  -Integer.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-2147483647]", bbv.getString());
    }

    @Test
    public void test_BasicIntVector_size_capacity_add() throws Exception {
        BasicIntVector bbv = new BasicIntVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(-1);
        bbv.add(1);
        bbv.add(Integer.MIN_VALUE);
        bbv.add(-Integer.MAX_VALUE);
        Assert.assertEquals("[,,-1,1,,-2147483647]", bbv.getString());
    }

    @Test
    public void test_BasicIntVector_set_type_not_match() throws Exception {
        BasicIntVector bbv = new BasicIntVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only Integer or null is supported.", re);
    }

    @Test
    public void test_BasicIntVector_add_type_not_match() throws Exception {
        BasicIntVector bbv = new BasicIntVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only Integer or null is supported.", re);
    }

    @Test
    public void test_BasicIntVector_wvtb() throws IOException {
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        ByteBuffer bb = biv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals("[0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 8, -128, 0, 0, 0]", Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicIntVector_asof_error(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        biv.asof(new BasicComplex(1.9,8.5));
    }

    @Test
    public void test_BasicIntVector_asof_normal(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        assertEquals(3,biv.asof(new BasicInt(12)));
        assertEquals(0,biv.asof(new BasicInt(6)));
    }

    @Test
    public void test_BasicIntVector_Append() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{5,11,23});
        int size = biv.size;
        int capacity = biv.capacity;
        biv.Append(new BasicInt(12));
        assertEquals(size+1,biv.rows());
        assertEquals(capacity*2,biv.capacity);
        biv.Append(new BasicIntVector(new int[]{40,21,33}));
        assertEquals(size+4,biv.size);
        assertEquals(capacity*2+3,biv.capacity);
    }

    @Test
    public void TestCombineIntVector() throws Exception {
        int[] data = {4, 5, 3, 6};
        BasicIntVector v = new BasicIntVector(data );
        int[] data2 = { 2, 5, 1};
        BasicIntVector vector2 = new BasicIntVector(data2 );
        BasicIntVector res= (BasicIntVector) v.combine(vector2);
        int[] datas = {4, 5, 3, 6, 2, 5, 1};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());
        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicIntVector_toJSONString() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{5,11,23});
        String re = JSONObject.toJSONString(biv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[5,11,23],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicInt\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[5,11,23]\",\"table\":false,\"unitLength\":4,\"values\":[5,11,23],\"vector\":true}", re);
    }

}
