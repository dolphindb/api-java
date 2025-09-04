package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicDateVectorTest {

    @Test
    public void test_BasicDateVector() throws Exception{
        int[] array = new int[]{2861,7963,4565,2467,Integer.MIN_VALUE};
        BasicDateVector btv = new BasicDateVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("1976-10-03",btv.getDate(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[1977.11.01,1982.07.02,1991.10.21]",btv.getSubVector(indices).getString());
        btv.setDate(2, LocalDate.of(1984,7,14));
        assertEquals("1984-07-14",btv.getDate(2).toString());
        assertNull(btv.getDate(4));
        assertEquals(BasicDate.class,btv.getElementClass());
        assertEquals(new BasicDate(LocalDate.of(1976,10,3)),btv.get(3));
        BasicDateVector bdhv = new BasicDateVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("1970-01-01",bdhv.getDate(0).toString());
        List<Integer> list = Arrays.asList(2861,7963,4565,2467,Integer.MIN_VALUE);
        BasicDateVector bdhv2 = new BasicDateVector(list);
        assertEquals("1991-10-21",bdhv2.getDate(1).toString());
        ByteBuffer bb = ByteBuffer.allocate(10000);
        bdhv2.writeVectorToBuffer(bb);
    }

    @Test
    public void test_BasicDateVector_capacity_lt_size() throws Exception {
        BasicDateVector bbv = new BasicDateVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, 1);
        bbv.set(3, -1);
        bbv.set(4, 2);
        bbv.set(5, 10000);
        Assert.assertEquals("[,,1970.01.02,1969.12.31,1970.01.03,1997.05.19]", bbv.getString());
    }

    @Test
    public void test_BasicDateVector_size_capacity_set() throws Exception {
        BasicDateVector bbv = new BasicDateVector(6,6);
        Assert.assertEquals("[1970.01.01,1970.01.01,1970.01.01,1970.01.01,1970.01.01,1970.01.01]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, 1);
        bbv.set(3, -1);
        bbv.set(4, 2);
        bbv.set(5, 10000);
        Assert.assertEquals("[,,1970.01.02,1969.12.31,1970.01.03,1997.05.19]", bbv.getString());
    }

    @Test
    public void test_BasicDateVector_size_capacity_add() throws Exception {
        BasicDateVector bbv = new BasicDateVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(1);
        bbv.add(-1);
        bbv.add(2);
        bbv.add(10000);
        Assert.assertEquals("[,,1970.01.02,1969.12.31,1970.01.03,1997.05.19]", bbv.getString());
    }

    @Test
    public void test_BasicDateVector_set_type_not_match() throws Exception {
        BasicDateVector bbv = new BasicDateVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDate, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateVector_add_type_not_match() throws Exception {
        BasicDateVector bbv = new BasicDateVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDate, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateVector_Append() throws Exception {
        BasicDateVector bdv = new BasicDateVector(new int[]{25,220,280});
        int size = bdv.size;
        int capacity = bdv.capacity;
        bdv.Append(new BasicDate(317));
        assertEquals(capacity*2,bdv.capacity);
        bdv.Append(new BasicDateVector(new int[]{420,587,618}));
        assertEquals(capacity*2+3,bdv.capacity);
        assertEquals(size+4,bdv.size);
        System.out.println(bdv.getString());
    }

    @Test
    public void testDateCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateVector v = new BasicDateVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateVector v1 = new BasicDateVector(list1);
        BasicDateVector v2 = (BasicDateVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateVector v3 = new BasicDateVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicDateVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateVector v = new BasicDateVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDate\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.02,1970.01.03,1970.01.04]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
