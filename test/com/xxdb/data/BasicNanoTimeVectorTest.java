package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.*;

public class BasicNanoTimeVectorTest {
    @Test
    public void test_BasicNanoTimeVector(){
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimeVector btv = new BasicNanoTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("03:20:13.435579",btv.getNanoTime(3).toString());
        assertNull(btv.getNanoTime(4));
        int[] indices = new int[]{0,2,1};
        assertEquals("[06:34:01.343568000,06:41:44.786790000,06:39:55.876902000]",btv.getSubVector(indices).getString());
        btv.setNanoTime(2, LocalTime.of(22,12,17));
        assertEquals("22:12:17",btv.getNanoTime(2).toString());
        assertEquals("22:12:17.000000000",btv.get(2).toString());
        assertEquals(BasicNanoTime.class,btv.getElementClass());
        BasicNanoTimeVector bdhv = new BasicNanoTimeVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("00:00",bdhv.getNanoTime(0).toString());
        BasicNanoTimeVector bmv = new BasicNanoTimeVector(4);
        bmv = btv;
        assertEquals("22:12:17",bmv.getNanoTime(2).toString());
    }

    @Test
    public void test_BasicNanoTimeVector_capacity_lt_size() throws Exception {
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalTime.of(23,11,20));
        bbv.set(3,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.set(4, (long)1);
        bbv.set(5, (long)0);
        Assert.assertEquals("[,,23:11:20.000000000,01:11:20.999999999,00:00:00.000000001,00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimeVector_size_capacity_set() throws Exception {
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(6,6);
        Assert.assertEquals("[00:00:00.000000000,00:00:00.000000000,00:00:00.000000000,00:00:00.000000000,00:00:00.000000000,00:00:00.000000000]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalTime.of(23,11,20));
        bbv.set(3,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.set(4, (long)1);
        bbv.set(5, (long)0);
        Assert.assertEquals("[,,23:11:20.000000000,01:11:20.999999999,00:00:00.000000001,00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimeVector_size_capacity_add() throws Exception {
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(LocalTime.of(23,11,20));
        bbv.add( LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.add((long)1);
        bbv.add((long)0);
        Assert.assertEquals("[,,23:11:20.000000000,01:11:20.999999999,00:00:00.000000001,00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimeVector_set_type_not_match() throws Exception {
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only LocalTime, LocalDateTime, Long or null is supported.", re);
    }

    @Test
    public void test_BasicNanoTimeVector_add_type_not_match() throws Exception {
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, LocalDateTime, Long or null is supported.", re);
    }

    @Test
    public void test_BasicNanotimeVector_Append() throws Exception {
        BasicNanoTimeVector bntv = new BasicNanoTimeVector(new long[]{2314571,72668945,29934552});
        int size = bntv.size;
        int capacity = bntv.capacity;
        bntv.Append(new BasicNanoTime(899034671));
        bntv.Append(new BasicNanoTime(9012343536L));
        assertEquals(size+2,bntv.size);
        assertEquals(capacity*2,bntv.capacity);
        bntv.Append(new BasicNanoTimeVector(new long[]{65534,21485432,1798093345}));
        assertEquals(capacity*2+3,bntv.capacity);
        assertNotEquals(bntv.size,bntv.capacity);
    }

    @Test
    public void testNanoTimeCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicNanoTimeVector v = new BasicNanoTimeVector(list);
        List<Long> list1 = Arrays.asList(1l,2l,3l);
        BasicNanoTimeVector v1 = new BasicNanoTimeVector(list1);
        BasicNanoTimeVector v2 = (BasicNanoTimeVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,1l,2l,3l);
        BasicNanoTimeVector v3 = new BasicNanoTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicNanoTimeVector_toJSONString() throws Exception {
        List<Long> list = Arrays.asList(1L,2L,3L);
        BasicNanoTimeVector v = new BasicNanoTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_NANOTIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicNanoTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:00.000000001,00:00:00.000000002,00:00:00.000000003]\",\"table\":false,\"unitLength\":16,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
