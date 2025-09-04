package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicSecondVectorTest {

    @Test
    public void test_BasicSecondVector(){
        BasicSecondVector bsv = new BasicSecondVector(4);
        bsv.setSecond(0, LocalTime.of(16,19,53));
        bsv.setSecond(1,LocalTime.of(11,11,11));
        bsv.setSecond(2,LocalTime.of(7,55,18));
        bsv.setSecond(3,LocalTime.of(23,57,55));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bsv.getDataCategory());
        assertEquals(BasicSecond.class,bsv.getElementClass());
        BasicSecondVector bsv2 = new BasicSecondVector(Entity.DATA_FORM.DF_VECTOR,4);
        bsv2 = bsv;
        assertEquals("23:57:55",bsv2.getSecond(3).toString());
        assertEquals("23:57:55",bsv2.get(3).toString());
        int[] array = new int[]{28800,43215,21630,54845,Integer.MIN_VALUE};
        BasicSecondVector bsv3 = new BasicSecondVector(array,true);
        assertEquals("[08:00:00,06:00:30,12:00:15]",bsv3.getSubVector(new int[]{0,2,1}).getString());
        assertNull(bsv3.getSecond(4));
    }

    @Test
    public void test_BasicSecondVector_capacity_lt_size() throws Exception {
        BasicSecondVector bbv = new BasicSecondVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalTime.of(14,11,25));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);

        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,14:11:25,14:30:00,00:00:01,00:00:00]", bbv.getString());
    }

    @Test
    public void test_BasicSecondVector_size_capacity_set() throws Exception {
        BasicSecondVector bbv = new BasicSecondVector(6,6);
        Assert.assertEquals("[00:00:00,00:00:00,00:00:00,00:00:00,00:00:00,00:00:00]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalTime.of(14,11,25));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);

        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,14:11:25,14:30:00,00:00:01,00:00:00]", bbv.getString());
    }

    @Test
    public void test_BasicSecondVector_size_capacity_add() throws Exception {
        BasicSecondVector bbv = new BasicSecondVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add( null);
        bbv.add(LocalTime.of(14,11,25));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);

        bbv.add(calendar);
        bbv.add(1);
        bbv.add(0);
        Assert.assertEquals("[,,14:11:25,14:30:00,00:00:01,00:00:00]", bbv.getString());
    }

    @Test
    public void test_BasicSecondVector_set_type_not_match() throws Exception {
        BasicSecondVector bbv = new BasicSecondVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicSecondVector_add_type_not_match() throws Exception {
        BasicSecondVector bbv = new BasicSecondVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicSecondVector_Append() throws Exception {
        BasicSecondVector bsv = new BasicSecondVector(new int[]{12,34,56});
        int size = bsv.size;
        int capacity = bsv.capacity;
        bsv.Append(new BasicSecond(LocalTime.now()));
        assertEquals(size+1,bsv.size);
        assertEquals(capacity*2,bsv.capacity);
        bsv.Append(bsv);
        assertEquals(size+5,bsv.size);
        assertEquals(9,bsv.capacity);
    }

    @Test
    public void testSecondCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicSecondVector v = new BasicSecondVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicSecondVector v1 = new BasicSecondVector(list1);
        BasicSecondVector v2 = (BasicSecondVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicSecondVector v3 = new BasicSecondVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicSecondVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicSecondVector v = new BasicSecondVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_SECOND\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicSecond\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:01,00:00:02,00:00:03]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
