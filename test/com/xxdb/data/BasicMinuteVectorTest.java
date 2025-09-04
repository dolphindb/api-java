package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.*;

public class BasicMinuteVectorTest {
    @Test
    public void test_BasicMinuteVector(){
        int[] array = new int[]{286,796,456,246,Integer.MIN_VALUE};
        BasicMinuteVector btv = new BasicMinuteVector(array,true);
        assertNull(btv.getMinute(4));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("04:06",btv.getMinute(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[04:46m,07:36m,13:16m]",btv.getSubVector(indices).getString());
        btv.setMinute(2, LocalTime.of(14,11,25));
        assertEquals("14:11",btv.getMinute(2).toString());
        assertEquals(BasicMinute.class,btv.getElementClass());
        BasicMinuteVector bdhv = new BasicMinuteVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("00:00",bdhv.getMinute(0).toString());
        List<Integer> list = Arrays.asList(286,796,456,246,Integer.MIN_VALUE);
        BasicMinuteVector bdhv2 = new BasicMinuteVector(list);
        assertEquals("13:16",bdhv2.getMinute(1).toString());
        assertEquals("13:16m",bdhv2.get(1).getString());
        BasicMinuteVector bmv = new BasicMinuteVector(5);
        bmv = bdhv2;
        assertEquals("07:36",bmv.getMinute(2).toString());
    }

    @Test
    public void test_BasicMinuteVector_capacity_lt_size() throws Exception {
        BasicMinuteVector bbv = new BasicMinuteVector(6,1);
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
        bbv.set(5, -1);
        Assert.assertEquals("[,,14:11m,14:30m,00:01m,23:59m]", bbv.getString());
    }

    @Test
    public void test_BasicMinuteVector_size_capacity_set() throws Exception {
        BasicMinuteVector bbv = new BasicMinuteVector(6,6);
        Assert.assertEquals("[00:00m,00:00m,00:00m,00:00m,00:00m,00:00m]", bbv.getString());
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
        bbv.set(5, -1);
        Assert.assertEquals("[,,14:11m,14:30m,00:01m,23:59m]", bbv.getString());
    }

    @Test
    public void test_BasicMinuteVector_size_capacity_add() throws Exception {
        BasicMinuteVector bbv = new BasicMinuteVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(LocalTime.of(14,11,25));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);

        bbv.add(calendar);
        bbv.add(1);
        bbv.add(-1);
        Assert.assertEquals("[,,14:11m,14:30m,00:01m,23:59m]", bbv.getString());
    }

    @Test
    public void test_BasicMinuteVector_set_type_not_match() throws Exception {
        BasicMinuteVector bbv = new BasicMinuteVector(100,0);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicMinuteVector_add_type_not_match() throws Exception {
        BasicMinuteVector bbv = new BasicMinuteVector(0,0);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicMinuteVector_Append() throws Exception {
        BasicMinuteVector bmv = new BasicMinuteVector(new int[]{15,45});
        int size = bmv.size;
        int capacity = bmv.capacity;
        bmv.Append(new BasicMinute(LocalTime.now()));
        assertEquals(capacity*2,bmv.capacity);
        bmv.Append(new BasicMinuteVector(new int[]{78,32}));
        assertEquals(capacity*2+2,bmv.capacity);
        assertNotEquals(bmv.size,bmv.capacity);
        System.out.println(bmv.getString());
    }

    @Test
    public void testMinuteCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMinuteVector v = new BasicMinuteVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicMinuteVector v1 = new BasicMinuteVector(list1);
        BasicMinuteVector v2 = (BasicMinuteVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicMinuteVector v3 = new BasicMinuteVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicMinuteVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMinuteVector v = new BasicMinuteVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_MINUTE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicMinute\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:01m,00:02m,00:03m]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
