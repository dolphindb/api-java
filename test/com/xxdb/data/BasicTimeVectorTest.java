package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicTimeVectorTest {

    @Test
    public void test_BasicTimeVector(){
        int[] array = new int[]{53000,75000,115000,145000,Integer.MIN_VALUE};
        BasicTimeVector btv = new BasicTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("00:02:25",btv.getTime(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[00:00:53.000,00:01:55.000,00:01:15.000]",btv.getSubVector(indices).getString());
        btv.setTime(2, LocalTime.of(1,25,42));
        assertEquals("01:25:42",btv.getTime(2).toString());
        assertEquals(BasicTime.class,btv.getElementClass());
        BasicTimeVector btv2 = new BasicTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        assertNull(btv.getTime(4));
        BasicTimeVector btv3 = new BasicTimeVector(1);
        assertEquals("00:00",btv3.getTime(0).toString());
        assertEquals(new BasicTime(53000),btv.get(0));
    }

    @Test(expected = NullPointerException.class)
    public void test_BasicTimeVector_LocalTime_null(){
        new BasicTimeVector((LocalTime[]) null);
    }

    @Test(expected = NullPointerException.class)
    public void test_BasicTimeVector_Calendar_null(){
        new BasicTimeVector((Calendar[]) null);
    }

    @Test
    public void test_BasicTimeVector_LocalTime() throws Exception{
        LocalTime current = LocalTime.now();
        BasicTimeVector v = new BasicTimeVector(new LocalTime[]{
                LocalTime.of(0, 0, 1, 1),
                LocalTime.of(23, 59, 59, 999999999),
                current
        });
        assertEquals(3, v.rows());
        assertEquals("00:00:00.000", v.getTime(0).toString());
        assertEquals("23:59:59.999", v.getTime(1).toString());
        assertEquals(current.withNano((current.getNano()/1000000)*1000000), v.getTime(2));
    }

    @Test
    public void test_BasicTimeVector_Calendar() throws Exception{
        Calendar calendar = Calendar.getInstance();
        calendar.set(1969, Calendar.DECEMBER, 31, 0, 0, 1);
        calendar.set(Calendar.MILLISECOND, 1);
        Calendar calendar2 = Calendar.getInstance();
        calendar2.set(2025, Calendar.JANUARY, 1, 23, 59, 59);
        calendar2.set(Calendar.MILLISECOND, 999);
        Calendar calendar3 = Calendar.getInstance();
        calendar3.set(2040, Calendar.JANUARY, 1, 12, 0, 1);
        calendar3.set(Calendar.MILLISECOND, 0);
        BasicTimeVector v = new BasicTimeVector(new Calendar[]{calendar, calendar2, calendar3});
        assertEquals(3, v.rows());
        assertEquals("00:00:01.001", v.getTime(0).toString());
        assertEquals("23:59:59.999", v.getTime(1).toString());
        assertEquals("12:00:01.000", v.getTime(2).toString());
    }

    @Test
    public void test_BasicTimeVector_capacity_lt_size() throws Exception {
        BasicTimeVector bbv = new BasicTimeVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalTime.of(01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,01:11:20.999,14:30:00.000,00:00:00.001,00:00:00.000]", bbv.getString());
    }

    @Test
    public void test_BasicTimeVector_size_capacity_set() throws Exception {
        BasicTimeVector bbv = new BasicTimeVector(6,6);
        Assert.assertEquals("[00:00:00.000,00:00:00.000,00:00:00.000,00:00:00.000,00:00:00.000,00:00:00.000]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalTime.of(01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,01:11:20.999,14:30:00.000,00:00:00.001,00:00:00.000]", bbv.getString());
    }

    @Test
    public void test_BasicTimeVector_size_capacity_add() throws Exception {
        BasicTimeVector bbv = new BasicTimeVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(LocalTime.of(01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.add(calendar);
        bbv.add(1);
        bbv.add(0);
        Assert.assertEquals("[,,01:11:20.999,14:30:00.000,00:00:00.001,00:00:00.000]", bbv.getString());
    }

    @Test
    public void test_BasicTimeVector_set_type_not_match() throws Exception {
        BasicTimeVector bbv = new BasicTimeVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicTimeVector_add_type_not_match() throws Exception {
        BasicTimeVector bbv = new BasicTimeVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicTimeVector_Append() throws Exception {
        BasicTimeVector btv = new BasicTimeVector(new int[]{1893,1976});
        int size = btv.size;
        int capacity = btv.capacity;
        btv.Append(new BasicTime(2022));
        assertEquals(size+1,btv.size);
        assertEquals(capacity*2,btv.capacity);
        btv.Append(new BasicTimeVector(new int[]{618,755,907}));
        assertEquals(size+4,btv.size);
        assertEquals(capacity*2+2,btv.capacity);
    }

    @Test
    public void testTimeCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicTimeVector v = new BasicTimeVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicTimeVector v1 = new BasicTimeVector(list1);
        BasicTimeVector v2 = (BasicTimeVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicTimeVector v3 = new BasicTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicTimeVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicTimeVector v = new BasicTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_TIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:00.001,00:00:00.002,00:00:00.003]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
