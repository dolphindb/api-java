package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicTimeStampVectorTest {

    @Test
    public void test_BasicTimeStampVector(){
        BasicTimestampVector btsv = new BasicTimestampVector(Entity.DATA_FORM.DF_VECTOR,5);
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicTimestampVector btv = new BasicTimestampVector(array,true);
        btsv = btv;
        assertEquals(BasicTimestamp.class,btsv.getElementClass());
        assertNull(btsv.getTimestamp(4));
        assertEquals("2733-11-07T14:06:30",btsv.getTimestamp(2).toString());
        assertEquals("2733.11.07T14:06:30.000",btsv.get(2).toString());
    }

    @Test
    public void test_BasicTimestampVector_capacity_lt_size() throws Exception {
        BasicTimestampVector bbv = new BasicTimestampVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.set(3, calendar);
        bbv.set(4, (long)1);
        bbv.set(5, (long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999,2023.11.15T14:30:00.000,1970.01.01T00:00:00.001,1970.01.01T00:00:00.000]", bbv.getString());
    }

    @Test
    public void test_BasicTimestampVector_size_capacity_set() throws Exception {
        BasicTimestampVector bbv = new BasicTimestampVector(6,6);
        Assert.assertEquals("[1970.01.01T00:00:00.000,1970.01.01T00:00:00.000,1970.01.01T00:00:00.000,1970.01.01T00:00:00.000,1970.01.01T00:00:00.000,1970.01.01T00:00:00.000]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.set(3, calendar);
        bbv.set(4, (long)1);
        bbv.set(5, (long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999,2023.11.15T14:30:00.000,1970.01.01T00:00:00.001,1970.01.01T00:00:00.000]", bbv.getString());
    }


    @Test
    public void test_BasicTimestampVector_size_capacity_add() throws Exception {
        BasicTimestampVector bbv = new BasicTimestampVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(LocalDateTime.of(2021,01,11,01,11,20,999999999));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        calendar.set(Calendar.HOUR_OF_DAY, 14);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        bbv.add(calendar);
        bbv.add((long)1);
        bbv.add((long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999,2023.11.15T14:30:00.000,1970.01.01T00:00:00.001,1970.01.01T00:00:00.000]", bbv.getString());
    }

    @Test
    public void test_BasicTimestampVector_set_type_not_match() throws Exception {
        BasicTimestampVector bbv = new BasicTimestampVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only LocalDateTime, Calendar, Long or null is supported.", re);
    }

    @Test
    public void test_BasicTimestampVector_add_type_not_match() throws Exception {
        BasicTimestampVector bbv = new BasicTimestampVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Calendar, Long or null is supported.", re);
    }

    @Test
    public void test_BasicTimeStampVector_Append() throws Exception {
        BasicTimestampVector btsv = new BasicTimestampVector(new long[]{34724264,7472947292L,3742839,3473293});
        int size = btsv.size;
        int capacity = btsv.capacity;
        btsv.Append(new BasicTimestamp(LocalDateTime.now()));
        assertEquals(capacity*2,btsv.capacity);
        btsv.Append(btsv);
        System.out.println(btsv.getString());
        assertEquals(size+6,btsv.size);
    }

    @Test
    public void testTimeStampCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicTimestampVector v = new BasicTimestampVector(list);
        List<Long> list1 = Arrays.asList(3l,2l,1l);
        BasicTimestampVector v1 = new BasicTimestampVector(list1);
        BasicTimestampVector v2 = (BasicTimestampVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,3l,2l,1l);
        BasicTimestampVector v3 = new BasicTimestampVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicTimestampVector_toJSONString() throws Exception {
        List<Long> list = Arrays.asList(1L,2L,3L);
        BasicTimestampVector v = new BasicTimestampVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_TIMESTAMP\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicTimestamp\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002,1970.01.01T00:00:00.003]\",\"table\":false,\"unitLength\":16,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
