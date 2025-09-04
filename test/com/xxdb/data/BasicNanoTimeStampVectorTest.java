package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicNanoTimeStampVectorTest {

    @Test
    public void test_BasicNanoTimeStampVector(){
        BasicNanoTimestampVector bnts = new BasicNanoTimestampVector(Entity.DATA_FORM.DF_VECTOR,5);
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimestampVector btv = new BasicNanoTimestampVector(array,true);
        assertNull(btv.getNanoTimestamp(4));
        System.out.println(btv.getNanoTimestamp(4));
        bnts = btv;
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bnts.getDataCategory());
        assertEquals("[1970.01.01T03:20:13.435579000,1970.01.01T06:34:01.343568000," +
                "1970.01.01T06:39:55.876902000,1970.01.01T06:41:44.786790000]",btv.getSubVector(new int[]{3,0,1,2}).getString());
        assertEquals(BasicNanoTimestamp.class,bnts.getElementClass());
        btv.setNanoTimestamp(4, LocalDateTime.MIN);
        System.out.println(LocalDateTime.MIN.toString());
        assertEquals("1982-02-08T12:37:20",btv.getNanoTimestamp(4).toString());
        assertEquals("1982.02.08T12:37:20.000000000",btv.get(4).getString());
        assertEquals("1970-01-01T06:41:44.786790",bnts.getNanoTimestamp(2).toString());
    }

    @Test
    public void test_BasicNanoTimestampVector_capacity_lt_size() throws Exception {
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.set(3, (long)1);
        bbv.set(4, (long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999999999,1970.01.01T00:00:00.000000001,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimestampVector_size_capacity_set() throws Exception {
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(6,6);
        Assert.assertEquals("[1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2,  LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.set(3, (long)1);
        bbv.set(4, (long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999999999,1970.01.01T00:00:00.000000001,1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimestampVector_size_capacity_add() throws Exception {
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add( LocalDateTime.of(2021,01,11,01,11,20,999999999));
        bbv.add((long)1);
        bbv.add((long)0);
        Assert.assertEquals("[,,2021.01.11T01:11:20.999999999,1970.01.01T00:00:00.000000001,1970.01.01T00:00:00.000000000]", bbv.getString());
    }

    @Test
    public void test_BasicNanoTimestampVector_set_type_not_match() throws Exception {
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Long or null is supported.", re);
    }

    @Test
    public void test_BasicNanoTimestampVector_add_type_not_match() throws Exception {
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Long or null is supported.", re);
    }

    @Test
    public void test_BasicNanoTimeStampVector_Append() throws Exception {
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(new long[]{7554784040L,46274927491L});
        int capacity = bntsv.capacity;
        bntsv.Append(new BasicNanoTimestamp(4738492949L));
        bntsv.Append(new BasicNanoTimestamp(7843849393L));
        assertEquals(bntsv.size,bntsv.capacity);
        bntsv.Append(new BasicNanoTimestampVector(new long[]{36273293,3749284,73859372,47593902}));
        assertEquals(capacity*4,bntsv.size);
        String re = JSONObject.toJSONString(bntsv);
        System.out.println(re);
    }

    @Test
    public void testNanoTimeStampCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicNanoTimestampVector v = new BasicNanoTimestampVector(list);
        List<Long> list1 = Arrays.asList(1l,2l,3l);
        BasicNanoTimestampVector v1 = new BasicNanoTimestampVector(list1);
        BasicNanoTimestampVector v2 = (BasicNanoTimestampVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,1l,2l,3l);
        BasicNanoTimestampVector v3 = new BasicNanoTimestampVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicNanoTimeStampVector_toJSONString() throws Exception {
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(new long[]{7554784040L,46274927491L});
        String re = JSONObject.toJSONString(bntsv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[7554784040,46274927491],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_NANOTIMESTAMP\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicNanoTimestamp\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T00:00:07.554784040,1970.01.01T00:00:46.274927491]\",\"table\":false,\"unitLength\":16,\"values\":[7554784040,46274927491],\"vector\":true}", re);
    }
}
