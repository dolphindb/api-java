package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalTime;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BasicMonthVectorTest {

    @Test
    public void test_BasicMonthVector(){
        int[] array = new int[]{23641,23995,24104,1201};
        BasicMonthVector btv = new BasicMonthVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("0100-02",btv.getMonth(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[1970.02M,2008.09M,1999.08M]",btv.getSubVector(indices).getString());
        btv.setMonth(2, YearMonth.of(2012,12));
        assertEquals("2012-12",btv.getMonth(2).toString());
        assertEquals(YearMonth.class,btv.getElementClass());
        assertEquals("2012.12M",btv.get(2).getString());
        BasicMonthVector bdhv = new BasicMonthVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("0000-01",bdhv.getMonth(0).toString());
        List<Integer> list = Arrays.asList(23641,23995,24104,1201);
        BasicMonthVector bdhv2 = new BasicMonthVector(list);
        assertEquals("1999-08",bdhv2.getMonth(1).toString());
        BasicMonthVector bmv = new BasicMonthVector(4);
        bmv = bdhv2;
        assertEquals("2008-09",bmv.getMonth(2).toString());
    }

    @Test
    public void test_BasicMonthVector_capacity_lt_size() throws Exception {
        BasicMonthVector bbv = new BasicMonthVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, YearMonth.of(2021,11));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,2021.11M,2023.11M,0000.02M,0000.01M]", bbv.getString());
    }

    @Test
    public void test_BasicMonthVector_size_capacity_set() throws Exception {
        BasicMonthVector bbv = new BasicMonthVector(6,6);
        Assert.assertEquals("[0000.01M,0000.01M,0000.01M,0000.01M,0000.01M,0000.01M]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, YearMonth.of(2021,11));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        bbv.set(3, calendar);
        bbv.set(4, 1);
        bbv.set(5, 0);
        Assert.assertEquals("[,,2021.11M,2023.11M,0000.02M,0000.01M]", bbv.getString());
    }

    @Test
    public void test_BasicMonthVector_size_capacity_add() throws Exception {
        BasicMonthVector bbv = new BasicMonthVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(YearMonth.of(2021,11));
        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 15);
        bbv.add(calendar);
        bbv.add(1);
        bbv.add( 0);
        Assert.assertEquals("[,,2021.11M,2023.11M,0000.02M,0000.01M]", bbv.getString());
    }

    @Test
    public void test_BasicMonthVector_set_type_not_match() throws Exception {
        BasicMonthVector bbv = new BasicMonthVector(100,0);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only YearMonth, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicMonthVector_add_type_not_match() throws Exception {
        BasicMonthVector bbv = new BasicMonthVector(0,0);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only YearMonth, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicMonthVector_Append() throws Exception {
        BasicMonthVector bmv = new BasicMonthVector(new int[]{1,3,5});
        int size = bmv.size;
        int capacity = bmv.capacity;
        bmv.Append(new BasicMonth(13));
        assertEquals(capacity*2,bmv.capacity);
        bmv.Append(new BasicMonthVector(new int[]{7,8,10,12}));
        assertEquals(size+5,bmv.size);
        assertNotEquals(bmv.size,bmv.capacity);
    }

    @Test
    public void testMonthCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMonthVector v = new BasicMonthVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicMonthVector v1 = new BasicMonthVector(list1);
        BasicMonthVector v2 = (BasicMonthVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicMonthVector v3 = new BasicMonthVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void test_BasicMonthVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMonthVector v = new BasicMonthVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_MONTH\",\"dictionary\":false,\"elementClass\":\"java.time.YearMonth\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[0000.02M,0000.03M,0000.04M]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
