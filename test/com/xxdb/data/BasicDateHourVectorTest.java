package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicDateHourVectorTest {

    @Test
    public void test_BasicDateHourVector() throws Exception{
        int[] array = new int[]{2861000,7963000,4565000,2467000,Integer.MIN_VALUE};
        BasicDateHourVector btv = new BasicDateHourVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("2251-06-08T16:00",btv.getDateHour(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[2296.05.19T08,2490.10.09T08,2878.05.31T16]",btv.getSubVector(indices).getString());
        btv.setDateHour(2, LocalDateTime.of(1984,7,14,11,25));
        assertEquals("1984-07-14T11:00",btv.getDateHour(2).toString());
        assertNull(btv.getDateHour(4));
        assertEquals("2251.06.08T16",btv.get(3).getString());
        assertEquals(BasicDateHour.class,btv.getElementClass());
        BasicDateHourVector bdhv = new BasicDateHourVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("1970-01-01T00:00",bdhv.getDateHour(0).toString());
        List<Integer> list = Arrays.asList(2861000,7963000,4565000,2467000,Integer.MIN_VALUE);
        BasicDateHourVector bdhv2 = new BasicDateHourVector(list);
        assertEquals("2878-05-31T16:00",bdhv2.getDateHour(1).toString());
        ByteBuffer bb = ByteBuffer.allocate(10000);
        bdhv2.writeVectorToBuffer(bb);
    }

    @Test
    public void test_BasicDateHourVector_capacity_lt_size() throws Exception {
        BasicDateHourVector bbv = new BasicDateHourVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalDateTime.of(2022,7,29,11,07));
        bbv.set(3, LocalDateTime.of(1970,1,1,11,11));
        bbv.set(4, LocalDateTime.of(1993,6,23,15,36));
        bbv.set(5, LocalDateTime.MIN);
        Assert.assertEquals("[,,2022.07.29T11,1970.01.01T11,1993.06.23T15,+23758.06.10T16]", bbv.getString());
    }

    @Test
    public void test_BasicDateHourVector_size_capacity_set() throws Exception {
        BasicDateHourVector bbv = new BasicDateHourVector(6,6);
        Assert.assertEquals("[1970.01.01T00,1970.01.01T00,1970.01.01T00,1970.01.01T00,1970.01.01T00,1970.01.01T00]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, LocalDateTime.of(2022,7,29,11,07));
        bbv.set(3, LocalDateTime.of(1970,1,1,11,11));
        bbv.set(4, LocalDateTime.of(1993,6,23,15,36));
        bbv.set(5, LocalDateTime.MIN);
        Assert.assertEquals("[,,2022.07.29T11,1970.01.01T11,1993.06.23T15,+23758.06.10T16]", bbv.getString());
    }

    @Test
    public void test_BasicDateHourVector_size_capacity_add() throws Exception {
        BasicDateHourVector bbv = new BasicDateHourVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(LocalDateTime.of(2022,7,29,11,07));
        bbv.add(LocalDateTime.of(1970,1,1,11,11));
        bbv.add(LocalDateTime.of(1993,6,23,15,36));
        bbv.add(LocalDateTime.MIN);
        Assert.assertEquals("[,,2022.07.29T11,1970.01.01T11,1993.06.23T15,+23758.06.10T16]", bbv.getString());
    }

    @Test
    public void test_BasicDateHourVector_set_type_not_match() throws Exception {
        BasicDateHourVector bbv = new BasicDateHourVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateHourVector_add_type_not_match() throws Exception {
        BasicDateHourVector bbv = new BasicDateHourVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateHourVector_Append() throws Exception {
        BasicDateHourVector bdhv = new BasicDateHourVector(new int[]{476,1004});
        int size = bdhv.size;
        int capacity = bdhv.capacity;
        bdhv.Append(new BasicDateHour(LocalDateTime.now()));
        assertEquals(capacity*2,bdhv.capacity);
        System.out.println(bdhv.get(2));
        bdhv.Append(bdhv);
        assertEquals(size+4,bdhv.size);
        assertEquals(6,bdhv.capacity);
    }

    @Test
    public void testDateHourCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateHourVector v = new BasicDateHourVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateHourVector v1 = new BasicDateHourVector(list1);
        BasicDateHourVector v2 = (BasicDateHourVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateHourVector v3 = new BasicDateHourVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_BasicDateHourVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateHourVector v = new BasicDateHourVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATEHOUR\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateHour\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T01,1970.01.01T02,1970.01.01T03]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
