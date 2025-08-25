package com.xxdb.data;
import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import static com.xxdb.data.Utils.countMilliseconds;
import static org.junit.Assert.*;
public class BasicNanoTimeTest {
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
    public void test_BasicNanoTime() throws Exception {
        BasicNanoTime nt = new BasicNanoTime(LocalTime.of(11,07,10,10));
        System.out.println(nt.getString());
        assertEquals("11:07:10.000000010",nt.getString());
        BasicNanoTime nt1 = new BasicNanoTime(LocalDateTime.of(2099,7,29,11,07));
        System.out.println(nt1.getString());
        assertEquals("11:07:00.000000000",nt1.getString());
    }
    @Test
    public void test_BasicNanoTime_long(){
        String re = null;
        try{
            BasicNanoTime nano = new BasicNanoTime(-1);
            System.out.println(nano.getString());
        }catch(DateTimeException ex){
          re = ex.getMessage();
        }
        assertEquals("Invalid value for NanoOfDay (valid values 0 - 86399999999999): -1",re);
        BasicNanoTime nano1 = new BasicNanoTime(0);
        assertEquals("00:00:00.000000000",nano1.getString());
        BasicNanoTime nano2 = new BasicNanoTime(1);
        assertEquals("00:00:00.000000001",nano2.getString());
        BasicNanoTime nano3 = new BasicNanoTime(100);
        assertEquals("00:00:00.000000100",nano3.getString());
        BasicNanoTime nano4 = new BasicNanoTime(140000000000l);
        assertEquals("00:02:20.000000000",nano4.getString());
    }
    @Test
    public void test_BasicNanoTimeMatrix() throws Exception{
        BasicNanoTimeMatrix bdhm = new BasicNanoTimeMatrix(2,2);
        bdhm.setNanoTime(0,0,LocalTime.of(19,12,25));
        bdhm.setNanoTime(0,1,LocalTime.of(20,13,35));
        bdhm.setNanoTime(1,0,LocalTime.of(21,14,45));
        bdhm.setNanoTime(1,1,LocalTime.of(22,15,55));
        assertEquals("21:14:45",bdhm.getNanoTime(1,0).toString());
        assertEquals(BasicNanoTime.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicNanoTimeMatrix bdhm2 = new BasicNanoTimeMatrix(2,2,listofArrays);
        assertEquals("06:39:55.876902",bdhm2.getNanoTime(1,0).toString());
    }

    @Test
    public void test_BasicNanoTimeVector(){
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimeVector btv = new BasicNanoTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("03:20:13.435579",btv.getNanoTime(3).toString());
        assertNull(btv.getNanoTime(4));
        int[] indices = new int[]{0,2,1};
        assertEquals("[06:34:01.343568000,06:41:44.786790000,06:39:55.876902000]",btv.getSubVector(indices).getString());
        btv.setNanoTime(2,LocalTime.of(22,12,17));
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
    public void test_BasicNanoTime_toJSONString() throws Exception {
        BasicNanoTime date = new BasicNanoTime(100);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_NANOTIME\",\"dictionary\":false,\"jsonString\":\"\\\"00:00:00.000000100\\\"\",\"long\":100,\"matrix\":false,\"nanoTime\":\"00:00:00.000000100\",\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"00:00:00.000000100\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicNanoTimeVector_toJSONString() throws Exception {
        List<Long> list = Arrays.asList(1L,2L,3L);
        BasicNanoTimeVector v = new BasicNanoTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_NANOTIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicNanoTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:00.000000001,00:00:00.000000002,00:00:00.000000003]\",\"table\":false,\"unitLength\":16,\"values\":[1,2,3],\"vector\":true}", re);
    }
    @Test
    public void test_BasicNanoTimeMatrix_toJSONString() throws Exception {
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{1,2});
        listofArrays.add(new long[]{3,4});
        BasicNanoTimeMatrix bdhm2 = new BasicNanoTimeMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_NANOTIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicNanoTime\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0                 #1                \\n00:00:00.000000001 00:00:00.000000003\\n00:00:00.000000002 00:00:00.000000004\\n\",\"table\":false,\"vector\":false}", re);
    }
}
