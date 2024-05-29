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
public class BasicSecondTest {
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
    public void test_BasicSecond_int(){
        String re = null;
        try{
            BasicSecond sec = new BasicSecond(-1);
            System.out.println(sec.getString());
        }catch(DateTimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Invalid value for SecondOfMinute (valid values 0 - 59): -1",re);
        BasicSecond sec1 = new BasicSecond(0);
        assertEquals("00:00:00",sec1.getString());
        BasicSecond sec2 = new BasicSecond(1);
        assertEquals("00:00:01",sec2.getString());
        BasicSecond sec3 = new BasicSecond(100);
        assertEquals("00:01:40",sec3.getString());
        BasicSecond sec4 = new BasicSecond(24000);
        assertEquals("06:40:00",sec4.getString());
    }
    @Test
    public void test_BasicSecondMatrix() throws Exception {
        BasicSecondMatrix bsm = new BasicSecondMatrix(2,2);
        bsm.setSecond(0,0,LocalTime.of(16,19,53));
        bsm.setSecond(0,1,LocalTime.of(11,11,11));
        bsm.setSecond(1,0,LocalTime.of(7,55,18));
        bsm.setSecond(1,1,LocalTime.of(23,57,55));
        assertEquals("11:11:11",bsm.getSecond(0,1).toString());
        assertEquals("23:57:55",bsm.get(1,1).getString());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bsm.getDataCategory());
        assertEquals(BasicSecond.class,bsm.getElementClass());
        List<int[]> listofArrays = new ArrayList<>();
        listofArrays.add(new int[]{28800,43215});
        listofArrays.add(new int[]{21600,54800});
        BasicSecondMatrix bsm2 = new BasicSecondMatrix(2,2,listofArrays);
        assertEquals("12:00:15",bsm2.getSecond(1,0).toString());
    }

    @Test
    public void test_BasicSecondVector(){
        BasicSecondVector bsv = new BasicSecondVector(4);
        bsv.setSecond(0,LocalTime.of(16,19,53));
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
    public void test_BasicSecondVector_Append() throws Exception {
        BasicSecondVector bsv = new BasicSecondVector(new int[]{12,34,56});
        int size = bsv.size;
        int capacity = bsv.capaticy;
        bsv.Append(new BasicSecond(LocalTime.now()));
        assertEquals(size+1,bsv.size);
        assertEquals(capacity*2,bsv.capaticy);
        bsv.Append(bsv);
        assertEquals(size+5,bsv.size);
        assertEquals(capacity*2+4,bsv.capaticy);
    }
    @Test
    public void test_BasicSecond_toJSONString() throws Exception {
        BasicSecond date = new BasicSecond(60);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_SECOND\",\"dictionary\":false,\"int\":60,\"jsonString\":\"\\\"00:01:00\\\"\",\"matrix\":false,\"null\":false,\"number\":60,\"pair\":false,\"scalar\":true,\"scale\":0,\"second\":\"00:01:00\",\"string\":\"00:01:00\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicSecondVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicSecondVector v = new BasicSecondVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_SECOND\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicSecond\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:01,00:00:02,00:00:03]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
    @Test
    public void test_BasicSecondMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{1,2});
        listofArrays.add(new int[]{3,4});
        BasicSecondMatrix bdhm2 = new BasicSecondMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_SECOND\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicSecond\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0       #1      \\n00:00:01 00:00:03\\n00:00:02 00:00:04\\n\",\"table\":false,\"vector\":false}", re);
    }
}
