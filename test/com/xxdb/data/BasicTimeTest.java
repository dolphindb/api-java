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
public class BasicTimeTest {
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
    public void test_BasicDate(){
        LocalDate dt = LocalDate.of(2022,1,31);
        BasicDate date = new BasicDate(dt);
        assertEquals("2022.01.31",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2022,2,28);
        date = new BasicDate(dt);
        assertEquals("2022.02.28",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,2,29);
        date = new BasicDate(dt);
        assertEquals("2008.02.29",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,3,31);
        date = new BasicDate(dt);
        String[] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,1,1);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,12,31);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,3,1);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2000,2,29);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

    }
    @Test
    public void test_BasicTime_constructor(){
        assertTrue(new BasicTime(new GregorianCalendar()).getString().contains(new SimpleDateFormat("HH:mm:ss").format(new Date())));
        assertFalse(new BasicTime(new GregorianCalendar()).equals(null));
    }
    @Test
    public void test_BasicTimeMatrix() throws Exception {
        BasicTimeMatrix btm = new BasicTimeMatrix(2,2);
        btm.setTime(0,0,LocalTime.of(1,7,44));
        btm.setTime(0,1,LocalTime.of(3,17));
        btm.setTime(1,0,LocalTime.of(11,36,52));
        btm.setTime(1,1,LocalTime.now());
        assertEquals("11:36:52",btm.getTime(1,0).toString());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btm.getDataCategory());
        assertEquals(BasicTime.class,btm.getElementClass());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{61000,63000});
        listofArrays.add(new int[]{65000,67000});
        BasicTimeMatrix btm2 = new BasicTimeMatrix(2,2,listofArrays);
        assertEquals("00:01:07",btm2.getTime(1,1).toString());
    }

    @Test
    public void test_BasicTimeVector(){
        int[] array = new int[]{53000,75000,115000,145000,Integer.MIN_VALUE};
        BasicTimeVector btv = new BasicTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("00:02:25",btv.getTime(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[00:00:53.000,00:01:55.000,00:01:15.000]",btv.getSubVector(indices).getString());
        btv.setTime(2,LocalTime.of(1,25,42));
        assertEquals("01:25:42",btv.getTime(2).toString());
        assertEquals(BasicTime.class,btv.getElementClass());
        BasicTimeVector btv2 = new BasicTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        assertNull(btv.getTime(4));
        BasicTimeVector btv3 = new BasicTimeVector(1);
        assertEquals("00:00",btv3.getTime(0).toString());
        assertEquals(new BasicTime(53000),btv.get(0));
    }

    @Test
    public void test_BasicTimeVector_Append() throws Exception {
        BasicTimeVector btv = new BasicTimeVector(new int[]{1893,1976});
        int size = btv.size;
        int capacity = btv.capaticy;
        btv.Append(new BasicTime(2022));
        assertEquals(size+1,btv.size);
        assertEquals(capacity*2,btv.capaticy);
        btv.Append(new BasicTimeVector(new int[]{618,755,907}));
        assertEquals(size+4,btv.size);
        assertEquals(capacity*2+3,btv.capaticy);
    }
    @Test
    public void test_BasicTime(){
        assertFalse(new BasicDate(new GregorianCalendar()).equals(null));
        assertFalse(new BasicDateHour(2).equals(null));
        assertFalse(new BasicDateTime(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMinute(LocalTime.now()).equals(null));
        assertFalse(new BasicMinute(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMonth(2022, Month.JULY).equals(null));
        assertFalse(new BasicMonth(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMonth(YearMonth.of(2022,7)).equals(null));
        assertFalse(new BasicNanoTime(LocalTime.now()).equals(null));
        assertFalse(new BasicNanoTimestamp(3000L).equals(null));
        assertFalse(new BasicSecond(LocalTime.now()).equals(null));
        assertFalse(new BasicSecond(new GregorianCalendar()).equals(null));
        assertFalse(new BasicTimestamp(new GregorianCalendar()).equals(null));
    }
    @Test
    public void test_BasicTime_toJSONString() throws Exception {
        BasicTime date = new BasicTime(53000);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_TIME\",\"dictionary\":false,\"int\":53000,\"jsonString\":\"\\\"00:00:53.000\\\"\",\"matrix\":false,\"null\":false,\"number\":53000,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"00:00:53.000\",\"table\":false,\"time\":\"00:00:53\",\"vector\":false}", re);
    }
    @Test
    public void test_BasicTimeVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicTimeVector v = new BasicTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_TIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[00:00:00.001,00:00:00.002,00:00:00.003]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
    }
    @Test
    public void test_BasicTimeMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861,7963});
        listofArrays.add(new int[]{4565,2467});
        BasicTimeMatrix bdhm2 = new BasicTimeMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_TIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicTime\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0           #1          \\n00:00:02.861 00:00:04.565\\n00:00:07.963 00:00:02.467\\n\",\"table\":false,\"vector\":false}", re);
    }
}
