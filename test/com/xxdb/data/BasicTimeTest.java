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
    public void test_BasicTime_int(){
        String re = null;
        try{
            BasicTime time = new BasicTime(-1);
            System.out.println(time.getString());
        }catch(DateTimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Invalid value for NanoOfSecond (valid values 0 - 999999999): -1000000",re);
        BasicTime time1 = new BasicTime(0);
        assertEquals("00:00:00.000",time1.getString());
        BasicTime time2 = new BasicTime(1);
        assertEquals("00:00:00.001",time2.getString());
        BasicTime time3 = new BasicTime(100);
        assertEquals("00:00:00.100",time3.getString());
        BasicTime time4 = new BasicTime(140000);
        assertEquals("00:02:20.000",time4.getString());
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
