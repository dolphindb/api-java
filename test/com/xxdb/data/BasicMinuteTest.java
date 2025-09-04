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
public class BasicMinuteTest {
    @Test
    public void test_BasicMinute() throws Exception {
        BasicMinute nt = new BasicMinute(-5);
        System.out.println(nt.getString());
        assertEquals("23:55m",nt.getString());
        BasicMinute nt1 = new BasicMinute(0);
        System.out.println(nt1.getString());
        assertEquals("00:00m",nt1.getString());
        BasicMinute nt2 = new BasicMinute(10);
        System.out.println(nt2.getString());
        assertEquals("00:10m",nt2.getString());
        BasicMinute nt3 = new BasicMinute(100);
        System.out.println(nt3.getString());
        assertEquals("01:40m",nt3.getString());
    }
    @Test
    public void test_BasicMinute_int(){
        BasicMinute min = new BasicMinute(-1);
        assertEquals("23:59m",min.getString());
        BasicMinute min1 = new BasicMinute(0);
        assertEquals("00:00m",min1.getString());
        BasicMinute min2 = new BasicMinute(1);
        assertEquals("00:01m",min2.getString());
        BasicMinute min3 = new BasicMinute(100);
        assertEquals("01:40m",min3.getString());
        BasicMinute min4 = new BasicMinute(1400);
        assertEquals("23:20m",min4.getString());
    }
    @Test
    public void test_BasicMinuteMatrix() throws Exception{
        BasicMinuteMatrix bdhm = new BasicMinuteMatrix(2,2);
        bdhm.setMinute(0,0,LocalTime.of(1,9,4));
        bdhm.setMinute(0,1,LocalTime.of(1,11,11));
        bdhm.setMinute(1,0,LocalTime.of(23,15,36));
        bdhm.setMinute(1,1,LocalTime.MIN);
        assertEquals("23:15",bdhm.getMinute(1,0).toString());
        assertEquals(BasicMinute.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{125,300});
        listofArrays.add(new int[]{456,200});
        BasicMinuteMatrix bdhm2 = new BasicMinuteMatrix(2,2,listofArrays);
        assertEquals("05:00",bdhm2.getMinute(1,0).toString());
    }

    @Test
    public void test_BasicMinute_toJSONString() throws Exception {
        BasicMinute date = new BasicMinute(100);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_MINUTE\",\"dictionary\":false,\"int\":100,\"jsonString\":\"\\\"01:40m\\\"\",\"matrix\":false,\"minute\":\"01:40:00\",\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"01:40m\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicMinuteMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{1,2});
        listofArrays.add(new int[]{3,4});
        BasicMinuteMatrix bdhm2 = new BasicMinuteMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_MINUTE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicMinute\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0     #1    \\n00:01m 00:03m\\n00:02m 00:04m\\n\",\"table\":false,\"vector\":false}", re);
    }
}
