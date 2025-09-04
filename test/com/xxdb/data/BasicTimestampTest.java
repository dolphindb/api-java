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
public class BasicTimestampTest {
    @Test
    public void test_BasicTimestamp(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2,4000000);
        BasicTimestamp date = new BasicTimestamp(dt);
        LocalDateTime dt1 = LocalDateTime.of(2023,9,18,14,51,24,000);
        BasicTimestamp date1 = new BasicTimestamp(dt1);
        System.out.println(date1.getLong());
        String [] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,2,28,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
    }
    @Test
    public void test_BasicTimestamp_long(){
        BasicTimestamp nano = new BasicTimestamp(-1);
        assertEquals("1969.12.31T23:59:59.999",nano.getString());
        BasicTimestamp nano1 = new BasicTimestamp(0);
        assertEquals("1970.01.01T00:00:00.000",nano1.getString());
        BasicTimestamp nano2 = new BasicTimestamp(1);
        assertEquals("1970.01.01T00:00:00.001",nano2.getString());
        BasicTimestamp nano3 = new BasicTimestamp(100);
        assertEquals("1970.01.01T00:00:00.100",nano3.getString());
        BasicTimestamp nano4 = new BasicTimestamp(140000000000l);
        assertEquals("1974.06.09T08:53:20.000",nano4.getString());
    }
    @Test
    public void test_BasicTimeStampMatrix() throws Exception {
        BasicTimestampMatrix bdhm = new BasicTimestampMatrix(2,2);
        bdhm.setTimestamp(0,0,LocalDateTime.of(1970,11,22,19,12,25));
        bdhm.setTimestamp(0,1,LocalDateTime.of(1978,12,13,20,13,35));
        bdhm.setTimestamp(1,0,LocalDateTime.of(1984,5,18,21,14,45));
        bdhm.setTimestamp(1,1,LocalDateTime.of(1987,12,12,22,15,55));
        assertEquals("1984-05-18T21:14:45",bdhm.getTimestamp(1,0).toString());
        assertEquals(BasicTimestamp.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicTimestampMatrix bdhm2 = new BasicTimestampMatrix(2,2,listofArrays);
        assertEquals("2730-05-27T01:21:42",bdhm2.getTimestamp(1,0).toString());
    }

    @Test
    public void test_BasicTimestamp_toJSONString() throws Exception {
        BasicTimestamp date = new BasicTimestamp(100);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_TIMESTAMP\",\"dictionary\":false,\"jsonString\":\"\\\"1970.01.01T00:00:00.100\\\"\",\"long\":100,\"matrix\":false,\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1970.01.01T00:00:00.100\",\"table\":false,\"timestamp\":\"1970-01-01 00:00:00.100\",\"vector\":false}", re);
    }

    @Test
    public void test_BasicTimestampMatrix_toJSONString() throws Exception {
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{1,2});
        listofArrays.add(new long[]{3,4});
        BasicTimestampMatrix bdhm2 = new BasicTimestampMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_TIMESTAMP\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicTimestamp\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0                      #1                     \\n1970.01.01T00:00:00.001 1970.01.01T00:00:00.003\\n1970.01.01T00:00:00.002 1970.01.01T00:00:00.004\\n\",\"table\":false,\"vector\":false}", re);
    }
}
