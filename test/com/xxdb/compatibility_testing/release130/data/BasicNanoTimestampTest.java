package com.xxdb.compatibility_testing.release130.data;
import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import static com.xxdb.data.Utils.countMilliseconds;
import static org.junit.Assert.*;
public class BasicNanoTimestampTest {
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
    public void test_BasicNanoTimestamp(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2,32154365);
        BasicNanoTimestamp date = new BasicNanoTimestamp(dt);
        String [] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,2,28,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
    }
    @Test
    public void test_BasicNanoTimestamp_special_time() throws Exception {
        BasicNanoTimestamp nt = new BasicNanoTimestamp(LocalDateTime.of(2000,7,29,11,07));
        System.out.println(nt.getString());
        assertEquals("2000.07.29T11:07:00.000000000",nt.getString());
        BasicNanoTimestamp nt1 = new BasicNanoTimestamp(LocalDateTime.of(1969,7,29,11,07));
        System.out.println(nt1.getString());
        assertEquals("1969.07.29T11:07:00.000000000",nt1.getString());
        BasicNanoTimestamp nt2 = new BasicNanoTimestamp(LocalDateTime.of(2099,7,29,11,07));
        System.out.println(nt2.getString());
        assertEquals("2099.07.29T11:07:00.000000000",nt2.getString());
    }
    @Test
    public void test_countNanoseconds() throws Exception {
        BasicNanoTimestamp nt = new BasicNanoTimestamp(LocalDateTime.of(2000,7,29,11,07));
        System.out.println(nt.getString());
        assertEquals("2000.07.29T11:07:00.000000000",nt.getString());
        BasicNanoTimestamp nt1 = new BasicNanoTimestamp(LocalDateTime.of(1969,7,29,11,07));
        System.out.println(nt1.getString());
        assertEquals("1969.07.29T11:07:00.000000000",nt1.getString());
        BasicNanoTimestamp nt2 = new BasicNanoTimestamp(LocalDateTime.of(2099,7,29,11,07));
        System.out.println(nt2.getString());
        assertEquals("2099.07.29T11:07:00.000000000",nt2.getString());
    }
    @Test
    public void test_BasicNanoTimeStampMatrix() throws Exception{
        BasicNanoTimestampMatrix bdhm = new BasicNanoTimestampMatrix(2,2);
        bdhm.setNanoTimestamp(0,0,LocalDateTime.of(1970,11,22,19,12,25));
        bdhm.setNanoTimestamp(0,1,LocalDateTime.of(1978,12,13,20,13,35));
        bdhm.setNanoTimestamp(1,0,LocalDateTime.of(1984,5,18,21,14,45));
        bdhm.setNanoTimestamp(1,1,LocalDateTime.of(1987,12,12,22,15,55));
        assertEquals("1984-05-18T21:14:45",bdhm.getNanoTimestamp(1,0).toString());
        assertEquals(BasicNanoTimestamp.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicNanoTimestampMatrix bdhm2 = new BasicNanoTimestampMatrix(2,2,listofArrays);
        assertEquals("1970-01-01T06:39:55.876902",bdhm2.getNanoTimestamp(1,0).toString());
    }

    @Test
    public void test_BasicNanoTimeStampVector(){
        BasicNanoTimestampVector bnts = new BasicNanoTimestampVector(5);
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimestampVector btv = new BasicNanoTimestampVector(array);
        assertNull(btv.getNanoTimestamp(4));
        System.out.println(btv.getNanoTimestamp(4));
        bnts = btv;
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bnts.getDataCategory());
        assertEquals("[1970.01.01T03:20:13.435579000,1970.01.01T06:34:01.343568000," +
                "1970.01.01T06:39:55.876902000,1970.01.01T06:41:44.786790000]",btv.getSubVector(new int[]{3,0,1,2}).getString());
        assertEquals(BasicNanoTimestamp.class,bnts.getElementClass());
        btv.setNanoTimestamp(4,LocalDateTime.MIN);
        assertEquals("1982-02-08T12:37:20",btv.getNanoTimestamp(4).toString());
        assertEquals("1982.02.08T12:37:20.000000000",btv.get(4).getString());
        assertEquals("1970-01-01T06:41:44.786790",bnts.getNanoTimestamp(2).toString());
    }
    @Test
    public void test_BasicNanoTimeStamp_toJSONString() throws Exception {
        BasicNanoTimestamp date = new BasicNanoTimestamp(LocalDateTime.from(LocalTime.ofSecondOfDay(100)));
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_NANOTIMESTAMP\",\"dictionary\":false,\"jsonString\":\"\\\"1970.01.01T00:00:00.000000100\\\"\",\"long\":100,\"matrix\":false,\"nanoTimestamp\":\"1970-01-01 00:00:00.000000100\",\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1970.01.01T00:00:00.000000100\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicNanoTimeStampMatrix_toJSONString() throws Exception {
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{1,2});
        listofArrays.add(new long[]{3,4});
        BasicNanoTimestampMatrix bdhm2 = new BasicNanoTimestampMatrix(2,2,listofArrays);
        System.out.println(bdhm2.getString());

        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_NANOTIMESTAMP\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicNanoTimestamp\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0                        #1                       \\n1970.01.01T00:00:00.000...1970.01.01T00:00:00.00...\\n1970.01.01T00:00:00.000...1970.01.01T00:00:00.00...\\n\",\"table\":false,\"vector\":false}", re);
    }

}
