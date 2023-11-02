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
public class BasicDateHourTest {
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
    public void test_BasicDateHour(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(2022,0,31,2,2,2);
        BasicDateHour date = new BasicDateHour(calendar);
        assertEquals("2022.01.31T02",date.getString());
        LocalDateTime dt = LocalDateTime.of(2022,2,28,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.02.28T02",date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2008.02.29T02",date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.03.31T02",date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.01.01T02",date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.12.31T02",date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.03.01T02",date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2000.02.29T02",date.getString());
    }
    @Test
    public void test_BasicDateHourMatrix() throws Exception {
        BasicDateHourMatrix bdhm = new BasicDateHourMatrix(2,2);
        bdhm.setDateHour(0,0,LocalDateTime.of(2022,7,29,11,07));
        bdhm.setDateHour(0,1,LocalDateTime.of(1970,1,1,11,11));
        bdhm.setDateHour(1,0,LocalDateTime.of(1993,6,23,15,36));
        bdhm.setDateHour(1,1,LocalDateTime.MIN);
        assertEquals("1993-06-23T15:00",bdhm.getDateHour(1,0).toString());
        assertEquals(BasicDateHour.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861000,7963000});
        listofArrays.add(new int[]{4565000,2467000});
        BasicDateHourMatrix bdhm2 = new BasicDateHourMatrix(2,2,listofArrays);
        assertEquals("2490-10-09T08:00",bdhm2.getDateHour(0,1).toString());
    }

    @Test
    public void test_BasicDateHourVector() throws Exception{
        int[] array = new int[]{2861000,7963000,4565000,2467000,Integer.MIN_VALUE};
        BasicDateHourVector btv = new BasicDateHourVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("2251-06-08T16:00",btv.getDateHour(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[2296.05.19T08,2490.10.09T08,2878.05.31T16]",btv.getSubVector(indices).getString());
        btv.setDateHour(2,LocalDateTime.of(1984,7,14,11,25));
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
    public void test_BasicDateHourVector_Append() throws Exception {
        BasicDateHourVector bdhv = new BasicDateHourVector(new int[]{476,1004});
        int size = bdhv.size;
        int capacity = bdhv.capaticy;
        bdhv.Append(new BasicDateHour(LocalDateTime.now()));
        assertEquals(capacity*2,bdhv.capaticy);
        System.out.println(bdhv.get(2));
        bdhv.Append(bdhv);
        assertEquals(size+4,bdhv.size);
        assertEquals(7,bdhv.capaticy);
    }
    @Test
    public void test_BasicDateHour_toJSONString() throws Exception {
        BasicDateHour date = new BasicDateHour(100);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DATEHOUR\",\"dateHour\":\"1970-01-05 04:00:00\",\"dictionary\":false,\"int\":100,\"jsonString\":\"\\\"1970.01.05T04\\\"\",\"matrix\":false,\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1970.01.05T04\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicDateHourVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateHourVector v = new BasicDateHourVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATEHOUR\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateHour\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T01,1970.01.01T02,1970.01.01T03]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
    }
    @Test
    public void test_BasicDateHourMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{1,2});
        listofArrays.add(new int[]{3,4});
        BasicDateHourMatrix bdhm2 = new BasicDateHourMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_DATEHOUR\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateHour\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0            #1           \\n1970.01.01T01 1970.01.01T03\\n1970.01.01T02 1970.01.01T04\\n\",\"table\":false,\"vector\":false}", re);
    }
}
