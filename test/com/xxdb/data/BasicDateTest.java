package com.xxdb.data;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import static org.junit.Assert.*;
public class BasicDateTest {
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
    public void test_BasicDate_int(){
        BasicDate v = new BasicDate(1);
        assertEquals("1970.01.02" ,v.getString());
        BasicDate v1 = new BasicDate(100);
        assertEquals("1970.04.11" ,v1.getString());
        BasicDate v2 = new BasicDate(100000);
        assertEquals("2243.10.17" ,v2.getString());
        BasicDate v3 = new BasicDate(-1);
        assertEquals("1969.12.31" ,v3.getString());
        BasicDate v4 = new BasicDate(0);
        assertEquals("1970.01.01" ,v4.getString());
    }

    @Test
    public void test_BasicDateMatrix() throws Exception {
        BasicDateMatrix bdhm = new BasicDateMatrix(2,2);
        bdhm.setDate(0,0,LocalDate.of(2022,7,29));
        bdhm.setDate(0,1,LocalDate.of(1970,1,1));
        bdhm.setDate(1,0,LocalDate.of(1993,6,23));
        bdhm.setDate(1,1,LocalDate.MIN);
        assertEquals("1993-06-23",bdhm.getDate(1,0).toString());
        assertEquals(BasicDate.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861,7963});
        listofArrays.add(new int[]{4565,2467});
        BasicDateMatrix bdhm2 = new BasicDateMatrix(2,2,listofArrays);
        assertEquals("1982-07-02",bdhm2.getDate(0,1).toString());
    }

    @Test
    public void test_BasicDate_toJSONString() throws Exception {
        LocalDate dt = LocalDate.of(2022,1,31);
        BasicDate date = new BasicDate(dt);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DATE\",\"date\":\"2022-01-31\",\"dictionary\":false,\"int\":19023,\"jsonString\":\"\\\"2022.01.31\\\"\",\"matrix\":false,\"null\":false,\"number\":19023,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"2022.01.31\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void test_BasicDateMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861,7963});
        listofArrays.add(new int[]{4565,2467});
        BasicDateMatrix bdhm2 = new BasicDateMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_DATE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDate\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0         #1        \\n1977.11.01 1982.07.02\\n1991.10.21 1976.10.03\\n\",\"table\":false,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicDateMartix_getScale() throws Exception {
        BasicDateMatrix bdhm = new BasicDateMatrix(2,2);
        bdhm.setDate(0,0,LocalDate.of(2022,7,29));
        bdhm.setDate(0,1,LocalDate.of(1970,1,1));
        bdhm.setDate(1,0,LocalDate.of(1993,6,23));
        bdhm.setDate(1,1,LocalDate.MIN);
        bdhm.getScale();
    }
}
