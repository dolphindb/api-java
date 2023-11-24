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
public class BasicDateTest {
    @Test
    public void testDateCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateVector v = new BasicDateVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateVector v1 = new BasicDateVector(list1);
        BasicDateVector v2 = (BasicDateVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateVector v3 = new BasicDateVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
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
    public void test_BasicDateVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateVector v = new BasicDateVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATE\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDate\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.02,1970.01.03,1970.01.04]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
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
}
