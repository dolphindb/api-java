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
public class BasicMonthTest {
    @Test
    public void testMonthCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMonthVector v = new BasicMonthVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicMonthVector v1 = new BasicMonthVector(list1);
        BasicMonthVector v2 = (BasicMonthVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicMonthVector v3 = new BasicMonthVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void test_BasicMonth_negative() throws DateTimeException {
        String re = null;
        try{
            BasicMonth nt1 = new BasicMonth(-5);
            System.out.println(nt1.getString());
        }catch(DateTimeException e){
            re = e.getMessage();
        }
        assertEquals("Invalid value for MonthOfYear (valid values 1 - 12): -4",re);
    }
    @Test
    public void test_BasicMonthMatrix() throws Exception{
        BasicMonthMatrix bdhm = new BasicMonthMatrix(2,2);
        bdhm.setMonth(0,0,YearMonth.of(1978,12));
        bdhm.setMonth(0,1,YearMonth.of(1997,6));
        bdhm.setMonth(1,0,YearMonth.of(1999,12));
        bdhm.setMonth(1,1,YearMonth.of(2008,8));
        assertEquals("1999-12",bdhm.getMonth(1,0).toString());
        assertEquals(YearMonth.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{23641,23995});
        listofArrays.add(new int[]{24104,1201});
        BasicMonthMatrix bdhm2 = new BasicMonthMatrix(2,2,listofArrays);
        assertEquals("1999-08",bdhm2.getMonth(1,0).toString());
    }

    @Test
    public void test_BasicMonthVector(){
        int[] array = new int[]{23641,23995,24104,1201};
        BasicMonthVector btv = new BasicMonthVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("0100-02",btv.getMonth(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[1970.02M,2008.09M,1999.08M]",btv.getSubVector(indices).getString());
        btv.setMonth(2,YearMonth.of(2012,12));
        assertEquals("2012-12",btv.getMonth(2).toString());
        assertEquals(YearMonth.class,btv.getElementClass());
        assertEquals("2012.12M",btv.get(2).getString());
        BasicMonthVector bdhv = new BasicMonthVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("0000-01",bdhv.getMonth(0).toString());
        List<Integer> list = Arrays.asList(23641,23995,24104,1201);
        BasicMonthVector bdhv2 = new BasicMonthVector(list);
        assertEquals("1999-08",bdhv2.getMonth(1).toString());
        BasicMonthVector bmv = new BasicMonthVector(4);
        bmv = bdhv2;
        assertEquals("2008-09",bmv.getMonth(2).toString());
    }

    @Test
    public void test_BasicMonthVector_Append() throws Exception {
        BasicMonthVector bmv = new BasicMonthVector(new int[]{1,3,5});
        int size = bmv.size;
        int capacity = bmv.capaticy;
        bmv.Append(new BasicMonth(13));
        assertEquals(capacity*2,bmv.capaticy);
        bmv.Append(new BasicMonthVector(new int[]{7,8,10,12}));
        assertEquals(size+5,bmv.size);
        assertNotEquals(bmv.size,bmv.capaticy);
    }
    @Test
    public void test_BasicMonth_toJSONString() throws Exception {
        BasicMonth mo = new BasicMonth(2022, Month.JULY);
        String re = JSONObject.toJSONString(mo);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_MONTH\",\"dictionary\":false,\"int\":24270,\"jsonString\":\"\\\"2022.07M\\\"\",\"matrix\":false,\"month\":{\"leapYear\":false,\"month\":\"JULY\",\"monthValue\":7,\"year\":2022},\"null\":false,\"number\":24270,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"2022.07M\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicMonthVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMonthVector v = new BasicMonthVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_MONTH\",\"dictionary\":false,\"elementClass\":\"java.time.YearMonth\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[0000.02M,0000.03M,0000.04M]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
    }
    @Test
    public void test_BasicMonthMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861,7963});
        listofArrays.add(new int[]{4565,2467});
        BasicMonthMatrix bdhm2 = new BasicMonthMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_MONTH\",\"dictionary\":false,\"elementClass\":\"java.time.YearMonth\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0       #1      \\n0238.06M 0380.06M\\n0663.08M 0205.08M\\n\",\"table\":false,\"vector\":false}", re);
    }
}
