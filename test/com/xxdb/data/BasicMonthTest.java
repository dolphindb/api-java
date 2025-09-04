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
    public void test_BasicMonth_int(){
        BasicMonth mon1 = new BasicMonth(0);
        assertEquals("0000.01M",mon1.getString());
        BasicMonth mon2 = new BasicMonth(1);
        assertEquals("0000.02M",mon2.getString());
        BasicMonth mon3 = new BasicMonth(100);
        assertEquals("0008.05M",mon3.getString());
        BasicMonth mon4 = new BasicMonth(1400);
        assertEquals("0116.09M",mon4.getString());
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
    public void test_BasicMonth_toJSONString() throws Exception {
        BasicMonth mo = new BasicMonth(2022, Month.JULY);
        String re = JSONObject.toJSONString(mo);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_MONTH\",\"dictionary\":false,\"int\":24270,\"jsonString\":\"\\\"2022.07M\\\"\",\"matrix\":false,\"month\":{\"leapYear\":false,\"month\":\"JULY\",\"monthValue\":7,\"year\":2022},\"null\":false,\"number\":24270,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"2022.07M\",\"table\":false,\"vector\":false}", re);
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
