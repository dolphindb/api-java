package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicDateTimeTest {
    @Test
    public void test_BasicDateTime(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2);
        BasicDateTime date = new BasicDateTime(dt);
        String[] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2022,2,28,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,2,29,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,3,31,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,1,1,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,12,31,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,3,1,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2000,2,29,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());
    }
    @Test
    public void test_BasicDateTime_int(){
        BasicDateTime date = new BasicDateTime(-1);
        assertEquals("1969.12.31T23:59:59",date.getString());
        BasicDateTime date1 = new BasicDateTime(0);
        assertEquals("1970.01.01T00:00:00",date1.getString());
        BasicDateTime date2 = new BasicDateTime(1);
        assertEquals("1970.01.01T00:00:01",date2.getString());
        BasicDateTime date3 = new BasicDateTime(100);
        assertEquals("1970.01.01T00:01:40",date3.getString());
        BasicDateTime date4 = new BasicDateTime(1000000);
        assertEquals("1970.01.12T13:46:40",date4.getString());
    }
    @Test
    public void test_BasicDateTime_specimal_time() throws Exception {
        BasicDateTime nt = new BasicDateTime(LocalDateTime.of(2000,7,29,11,07));
        System.out.println(nt.getString());
        assertEquals("2000.07.29T11:07:00",nt.getString());
        BasicDateTime nt1 = new BasicDateTime(LocalDateTime.of(1969,7,29,11,07));
        System.out.println(nt1.getString());
        assertEquals("1969.07.29T11:07:00",nt1.getString());
        BasicDateTime nt2 = new BasicDateTime(LocalDateTime.of(2038,1,1,11,07));
        System.out.println(nt2.getString());
        assertEquals("2038.01.01T11:07:00",nt2.getString());
    }
    @Test
    public void test_BasicDateTimeMatrix(){
        BasicDateTimeMatrix bdtm = new BasicDateTimeMatrix(2,2);
        bdtm.setDateTime(0,0,LocalDateTime.of(2022,8,10,15,55));
        bdtm.setDateTime(0,1,LocalDateTime.of(1978,12,13,17,58));
        bdtm.setDateTime(1,0,LocalDateTime.MIN);
        bdtm.setDateTime(1,1,LocalDateTime.MAX);
        assertEquals("1966-02-13T07:02:23",bdtm.getDateTime(1,1).toString());
        assertEquals("1982.02.08T12:37:20",bdtm.get(1,0).getString());
        assertEquals(BasicDateTime.class,bdtm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdtm.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DATETIME,bdtm.getDataType());
    }

    @Test
    public void test_BasicDateTimeMatrix_list() throws Exception {
        List<int[]> list = new ArrayList<>();
        int[] a = new int[]{237651730,257689940};
        int[] b = new int[]{323537820,454523230};
        list.add(a);
        list.add(b);
        BasicDateTimeMatrix bdtm = new BasicDateTimeMatrix(2,2,list);
        ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
        String HOST = bundle.getString("HOST");
        int PORT = Integer.parseInt(bundle.getString("PORT"));
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeMatrix",bdtm);
        conn.upload(map);
        BasicDateTimeMatrix bdtm2 = (BasicDateTimeMatrix) conn.run("dateTimeMatrix");
        assertEquals("1984-05-27T16:27:10",bdtm2.getDateTime(1,1).toString());
        conn.close();
    }

    @Test
    public void test_BasicDateTime_toJSONString() throws Exception {
        BasicDateTime date = new BasicDateTime(100);
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DATETIME\",\"dateTime\":\"1970-01-01 00:01:40\",\"dictionary\":false,\"int\":100,\"jsonString\":\"\\\"1970.01.01T00:01:40\\\"\",\"matrix\":false,\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1970.01.01T00:01:40\",\"table\":false,\"vector\":false}", re);
    }

    @Test
    public void test_BasicDateTimeMatrix_toJSONString() throws Exception {
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{1,2});
        listofArrays.add(new int[]{3,4});
        BasicDateTimeMatrix bdhm2 = new BasicDateTimeMatrix(2,2,listofArrays);
        String re = JSONObject.toJSONString(bdhm2);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_DATETIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateTime\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0                  #1                 \\n1970.01.01T00:00:01 1970.01.01T00:00:03\\n1970.01.01T00:00:02 1970.01.01T00:00:04\\n\",\"table\":false,\"vector\":false}", re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicDateTimeMartix_getScale() throws Exception {
        BasicDateTimeMatrix bdtm = new BasicDateTimeMatrix(2,2);
        bdtm.setDateTime(0,0,LocalDateTime.of(2022,8,10,15,55));
        bdtm.setDateTime(0,1,LocalDateTime.of(1978,12,13,17,58));
        bdtm.setDateTime(1,0,LocalDateTime.MIN);
        bdtm.setDateTime(1,1,LocalDateTime.MAX);
        bdtm.getScale();
    }
}
