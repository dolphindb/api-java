package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicDateTimeTest {
    @Test
    public void testDateTimeCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateTimeVector v = new BasicDateTimeVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateTimeVector v1 = new BasicDateTimeVector(list1);
        BasicDateTimeVector v2 = (BasicDateTimeVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateTimeVector v3 = new BasicDateTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
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
    public void test_BasicDateTime_specimal_time() throws Exception {
        BasicDateTime nt = new BasicDateTime(LocalDateTime.of(2000,7,29,11,07));
        System.out.println(nt.getString());
        assertEquals("2000.07.29T11:07:00",nt.getString());
        BasicDateTime nt1 = new BasicDateTime(LocalDateTime.of(1969,7,29,11,07));
        System.out.println(nt1.getString());
        assertEquals("1969.07.29T11:07:00",nt1.getString());
        BasicDateTime nt2 = new BasicDateTime(LocalDateTime.of(2099,7,29,11,07));
        System.out.println(nt2.getString());
        assertEquals("2099.07.29T11:07:00",nt2.getString());
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
        conn.connect(HOST,PORT);
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeMatrix",bdtm);
        conn.upload(map);
        BasicDateTimeMatrix bdtm2 = (BasicDateTimeMatrix) conn.run("dateTimeMatrix");
        assertEquals("1984-05-27T16:27:10",bdtm2.getDateTime(1,1).toString());
        conn.close();
    }

    @Test
    public void test_BasicDateTimeVector() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(3);
        bdtv.setDateTime(0,LocalDateTime.MIN);
        bdtv.setDateTime(1,LocalDateTime.MAX);
        bdtv.setDateTime(2,LocalDateTime.now());
        assertEquals("1982-02-08T12:37:20",bdtv.getDateTime(0).toString());
        assertEquals("1982.02.08T12:37:20",bdtv.get(0).toString());
        bdtv.setNull(2);
        assertNull(bdtv.getDateTime(2));
        ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
        String HOST = bundle.getString("HOST");
        int PORT = Integer.parseInt(bundle.getString("PORT"));
        // int PORT = 8848;
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeVector",bdtv);
        conn.upload(map);
        BasicDateTimeVector bdtv2 = (BasicDateTimeVector) conn.run("dateTimeVector");
        assertEquals(BasicDateTime.class,bdtv2.getElementClass());
        conn.close();
    }

    @Test
    public void test_BasicDateTimeVector_wvtb() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(3);
        bdtv.setDateTime(0,LocalDateTime.MIN);
        bdtv.setDateTime(1,LocalDateTime.MAX);
        bdtv.setDateTime(2,LocalDateTime.now());
        ByteBuffer bb = bdtv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals(0,bb.get());
    }

    @Test
    public void test_BasicDateTime_toJSONString() throws Exception {
        BasicDateTime date = new BasicDateTime(LocalDateTime.of(1970,01,01,00,01,40));
        String re = JSONObject.toJSONString(date);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DATETIME\",\"dateTime\":\"1970-01-01 00:01:40\",\"dictionary\":false,\"int\":100,\"jsonString\":\"\\\"1970.01.01T00:01:40\\\"\",\"matrix\":false,\"null\":false,\"number\":100,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1970.01.01T00:01:40\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicDateTimeVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateTimeVector v = new BasicDateTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATETIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T00:00:01,1970.01.01T00:00:02,1970.01.01T00:00:03]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
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
}
