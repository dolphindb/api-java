package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicDateTimeVectorTest {

    @Test
    public void test_BasicDateTimeVector() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        bdtv.setDateTime(0, LocalDateTime.MIN);
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
        conn.connect(HOST,PORT,"admin","123456");
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeVector",bdtv);
        conn.upload(map);
        BasicDateTimeVector bdtv2 = (BasicDateTimeVector) conn.run("dateTimeVector");
        assertEquals(BasicDateTime.class,bdtv2.getElementClass());
        conn.close();
    }

    @Test
    public void test_BasicDateTimeVector_capacity_lt_size() throws Exception {
        BasicDateTimeVector bbv = new BasicDateTimeVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, 1);
        bbv.set(3, -1);
        bbv.set(4, 2);
        bbv.set(5, 10000);
        Assert.assertEquals("[,,1970.01.01T00:00:01,1969.12.31T23:59:59,1970.01.01T00:00:02,1970.01.01T02:46:40]", bbv.getString());
    }

    @Test
    public void test_BasicDateTimeVector_size_capacity_set() throws Exception {
        BasicDateTimeVector bbv = new BasicDateTimeVector(6,6);
        Assert.assertEquals("[1970.01.01T00:00:00,1970.01.01T00:00:00,1970.01.01T00:00:00,1970.01.01T00:00:00,1970.01.01T00:00:00,1970.01.01T00:00:00]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, 1);
        bbv.set(3, -1);
        bbv.set(4, 2);
        bbv.set(5, 10000);
        Assert.assertEquals("[,,1970.01.01T00:00:01,1969.12.31T23:59:59,1970.01.01T00:00:02,1970.01.01T02:46:40]", bbv.getString());
    }

    @Test
    public void test_BasicDateTimeVector_size_capacity_add() throws Exception {
        BasicDateTimeVector bbv = new BasicDateTimeVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add( (Object)null);
        bbv.add( null);
        bbv.add( 1);
        bbv.add( -1);
        bbv.add( 2);
        bbv.add( 10000);
        Assert.assertEquals("[,,1970.01.01T00:00:01,1969.12.31T23:59:59,1970.01.01T00:00:02,1970.01.01T02:46:40]", bbv.getString());
    }

    @Test
    public void test_BasicDateTimeVector_set_type_not_match() throws Exception {
        BasicDateTimeVector bbv = new BasicDateTimeVector(1,1);
        String re = null;
        try{
            bbv.set(0,"1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateTimeVector_add_type_not_match() throws Exception {
        BasicDateTimeVector bbv = new BasicDateTimeVector(1,1);
        String re = null;
        try{
            bbv.add("1");
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.String. Only LocalDateTime, Calendar, Integer or null is supported.", re);
    }

    @Test
    public void test_BasicDateTimeVector_wvtb() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        bdtv.setDateTime(0,LocalDateTime.MIN);
        bdtv.setDateTime(1,LocalDateTime.MAX);
        bdtv.setDateTime(2,LocalDateTime.now());
        ByteBuffer bb = bdtv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals(0,bb.get());
    }

    @Test
    public void test_BasicDateTimeVector_other(){
        int[] arr = new int[]{32245761,43556722,53367869};
        BasicDateTimeVector bdtv = new BasicDateTimeVector(arr,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdtv.getDataCategory());
        assertEquals("[1971.09.10T16:24:29,1971.01.09T05:09:21,1971.05.20T03:05:22]",bdtv.getSubVector(new int[]{2,0,1}).getString());
    }

    @Test
    public void test_BasicDateTimeVector_Append() throws Exception {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{3354,324,342});
        bdtv.Append(bdtv);
        assertEquals(bdtv.size,bdtv.capacity);
        int size = bdtv.size;
        int capacity = bdtv.capacity;
        bdtv.Append(new BasicDateTime(37483940));
        bdtv.Append(new BasicDateTime(LocalDateTime.now()));
        assertEquals(capacity*2,bdtv.capacity);
        assertEquals(size+2,bdtv.size);
        System.out.println(bdtv.getString());
    }

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
    public void test_BasicDateTimeVector_toJSONString() throws Exception {
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateTimeVector v = new BasicDateTimeVector(list);
        String re = JSONObject.toJSONString(v);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[1,2,3],\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DATETIME\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDateTime\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1970.01.01T00:00:01,1970.01.01T00:00:02,1970.01.01T00:00:03]\",\"table\":false,\"unitLength\":4,\"values\":[1,2,3],\"vector\":true}", re);
    }
}
