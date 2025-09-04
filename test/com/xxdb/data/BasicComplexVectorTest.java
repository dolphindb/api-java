package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class BasicComplexVectorTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_BasicComplexVector_list() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        list.add(null);
        BasicComplexVector bcv = new BasicComplexVector(list);
        assertEquals("5.6+6.5i",bcv.get(2).getString());
        assertEquals(new BasicComplex(5.6,6.5),bcv.get(2));
        assertEquals("[1.0+9.2i,5.6+6.5i,3.8+7.4i,5.6+6.5i]",bcv.getSubVector(new int[]{0,2,1,2}).getString());
        Map<String,Entity> map = new HashMap<>();
        map.put("complexVector",bcv);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.upload(map);
        BasicComplexVector bcv2 = (BasicComplexVector) conn.run("complexVector;");
        assertEquals(BasicComplex.class,bcv2.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.BINARY,bcv2.getDataCategory());
        assertFalse(bcv2.isNull(1));
        bcv2.setNull(3);
        assertTrue(bcv.isNull(3));
    }

    @Test
    public void test_BasicComplexVector_array(){
        Double2[] array = new Double2[]{null,new Double2(1.2,2.3),null,new Double2(3.4,4.5),new Double2(5.6,6.7)};
        BasicComplexVector bcv = new BasicComplexVector(array);
        Double2[] dou = bcv.getSubArray(new int[]{0,1,2,3,4});
        assertTrue(bcv.isNull(0));
        bcv.setComplex(2,3.9,6.2);
        assertFalse(bcv.isNull(2));
        System.out.println(bcv.hashBucket(1,1));
    }

    @Test
    public void test_BasicComplexVector_capacity_lt_size() throws Exception {
        BasicComplexVector bbv = new BasicComplexVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Double2(1.2,2.3));
        bbv.set(3, new Double2(-1.2,-2.3));
        bbv.set(4, new Double2(3.4,4.5));
        bbv.set(5, new Double2(-3.4,-4.5));
        Assert.assertEquals("[,,1.2+2.3i,-1.2-2.3i,3.4+4.5i,-3.4-4.5i]", bbv.getString());
    }

    @Test
    public void test_BasicComplexVector_size_capacity_set() throws Exception {
        BasicComplexVector bbv = new BasicComplexVector(6,6);
        Assert.assertEquals("[0.0+0.0i,0.0+0.0i,0.0+0.0i,0.0+0.0i,0.0+0.0i,0.0+0.0i]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Double2(1.2,2.3));
        bbv.set(3, new Double2(-1.2,-2.3));
        bbv.set(4, new Double2(3.4,4.5));
        bbv.set(5, new Double2(-3.4,-4.5));
        Assert.assertEquals("[,,1.2+2.3i,-1.2-2.3i,3.4+4.5i,-3.4-4.5i]", bbv.getString());
    }

    @Test
    public void test_BasicComplexVector_size_capacity_add() throws Exception {
        BasicComplexVector bbv = new BasicComplexVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add((Object)null);
        bbv.add(new Double2(1.2,2.3));
        bbv.add(new Double2(-1.2,-2.3));
        bbv.add(new Double2(3.4,4.5));
        bbv.add(new Double2(-3.4,-4.5));
        Assert.assertEquals("[,,1.2+2.3i,-1.2-2.3i,3.4+4.5i,-3.4-4.5i]", bbv.getString());
    }

    @Test
    public void test_BasicComplexVector_set_type_not_match() throws Exception {
        BasicComplexVector bbv = new BasicComplexVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Double2 or null is supported.", re);
    }

    @Test
    public void test_BasicComplexVector_add_type_not_match() throws Exception {
        BasicComplexVector bbv = new BasicComplexVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Double2 or null is supported.", re);
    }

    
    @Test
    public void test_BasicComplexVector_combine() {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0, 9.2));
        list.add(new Double2(3.8, 7.4));
        list.add(new Double2(5.6, 6.5));
        BasicComplexVector bcv = new BasicComplexVector(list);
        Double2[] array = new Double2[]{null,new Double2(1.2,2.3),null,new Double2(3.4,4.5),new Double2(5.6,6.7)};
        BasicComplexVector bcv2 = new BasicComplexVector(array);
        assertEquals("[1.0+9.2i,3.8+7.4i,5.6+6.5i,,1.2+2.3i,,3.4+4.5i,5.6+6.7i]",bcv.combine(bcv2).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicComplexVector_asof(){
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0, 9.2));
        list.add(new Double2(3.8, 7.4));
        list.add(new Double2(5.6, 6.5));
        BasicComplexVector bcv = new BasicComplexVector(list);
        bcv.asof(new BasicPoint(1.2,2.4));
    }

    @Test
    public void test_BasicComplexVector_write() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0, 9.2));
        list.add(new Double2(3.8, 7.4));
        list.add(new Double2(5.6, 6.5));
        BasicComplexVector bcv = new BasicComplexVector(list);
        ByteBuffer bb = bcv.writeVectorToBuffer(ByteBuffer.allocate(64));
        System.out.println(bb.remaining());
        String str = "[64, 34, 102, 102, 102, 102, 102, 102, 63, -16, " +
                "0, 0, 0, 0, 0, 0, 64, 29, -103, -103, -103, -103, -103, " +
                "-102, 64, 14, 102, 102, 102, 102, 102, 102, 64, 26, 0, 0, 0," +
                " 0, 0, 0, 64, 22, 102, 102, 102, 102, 102, 102, 0, 0, 0, 0, 0, " +
                "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]";
        assertEquals(str, Arrays.toString(bb.array()));
        ByteBuffer buffer = ByteBuffer.allocate(64);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer bb2 = bcv.writeVectorToBuffer(buffer);
        String str2 = "[0, 0, 0, 0, 0, 0, -16, 63, 102, 102, 102, " +
                "102, 102, 102, 34, 64, 102, 102, 102, 102, 102, 102, " +
                "14, 64, -102, -103, -103, -103, -103, -103, 29, 64, 102, " +
                "102, 102, 102, 102, 102, 22, 64, 0, 0, 0, 0, 0, 0, 26, 64," +
                " 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]";
        assertEquals(str2,Arrays.toString(bb2.array()));
    }

    @Test
    public void test_BasicComplexVector_serialize() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0, 9.2));
        list.add(new Double2(3.8, 7.4));
        list.add(new Double2(5.6, 6.5));
        BasicComplexVector bcv = new BasicComplexVector(list);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(3,1);
        assertEquals(48,bcv.serialize(0,0,3,numElementAndPartial,ByteBuffer.allocate(64)));
        ByteBuffer buffer = ByteBuffer.allocate(48);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(48,bcv.serialize(0,0,4,numElementAndPartial,buffer));
    }

    @Test
    public void test_BasicComplexVector_Append() throws Exception {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0, 9.2));
        list.add(new Double2(3.8, 7.4));
        list.add(new Double2(5.6, 6.5));
        BasicComplexVector bcv = new BasicComplexVector(list);
        int size = bcv.rows();
        bcv.Append(new BasicComplex(7.2,3.9));
        assertEquals(size+1,bcv.rows());
        bcv.Append(new BasicComplexVector(new Double2[]{new Double2(0.9,0.2),new Double2(1.5,2.2)}));
        assertEquals(size+3,bcv.rows());
        assertEquals("0.9+0.2i",bcv.get(4).getString());
    }

    @Test
    public void test_BasicComplexVector_toJSONString() {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        list.add(null);
        BasicComplexVector bcv = new BasicComplexVector(list);
        String re = JSONObject.toJSONString(bcv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[{\"null\":false,\"x\":1.0,\"y\":9.2},{\"null\":false,\"x\":3.8,\"y\":7.4},{\"null\":false,\"x\":5.6,\"y\":6.5},{\"null\":true,\"x\":-1.7976931348623157E308,\"y\":-1.7976931348623157E308}],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_COMPLEX\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicComplex\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[1.0+9.2i,3.8+7.4i,5.6+6.5i,]\",\"table\":false,\"unitLength\":16,\"values\":[{\"null\":false,\"x\":1.0,\"y\":9.2},{\"null\":false,\"x\":3.8,\"y\":7.4},{\"null\":false,\"x\":5.6,\"y\":6.5},{\"null\":true,\"x\":-1.7976931348623157E308,\"y\":-1.7976931348623157E308}],\"vector\":true}", re);
    }
}
