package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicPointTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_BasicPoint(){
        BasicPoint bp = new BasicPoint(13.4,55.8);
        assertEquals(13.4,bp.getDouble2().x,0);
        assertEquals(55.8,bp.getDouble2().y,0);
        assertEquals(Entity.DATA_CATEGORY.BINARY,bp.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_POINT,bp.getDataType());
        assertEquals(-1429503999,bp.hashCode());
        assertEquals(-1,bp.hashBucket(1));
        assertEquals("\"(13.4, 55.8)\"",bp.getJsonString());
        bp.setNull();
        assertEquals("(,)",bp.getString());
        assertFalse(bp.equals(new BasicInt(7)));
    }

    @Test(expected = Exception.class)
    public void test_BasicPoint_getNumber() throws Exception {
        new BasicPoint(3,7).getNumber();
    }

    @Test(expected = Exception.class)
    public void test_BasicPoint_getTemporal() throws Exception {
        new BasicPoint(1,1).getTemporal();
    }

    @Test
    public void test_BasicPointVector_list(){
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(null);
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv = new BasicPointVector(list);
        assertEquals("[(3.8, 7.4),(5.6, 6.5),(1.0, 9.2)]",bpv.getSubVector(new int[]{1,3,0}).getString());
        assertTrue(bpv.isNull(2));
        assertEquals(new BasicPoint(1.0,9.2),bpv.get(0));
        assertEquals(Entity.DATA_CATEGORY.BINARY,bpv.getDataCategory());
        assertEquals(BasicPoint.class,bpv.getElementClass());
    }

    @Test
    public void test_BasicPointVector_combine(){
        Double2[] arr = new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),new Double2(5.6,6.5)};
        BasicPointVector bpv = new BasicPointVector(arr);
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        assertEquals(bpv.combine(bpv2).getString(), bpv2.combine(bpv).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicPointVector_asof(){
        Double2[] arr = new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),new Double2(5.6,6.5)};
        BasicPointVector bpv = new BasicPointVector(arr);
        bpv.asof(new BasicPoint(1.7,8.9));
    }

    @Test
    public void test_BasicPointVector_serialize() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        ByteBuffer bb1 = ByteBuffer.allocate(48);
        bb1.order(ByteOrder.BIG_ENDIAN);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(3,1);
        assertEquals(48,bpv2.serialize(0,0,3,numElementAndPartial,bb1));
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(48,bpv2.serialize(0,0,3,numElementAndPartial,bb));
        assertNotEquals(Arrays.toString(bb1.array()),Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicPointVector_wvtb() throws IOException {
        List<Double2> list = new ArrayList<>();
        list.add(new Double2(1.0,9.2));
        list.add(new Double2(3.8,7.4));
        list.add(new Double2(5.6,6.5));
        BasicPointVector bpv2 = new BasicPointVector(list);
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer bo = bpv2.writeVectorToBuffer(bb);
        ByteBuffer bb1 = ByteBuffer.allocate(48);
        bb1.order(ByteOrder.BIG_ENDIAN);
        ByteBuffer bo1 = bpv2.writeVectorToBuffer(bb1);
        assertNotEquals(Arrays.toString(bo.array()),Arrays.toString(bo1.array()));
    }

    @Test
    public void test_BasicPoint_toJSONString() throws Exception {
        BasicPoint bp = new BasicPoint(1,1);
        String re = JSONObject.toJSONString(bp);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_POINT\",\"dictionary\":false,\"double2\":{\"null\":false,\"x\":1.0,\"y\":1.0},\"jsonString\":\"\\\"(1.0, 1.0)\\\"\",\"matrix\":false,\"null\":false,\"pair\":false,\"scalar\":true,\"string\":\"(1.0, 1.0)\",\"table\":false,\"vector\":false,\"x\":1.0,\"y\":1.0}", re);
    }
    @Test
    public void test_BasicPointVector_toJSONString() throws Exception {
        BasicPointVector bpv = new BasicPointVector(new Double2[]{});
        String re = JSONObject.toJSONString(bpv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[],\"dataCategory\":\"BINARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_POINT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicPoint\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[]\",\"table\":false,\"unitLength\":16,\"vector\":true}", re);
    }
}
