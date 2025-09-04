package com.xxdb.data;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class BasicBooleanVectorTest {

    @Test
    public void TestCombineBooleanVector() throws Exception {
        BasicBooleanVector v = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector vector2 = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector res= (BasicBooleanVector) v.combine(vector2);
        BasicBooleanVector data=  new BasicBooleanVector(Arrays.asList(new Byte[]{0,1,0,1}));
        AbstractVector.Offect offect = new AbstractVector.Offect(1);
        assertEquals(-1,v.hashBucket(0,1));
        for (int i=0;i<res.rows();i++){
            assertEquals(((Scalar)data.get(i)).getNumber(),((Scalar)res.get(i)).getNumber());

        }
        assertEquals(4,res.rows());
    }

    @Test
    public void test_BasicBooleanVector_basic(){
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 1);
        list.add(1,(byte) 1);
        list.add(2,null);
        list.add(3,Byte.MAX_VALUE);
        list.add(4,(byte) 0);
        list.add(5,(byte) 0);
        BasicBooleanVector bbv = new BasicBooleanVector(list);
        assertTrue(bbv.isNull(2));
        assertEquals(Entity.DATA_TYPE.DT_BOOL,bbv.get(2).getDataType());
        assertEquals("[true,false,false,true]",bbv.getSubVector(new int[]{0,5,4,1}).getString());
        assertFalse(bbv.getBoolean(4));
        bbv.setBoolean(2,false);
        assertEquals("false",bbv.get(2).getString());
        bbv.setNull(4);
        assertTrue(bbv.isNull(4));
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,bbv.getDataCategory());
        assertEquals(BasicBoolean.class,bbv.getElementClass());
        Scalar value = new BasicPoint(8.2,7.4);
        bbv.asof(value);
    }

    @Test
    public void test_BasicBooleanVector_capacity_lt_size() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(6,1);
        bbv.set(0, (Object)null);
        bbv.set(2, new Boolean(true));
        bbv.set(3, new Boolean(false));
        bbv.set(4, true);
        bbv.set(5, false);
        Assert.assertEquals("[,false,true,false,true,false]", bbv.getString());
    }

    @Test
    public void test_BasicBooleanVector_size_capacity_set() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(6,6);
        Assert.assertEquals("[false,false,false,false,false,false]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new Boolean(true));
        bbv.set(3, new Boolean(false));
        bbv.set(4, true);
        bbv.set(5, false);
        Assert.assertEquals("[,,true,false,true,false]", bbv.getString());
    }

    @Test
    public void test_BasicBooleanVector_size_capacity_add() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(0,6);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add( (Object)null);
        bbv.add( null);
        bbv.add( new Boolean(true));
        bbv.add( new Boolean(false));
        bbv.add( true);
        bbv.add( false);
        Assert.assertEquals("[,,true,false,true,false]", bbv.getString());
    }

    @Test
    public void test_BasicBooleanVector_set_type_not_match() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Boolean or null is supported.", re);
    }

    @Test
    public void test_BasicBooleanVector_add_type_not_match() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(1,1);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only Boolean or null is supported.", re);
    }

    @Test
    public void test_BasicBooleanVector_convert() throws IOException {
        BasicBooleanVector bbv = new BasicBooleanVector(new boolean[]{false,true,true,false,true,false});
        ByteBuffer bb = bbv.writeVectorToBuffer(ByteBuffer.allocate(10));
        assertEquals((byte)1,bb.get(2));
    }

    @Test
    public void test_BasicBooleanVector_wvtb() throws IOException {
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 1);
        list.add(1,(byte) 1);
        list.add(2,null);
        list.add(3,Byte.MAX_VALUE);
        list.add(4,(byte) 0);
        list.add(5,(byte) 0);
        BasicBooleanVector bbv = new BasicBooleanVector(list);
        ByteBuffer bb = bbv.writeVectorToBuffer(ByteBuffer.allocate(6));
        assertEquals("[1, 1, -128, 127, 0, 0]", Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicBooleanVector_Append() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(new boolean[]{true,false,false,true});
        int size = bbv.rows();
        bbv.Append(new BasicBoolean(false));
        assertEquals(size+1,bbv.rows());
        bbv.Append(new BasicBooleanVector(new boolean[]{true,true,false}));
        assertEquals(size+4,bbv.rows());
    }

    @Test
    public void test_BasicBooleanVector_asof(){
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 1);
        BasicBooleanVector bbv = new BasicBooleanVector(list);
        Scalar value = new BasicPoint(8.2,7.4);
        String re = null;
        try{
            bbv.asof(value);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("BasicBooleanVector.asof not supported.", re);
    }
}
