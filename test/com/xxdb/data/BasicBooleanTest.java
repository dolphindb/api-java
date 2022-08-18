package com.xxdb.data;

import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BasicBooleanTest {
    @Test
    public void TestCombineBooleanVector() throws Exception {
        BasicBooleanVector v = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector vector2 = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector res= (BasicBooleanVector) v.combine(vector2);
        BasicBooleanVector data=  new BasicBooleanVector(Arrays.asList(new Byte[]{0,1,0,1}));
        AbstractVector.Offect offect = new AbstractVector.Offect(1);
        assertEquals(-1,v.hashBucket(0,1));
        for (int i=0;i<res.rows();i++){
            assertEquals(data.get(i).getNumber(),res.get(i).getNumber());

        }
        assertEquals(4,res.rows());
    }

    @Test
    public void test_BasicBoolean() throws Exception {
        BasicBoolean bb = new BasicBoolean(Byte.MIN_VALUE);
        assertEquals(1,bb.columns(),0);
        assertEquals(1,bb.rows());
        assertNull(bb.getNumber());
        assertFalse(bb.equals(3));
    }

    @Test(expected = Exception.class)
    public void test_BasicBooleanMatrix_null() throws Exception {
        BasicBooleanMatrix bbm = new BasicBooleanMatrix(2,2);
        bbm.setBoolean(0,1,false);
        bbm.setBoolean(0,0,true);
        bbm.setBoolean(1,0,false);
        bbm.setBoolean(1,1,true);
        System.out.println(bbm.getColumnLabel(1).getString());
        System.out.println(bbm.getColumnLabels());
        System.out.println(bbm.getRowLabels());
        assertFalse(bbm.getBoolean(1,0));
        assertFalse(bbm.isNull(1,1));
        assertEquals("true",bbm.get(0,0).getString());
        bbm.setNull(1,1);
        assertTrue(bbm.isNull(1,1));
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,bbm.getDataCategory());
        assertEquals(BasicBoolean.class,bbm.getElementClass());
        List<byte[]> listofarrays = new ArrayList<>();
        listofarrays.add(new byte[]{0,1,Byte.MIN_VALUE});
        listofarrays.add(new byte[]{Byte.MAX_VALUE,1,0});
        BasicBooleanMatrix bbm2 = new BasicBooleanMatrix(3,2,listofarrays);
        assertTrue(bbm2.isNull(2,0));
        BasicBooleanMatrix bbm3 = new BasicBooleanMatrix(1,1, null);
    }

    @Test(expected = Exception.class)
    public void test_BasicBooleanMatrix_lessRows() throws Exception {
        List<byte[]> listofarrays = new ArrayList<>();
        listofarrays.add(new byte[]{0,1,Byte.MIN_VALUE});
        listofarrays.add(new byte[]{Byte.MAX_VALUE,1,0});
        BasicBooleanMatrix bbm = new BasicBooleanMatrix(4,2,listofarrays);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicBooleanVector(){
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 1);
        list.add(1,(byte) 1);
        list.add(2,null);
        list.add(3,Byte.MAX_VALUE);
        list.add(4,(byte) 0);
        list.add(5,(byte) 0);
        BasicBooleanVector bbv = new BasicBooleanVector(list);
        assertTrue(bbv.isNull(2));
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
    public void test_basicBooleanVector_convert() throws IOException {
        BasicBooleanVector bbv = new BasicBooleanVector(new boolean[]{false,true,true,false,true,false});
        ByteBuffer bb = bbv.writeVectorToBuffer(ByteBuffer.allocate(10));
        assertEquals((byte)1,bb.get(2));
    }

    @Test
    public void test_basicBooleanVector_wvtb() throws IOException {
        List<Byte> list = new ArrayList<>();
        list.add(0,(byte) 1);
        list.add(1,(byte) 1);
        list.add(2,null);
        list.add(3,Byte.MAX_VALUE);
        list.add(4,(byte) 0);
        list.add(5,(byte) 0);
        BasicBooleanVector bbv = new BasicBooleanVector(list);
        ByteBuffer bb = bbv.writeVectorToBuffer(ByteBuffer.allocate(6));
        assertEquals("[1, 1, -128, 127, 0, 0]",Arrays.toString(bb.array()));
    }
}
