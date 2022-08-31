package com.xxdb.data;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BasicDoubleTest {

    @Test
    public void TestCombineDoubleVector() throws Exception {
        Double[] data = {1.1,0.0,1.3};
        BasicDoubleVector v = new BasicDoubleVector(Arrays.asList(data));
        Double[] data2 = {1.1,0.0,1.3};
        BasicDoubleVector vector2 = new BasicDoubleVector( Arrays.asList(data2));
        BasicDoubleVector res= (BasicDoubleVector) v.combine(vector2);
        Double[] datas = {1.1,0.0,1.3,1.1,0.0,1.3};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(6,res.rows());
    }

    @Test
    public void testBasicDoubleMatrix_function(){
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(2,2);
        bdm.setDouble(0,0,2.4);
        bdm.setDouble(0,1,5.2);
        bdm.setDouble(1,0,2.7);
        bdm.setDouble(1,1,3.8);
        assertEquals(3.8,bdm.getDouble(1,1),0);
        assertFalse(bdm.isNull(1,0));
        bdm.setNull(1,0);
        assertTrue(bdm.isNull(1,0));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bdm.getDataCategory());
        assertEquals(BasicDouble.class,bdm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicDoubleMatrix_listnull() throws Exception {
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(5,5,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicDoubleMatrix_error() throws Exception {
        List<double[]> list = new ArrayList<>();
        list.add(new double[]{8.2,8.5});
        list.add(new double[]{7.1,7.16});
        BasicDoubleMatrix bdm = new BasicDoubleMatrix(3,2,list);
    }

    @Test
    public void test_BasicDoubleVector_list(){
        List<Double> list = new ArrayList<>();
        list.add(8.14);
        list.add(8.13);
        list.add(null);
        list.add(8.18);
        BasicDoubleVector bdv = new BasicDoubleVector(list);
        assertTrue(bdv.isNull(2));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,bdv.getDataCategory());
        assertEquals(BasicDouble.class,bdv.getElementClass());
        assertEquals("[8.13,8.18,8.14]",bdv.getSubVector(new int[]{1,3,0}).getString());
    }

    @Test
    public void test_BasicDoubleVector_array() throws IOException {
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        ByteBuffer bb = bdv.writeVectorToBuffer(ByteBuffer.allocate(32));
        assertEquals("[64, 24, -52, -52, -52, -52, -52, -51, 64, 25, 30, " +
                "-72, 81, -21, -123, 31, 64, 29, 10, 61, 112, -93, -41, 10, 0, 0, " +
                "0, 0, 0, 0, 0, 0]",Arrays.toString(bb.array()));
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicDoubleVector_asof(){
        double[] arr = new double[]{6.20,6.28,7.26};
        BasicDoubleVector bdv = new BasicDoubleVector(arr,false);
        assertEquals(-1,bdv.asof(new BasicDouble(5.7)));
        assertEquals(0,bdv.asof(new BasicDouble(6.25)));
        bdv.asof(new BasicComplex(2.1,4.2));
    }

    @Test
    public void test_BasicDouble_get() throws Exception {
        BasicDouble bd = new BasicDouble(5.73);
        assertFalse(bd.equals(new BasicBoolean(true)));
        assertNull(new BasicDouble(-Double.MAX_VALUE).getNumber());
        assertEquals("1E-6",new BasicDouble(0.000001).getString());
        assertEquals("1E6",new BasicDouble(1000000.0).getString());
        assertEquals("1E6",new BasicDouble(1000000.0).getJsonString());
        assertEquals("Infinity",new BasicDouble(Double.POSITIVE_INFINITY).getString());
    }


}
