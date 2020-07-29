package com.xxdb.data;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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
            assertEquals(datas[i],res.get(i).getNumber());

        }
        assertEquals(6,res.rows());
    }
}
