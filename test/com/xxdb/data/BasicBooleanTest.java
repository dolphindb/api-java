package com.xxdb.data;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BasicBooleanTest {
    @Test
    public void TestCombineBooleanVector() throws Exception {
        BasicBooleanVector v = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector vector2 = new BasicBooleanVector(Arrays.asList(new Byte[]{0,1}));
        BasicBooleanVector res= (BasicBooleanVector) v.combine(vector2);
        BasicBooleanVector datas=  new BasicBooleanVector(Arrays.asList(new Byte[]{0,1,0,1}));
        for (int i=0;i<res.rows();i++){
            assertEquals(datas.get(i).getNumber(),res.get(i).getNumber());

        }
        assertEquals(4,res.rows());
    }
}
