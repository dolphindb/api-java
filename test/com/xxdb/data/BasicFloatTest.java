package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.DBConnectionTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BasicFloatTest {

    @Test
    public void TestCombineFloatVector() throws Exception {
        Float[] data = {1.1f,0.0f,1.3f};
        BasicFloatVector v = new BasicFloatVector(Arrays.asList(data));
        Float[] data2 = {1.1f,0.0f,1.3f};
        BasicFloatVector vector2 = new BasicFloatVector( Arrays.asList(data2));
        BasicFloatVector res= (BasicFloatVector) v.combine(vector2);
        Float[] datas = {1.1f,0.0f,1.3f,1.1f,0.0f,1.3f};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],res.get(i).getNumber());

        }
        assertEquals(6,res.rows());
    }
}
