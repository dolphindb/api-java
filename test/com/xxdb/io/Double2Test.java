package com.xxdb.io;

import com.xxdb.data.BasicBoolean;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ResourceBundle;


public class Double2Test {
    @Test
    public void test_Double2_isNull() throws IOException {
        Double2  double2 = new Double2(1,1);
        Assert.assertFalse(double2.isNull());
        Double2  double2_1 = new Double2(1,-Double.MAX_VALUE);
        Assert.assertTrue(double2_1.isNull());
        Double2  double2_2 = new Double2(-Double.MAX_VALUE,1);
        Assert.assertTrue(double2_2.isNull());
        Double2  double2_3 = new Double2(-Double.MAX_VALUE,-Double.MAX_VALUE);
        Assert.assertTrue(double2_3.isNull());
    }
    @Test
    public void test_Double2_equals() throws IOException {
        Double2  double2 = new Double2(1,1);
        Assert.assertTrue(double2.equals(double2));
        Assert.assertFalse(double2.equals(new BasicBoolean(true)));
        Assert.assertFalse(double2.equals(null));
        Assert.assertFalse(double2.equals(new Double2(-Double.MAX_VALUE,-Double.MAX_VALUE)));
        Assert.assertFalse(double2.equals(new Double2(1,2)));
        Assert.assertFalse(double2.equals(new Double2(2,1)));
        Assert.assertFalse(double2.equals(new Double2(-Double.MAX_VALUE,1)));
        Assert.assertFalse(double2.equals(new Double2(1,-Double.MAX_VALUE)));
    }
}
