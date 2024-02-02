package com.xxdb.io;

import com.xxdb.data.BasicBoolean;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ResourceBundle;


public class Long2Test {
    @Test
    public void test_Long2_equals() throws IOException {
        Long2  long2 = new Long2(1,1);
        Assert.assertTrue(long2.equals(long2));
        Assert.assertFalse(long2.equals(new BasicBoolean(true)));
        Assert.assertFalse(long2.equals(null));
        Assert.assertFalse(long2.equals(new Long2(0,0)));
        Assert.assertFalse(long2.equals(new Long2(1,2)));
        Assert.assertFalse(long2.equals(new Long2(2,1)));
        Assert.assertFalse(long2.equals(new Long2(0,1)));
        Assert.assertFalse(long2.equals(new Long2(1,0)));
    }
}
