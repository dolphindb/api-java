package com.xxdb.data;

import org.junit.Assert;
import org.junit.Test;

public class BasicStringTest {
    @Test
    public void test_getHash(){
        BasicString s = new BasicString("71da5e02-5c77-4cf1-a0d2-fb14f660d12a");
        int re = s.hashBucket(10);
        Assert.assertTrue(re<10);
    }
}
