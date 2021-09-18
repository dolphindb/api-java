package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BasicArrayVectorTest {
    @Test
    public void TestBasicIntArrayVector() throws Exception {
         DBConnection conn = new DBConnection();
         conn.connect("localhost", 8848);
         Entity obj = conn.run("a = array(INT[], 0, 20)\n" +
                 "for(i in 1..20){\n" +
                 "\ta.append!([1..100])\n" +
                 "};a");
    }
}
