package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.streaming.client.BasicMessage;
import org.junit.Test;

import java.util.HashMap;
import static org.junit.Assert.*;
public class BasicMessageTest {

    @Test
    public void test_BasicMessage() throws Exception {
        HashMap<String,Integer> map = new HashMap<>();
        map.put("dolphindb",1);
        map.put("mongodb",2);
        map.put("gaussdb",3);
        map.put("goldendb",4);
        BasicAnyVector bav = new BasicAnyVector(4);
        bav.set(0,new BasicInt(5));
        bav.set(1,new BasicPoint(6.4,9.2));
        bav.set(2,new BasicString("China"));
        bav.set(3,new BasicDouble(15.48));
        System.out.println(bav.getString());
        BasicMessage bm = new BasicMessage(0L,"first",bav,map);
        assertEquals("(6.4, 9.2)",bm.getValue(1).toString());
        assertEquals("China",bm.getValue("mongoDB").toString());
    }

}
