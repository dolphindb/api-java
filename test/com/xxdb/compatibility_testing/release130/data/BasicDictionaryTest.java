package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicDictionaryTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    DBConnection conn;

    @Test(expected = IllegalArgumentException.class)
    public void test_BasicDictionary_error(){
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_DICTIONARY, Entity.DATA_TYPE.DT_TIMESTAMP);
    }

    @Test
    public void test_BasicDictionary_function() throws IOException {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DATETIME,2);
        BasicString bs1 = new BasicString("MSFT");
        BasicDateTime bdt1 = new BasicDateTime(LocalDateTime.of(2022,8,10,17,36));
        assertTrue(bd.put(bs1,bdt1));
        BasicString bs2 = new BasicString("APPL");
        BasicDateTime bdt2 = new BasicDateTime(LocalDateTime.MIN);
        assertTrue(bd.put(bs2,bdt2));
        assertFalse(bd.put(new BasicInt(5),new BasicDate(new GregorianCalendar())));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bd.getDataCategory());
        assertEquals(1,bd.columns());
        assertEquals("2022.08.10T17:36:00",bd.get("MSFT").getString());
        assertEquals("[MSFT, APPL]",bd.keys().toString());
        assertEquals("[2022.08.10T17:36:00, 1982.02.08T12:37:20]",bd.values().toString());
        assertEquals("[MSFT=2022.08.10T17:36:00, APPL=1982.02.08T12:37:20]",bd.entrySet().toString());
    }

    @Test(expected = IOException.class)
    public void test_BasicDictionary_write_error() throws IOException {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_SYMBOL,Entity.DATA_TYPE.DT_DICTIONARY);
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bd.write(out);
    }
    @Test
    public void test_BasicDictionary_getString_bigcount() {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_INT, Entity.DATA_TYPE.DT_INT,25);
        for (int i = 0; i < 25; i++) {
            bd.put(new BasicInt(i + 1), new BasicInt(i));
        }
        assertTrue(bd.getString().contains("..."));
        System.out.println(bd.getString());
    }

    @Test
    public void test_BasicDictionary_toJSONString() throws IOException {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DATETIME,2);
        BasicString bs1 = new BasicString("MSFT");
        BasicDateTime bdt1 = new BasicDateTime(LocalDateTime.of(2022,8,10,17,36));
        assertTrue(bd.put(bs1,bdt1));
        String re = JSONObject.toJSONString(bd);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"TEMPORAL\",\"dataForm\":\"DF_DICTIONARY\",\"dataType\":\"DT_DATETIME\",\"dictionary\":true,\"keyDataType\":\"DT_STRING\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"{MSFT}->{2022.08.10T17:36:00}\",\"table\":false,\"vector\":false}", re);
    }

}
