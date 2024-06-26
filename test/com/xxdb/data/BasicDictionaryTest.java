package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.*;

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
    public void test_BasicDictionary_getString_DT_ANY() throws IOException {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_INT, Entity.DATA_TYPE.DT_ANY,1);
        conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity[] arr = new Entity[10];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }

        bd.put(new BasicInt(1),new BasicAnyVector(arr,true));
        assertEquals("1->(0,1,2,3,4,5,6,7,8,9)\n",bd.getString());
    }
    @Test
    public void test_BasicDictionary_upload() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_INT, Entity.DATA_TYPE.DT_ANY);
        Map<String,Entity> data = new HashMap<>();
        data.put("bd",bd);
        conn.upload(data);
        Dictionary re= (Dictionary) conn.run("bd");
        System.out.println(re.getString());
        assertEquals("", re.getString());
        Entity[] arr = new Entity[10];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }

        bd.put(new BasicInt(1),new BasicAnyVector(arr,true));
        data.put("bd",bd);
        conn.upload(data);
        Dictionary re1= (Dictionary) conn.run("bd");
        System.out.println(re1.getString());
        assertEquals("1->(0,1,2,3,4,5,6,7,8,9)", re1.getString());
    }
    @Test
    public void test_BasicDictionary_valueType_DT_ANY() throws IOException {
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_INT, Entity.DATA_TYPE.DT_ANY,1);
        conn = new DBConnection();
        conn.connect(HOST,PORT);
        bd.put(new BasicInt(1),new BasicInt(1));
        assertEquals("1->1\n",bd.getString());
        bd.put(new BasicInt(1),new BasicString("1121!@#$%^&*()_+-=`~{}[]|\":;',.ldfdf中文"));
        assertEquals("1->1121!@#$%^&*()_+-=`~{}[]|\":;',.ldfdf中文\n",bd.getString());
        bd.put(new BasicInt(1),new BasicDouble(1.666));
        assertEquals("1->1.666\n",bd.getString());
    }

    @Test(expected = IOException.class)
    public void test_BasicDictionary_Exception() throws IOException {
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return null;
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 192;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 0;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_TIME,in);
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
