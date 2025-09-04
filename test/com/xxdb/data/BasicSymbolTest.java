package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;
public class BasicSymbolTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(HOST,PORT,"admin","123456")){
                throw new IOException("Failed to connect to 2xdb server");
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    @Test(expected = IOException.class)
    public void test_BasicSymbolEntity() throws IOException {
        BasicSystemEntity bse = new BasicSystemEntity(new LittleEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }), Entity.DATA_TYPE.DT_ANY);
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,bse.getDataCategory());
        bse.writeScalarToOutputStream(new LittleEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        }));
    }

    @Test(timeout=120000)
    public void test_Symbol_getDataType() throws IOException {
        Entity re1 = conn.run("symbol([concat(take(`aaaaaaaaa,1)),concat(take(`aaaaaaaaa,1)),concat(take(`aaaaaaaaa,1)),concat(take(`aaaaaaaaa,1))])");
        assertEquals("SYMBOL",re1.getDataType().getName());
    }
}
