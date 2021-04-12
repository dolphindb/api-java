package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LZ4Test {

    private DBConnection conn;
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8848;

    @Before
    public void setUp() {
        conn = new DBConnection(false, false, true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testString() {
        BasicTable obj = null;
        try {
            obj = (BasicTable) conn.run("table(second(1..10240) as id, take(`hi`hello`hola, 10240) as value)");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < obj.rows(); i++) {
            System.out.println(obj.getRowJson(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }

}
