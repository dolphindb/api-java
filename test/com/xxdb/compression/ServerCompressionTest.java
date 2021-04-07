package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ServerCompressionTest {

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
    public void testCompressDelta() throws IOException {

        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int[] val = new int[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = rand.nextInt();
        }


        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicIntVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("share table(100:0,`tDate`tint,[DATE,INT]) as tb");
        BasicInt count = (BasicInt) conn.run("tableInsert{tb}", args);
        System.out.println(count.getInt());
//        BasicTable newT = (BasicTable) conn.run("tb");

    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}
