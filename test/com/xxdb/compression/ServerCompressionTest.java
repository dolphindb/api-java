package com.xxdb.compression;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

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
    public void testCompressLong() throws Exception {
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        long[] val = new long[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            if (i % 2 == 0)
                val[i] = Integer.MIN_VALUE - 300L;
            else
                val[i] = Integer.MIN_VALUE + 300L;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicLongVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,LONG])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressDouble() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        double[] val = new double[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            if (i % 2 == 0)
                val[i] = i;
            else
                val[i] = -i;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicDoubleVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,DOUBLE])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressInt() throws Exception {
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int[] val = new int[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            if (i % 2 == 0)
                val[i] = i;
            else
                val[i] = -i;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicIntVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,INT])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
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
            val[i] = 100000-i;
        }


        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicIntVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,INT])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
    }

    @Test
    public void testCompress() throws Exception {

        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        double[] val = new double[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = 3.5;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicDoubleVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,DOUBLE])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());


//        BasicAnyVector sizeV = (BasicAnyVector) conn.run("decompressTable", args);
//        for (int i = 1; i < sizeV.rows(); i++) {
//            BasicLongVector v = (BasicLongVector) sizeV.getEntity(i);
//            for (int j = 0; j < v.rows(); j++) {
////                if (i == 0)
////                    assertEquals(i + "," + j, time[j], v.getInt(j));
//                if (i == 1)
//                    assertEquals(i + "," + j, val[j], v.getLong(j));
//            }
//        }

        BasicDoubleVector my = (BasicDoubleVector) conn.run("readTable", args);
        System.out.println("-------------------------------------");
        System.out.println(my.rows());
        System.out.println(my.getDouble(0));

//        BasicByteVector my = (BasicByteVector) conn.run("readTable", args);
////        BasicByteVector server = (BasicByteVector) conn.run("compress(take(date(2000.01.01..2000.01.15), 100000), 'lz4')");
//        BasicByteVector server = (BasicByteVector) conn.run("compress(take([3.5],100000),'delta')");
//        assertEquals("rows different", server.rows(), my.rows());
//        compareByteVector(my, server);
    }


    public static void compareBasicTable(BasicTable table, BasicTable newTable) throws Exception {
        assertEquals("rows not equal", table.rows(), newTable.rows());
        assertEquals("cols not equal", table.columns(), newTable.columns());
        int count = 0;
        int rows = table.rows();
        int cols = table.columns();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                Entity e1 = table.getColumn(j).get(i);
                Entity e2 = newTable.getColumn(j).get(i);
                if (!e1.getString().equals(e2.getString())) {
                    if (count < 1000)
                        System.out.println(i + "," + j + ": " + " expected: " + e1.getString() + " actual: " + e2.getString());
                    count++;
                }
            }
        }
        assertEquals("Total unmatched values found", 0, count);
    }

    public static void compareByteVector(BasicByteVector my, BasicByteVector server) {
        System.out.println(my.rows());
        for (int i = 0; i < my.rows(); i++) {
            System.out.println(my.getByte(i));
            if (my.getByte(i) != server.getByte(i))
                System.out.println(i + ": " + " expected: " + my.getByte(i) + " actual: " + server.getByte(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}
