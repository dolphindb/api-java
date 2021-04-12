package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ServerCompressionTest {

    Random rand = new Random();
    private DBConnection conn;
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8848;
    int testPoints = 600000;

    BasicShortVector shortVector;
    BasicDateVector dateVector;
    BasicMinuteVector minuteVector;
    BasicSecondVector secondVector;
    BasicTimestampVector timestampVector;
    BasicNanoTimestampVector nanoTimestampVector;
    BasicFloatVector floatVector;
    BasicDoubleVector doubleVector;

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

    @Before
    public void prepareData() {
        short[] shorts = new short[testPoints];
        int[] date = new int[testPoints];
        int[] minute = new int[testPoints];
        int[] second = new int[testPoints];
        long[] timestamp = new long[testPoints];
        long[] nanotimestamp = new long[testPoints];
        float[] floats = new float[testPoints];
        double[] doubles = new double[testPoints];

        int baseDate = Utils.countDays(2000,1,1);
        int baseMinute = Utils.countMinutes(13, 8);
        int baseSecond = Utils.countSeconds(4, 5, 23);
        long basicTimestamp = Utils.countMilliseconds(2013,1,25,10,0,0,1);
        long basicNanoTimestamp = Utils.countNanoseconds(LocalTime.now());
        for (int i = 0; i < 100000; i++) {
            if (i % 2 == 0) {
                shorts[i] = (short) i;
                date[i] = baseDate + (i % 15);
                minute[i] = baseMinute + ( i % 300);
                second[i] = baseSecond + (i % 1800);
                timestamp[i] = basicTimestamp - (i % 5000);
                nanotimestamp[i] = basicNanoTimestamp + (i % 36000);
                floats[i] = i * 3.765f;
                doubles[i] = i * 3.141592657;
            } else {
                shorts[i] = (short) -i;
                date[i] = baseDate - (i % 15);
                minute[i] = baseMinute - ( i % 300);
                second[i] = baseSecond - (i % 1800);
                timestamp[i] = basicTimestamp + (i % 5000);
                nanotimestamp[i] = basicNanoTimestamp - (i % 36000);
                floats[i] = -i * 3.765f;
                doubles[i] = -i * 3.141592657;
            }
        }

        shortVector = new BasicShortVector(shorts);
        dateVector = new BasicDateVector(date);
        minuteVector = new BasicMinuteVector(minute);
        secondVector = new BasicSecondVector(second);
        timestampVector = new BasicTimestampVector(timestamp);
        nanoTimestampVector = new BasicNanoTimestampVector(nanotimestamp);
        floatVector = new BasicFloatVector(floats);
        doubleVector = new BasicDoubleVector(doubles);
    }

    @Test
    public void testCompressLZ4() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("short");
        colNames.add("date");
        colNames.add("minute");
        colNames.add("second");
        colNames.add("timestamp");
        colNames.add("nanotimestamp");
        colNames.add("float");
        colNames.add("double");

        shortVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        dateVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        minuteVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        secondVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        timestampVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        nanoTimestampVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        floatVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        doubleVector.setCompressedMethod(Vector.COMPRESS_LZ4);

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(shortVector);
        colVectors.add(dateVector);
        colVectors.add(minuteVector);
        colVectors.add(secondVector);
        colVectors.add(timestampVector);
        colVectors.add(nanoTimestampVector);
        colVectors.add(floatVector);
        colVectors.add(doubleVector);

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`short`date`minute`second`timestamp`nanotimestamp`float`double,[SHORT,DATE,MINUTE,SECOND,TIMESTAMP,NANOTIMESTAMP,FLOAT,DOUBLE])" +
                "share t as st");
        long startTs = System.currentTimeMillis();
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        System.out.println("LZ4 " + testPoints + "x" + table.columns()  + ": " + (System.currentTimeMillis() - startTs) + "ms");
        assertEquals(testPoints, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressDelta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("short");
        colNames.add("date");
        colNames.add("minute");
        colNames.add("second");
        colNames.add("timestamp");
        colNames.add("nanotimestamp");

        shortVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        dateVector.setCompressedMethod(Vector.COMPRESS_LZ4);
        minuteVector.setCompressedMethod(Vector.COMPRESS_DELTA);
        secondVector.setCompressedMethod(Vector.COMPRESS_DELTA);
        timestampVector.setCompressedMethod(Vector.COMPRESS_DELTA);
        nanoTimestampVector.setCompressedMethod(Vector.COMPRESS_DELTA);


        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(shortVector);
        colVectors.add(dateVector);
        colVectors.add(minuteVector);
        colVectors.add(secondVector);
        colVectors.add(timestampVector);
        colVectors.add(nanoTimestampVector);

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`short`date`minute`second`timestamp`nanotimestamp,[SHORT,DATE,MINUTE,SECOND,TIMESTAMP,NANOTIMESTAMP])" +
                "share t as st");
        long startTs = System.currentTimeMillis();
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        System.out.println("Delta of Delta " + testPoints + "x" + table.columns() + ": " + (System.currentTimeMillis() - startTs) + "ms");
        assertEquals(testPoints, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressTime() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("minute");
        colNames.add("second");
        colNames.add("timestamp");
        int[] date = new int[100000];
        int[] minute = new int[100000];
        int[] second = new int[100000];
        long[] timestamp = new long[100000];

        int baseDate = Utils.countDays(2000,1,1);
        int baseMinute = Utils.countMinutes(13, 8);
        int baseSecond = Utils.countSeconds(4, 5, 23);
        long basicTimestamp = Utils.countMilliseconds(2013,1,25,10,0,0,1);
        for (int i = 0; i < 100000; i++) {
            if (i % 2 == 0) {
                date[i] = baseDate + (i % 15);
                minute[i] = baseMinute + ( i % 300);
                second[i] = baseSecond + (i % 1800);
                timestamp[i] = basicTimestamp - (i % 5000);
            } else {
                date[i] = baseDate - (i % 15);
                minute[i] = baseMinute - ( i % 300);
                second[i] = baseSecond - (i % 1800);
                timestamp[i] = basicTimestamp + (i % 5000);
            }
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(date));
        colVectors.add(new BasicMinuteVector(minute));
        colVectors.add(new BasicSecondVector(second));
        colVectors.add(new BasicTimestampVector(timestamp));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`minute`second`timestamp,[DATE,MINUTE,SECOND,TIMESTAMP])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressLong() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[testPoints];
        long[] val = new long[testPoints];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < testPoints; i++) {
            time[i] = baseTime + (i % 15);
//            val[i] = rand.nextLong() >> 3;
            val[i] = i;
        }

        BasicDateVector dateVector = new BasicDateVector(time);
        dateVector.setCompressedMethod(2);
        BasicLongVector valVector = new BasicLongVector(val);
        valVector.setCompressedMethod(1);
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(valVector);
        colVectors.add(dateVector);

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`val`date,[LONG,DATE])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(testPoints, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressLong2() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        long[] val = new long[100000];
        long[] val2 = new long[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
//            val[i] = rand.nextLong() >> 3;
            val[i] = i;
            val2[i] = i;
        }

        BasicDateVector dateVector = new BasicDateVector(time);
        BasicLongVector valVector = new BasicLongVector(val);
        dateVector.setCompressedMethod(1);
        valVector.setCompressedMethod(1);
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(dateVector);
        colVectors.add(valVector);

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
            val[i] = rand.nextDouble();
        }

        List<Vector> colVectors = new ArrayList<>();
        BasicDateVector dateVector = new BasicDateVector(time);
        colVectors.add(dateVector);
        dateVector.setCompressedMethod(1);
        BasicDoubleVector valVector = new BasicDoubleVector(val);
        valVector.setCompressedMethod(2);
        colVectors.add(valVector);

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
            val[i] = rand.nextInt() >> 1;
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


    private static void compareBasicTable(BasicTable table, BasicTable newTable) throws Exception {
        assertEquals("rows not equal", table.rows(), newTable.rows());
        assertEquals("cols not equal", table.columns(), newTable.columns());
        int count = 0;
        int cols = table.columns();
        for (int i = 0; i < cols; i++) {
            AbstractVector v1 = (AbstractVector) table.getColumn(i);
            AbstractVector v2 = (AbstractVector) table.getColumn(i);
            if (!v1.equals(v2)) {
                for (int j = 0; j < table.rows(); j++) {
                    Entity e1 = table.getColumn(i).get(j);
                    Entity e2 = newTable.getColumn(i).get(j);
                    if (!e1.equals(e2)) {
                        if (count < 1000)
                            System.out.println(i + "," + j + ": " + " expected: " + e1.getString() + " actual: " + e2.getString());
                        count++;
                    }
                }
            }
        }
        assertEquals("Total unmatched values found", 0, count);
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}
