package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
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
        //include null
        int n=600000/2;
        BasicLongVector val1 = (BasicLongVector) conn.run("rand(-10000l..10000l,"+n+")  join take(long(),"+n+")");
        val1.setCompressedMethod(Vector.COMPRESS_LZ4);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,LONG])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null delta
        val1.setCompressedMethod(Vector.COMPRESS_DELTA);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,LONG])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
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

        int baseTime = Utils.countDays(1920,1,1);
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
    @Test(expected = RuntimeException.class)
    public void testCompressDoubledelta() throws Exception {
        int n=5000000;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n*2];
        double[] val = new double[n*2];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n*2; i++) {
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
    }

    @Test
    public void testCompressDouble() throws Exception {
        int n=5000000;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n*2];
        double[] val = new double[n*2];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n*2; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = rand.nextDouble();
        }

        List<Vector> colVectors = new ArrayList<>();
        BasicDateVector dateVector = new BasicDateVector(time);
        colVectors.add(dateVector);
        dateVector.setCompressedMethod(1);
        BasicDoubleVector valVector = new BasicDoubleVector(val);
        valVector.setCompressedMethod(1);
        colVectors.add(valVector);

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,DOUBLE])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n*2, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        BasicDoubleVector val1 = (BasicDoubleVector) conn.run("rand(10.0,"+n+")  join take(double(),"+n+")");
        val1.setCompressedMethod(Vector.COMPRESS_LZ4);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,DOUBLE])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressFloat() throws Exception {
        int n=50000;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        float[] val = new float[100000];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = rand.nextFloat();
        }

        List<Vector> colVectors = new ArrayList<>();
        BasicDateVector dateVector = new BasicDateVector(time);
        colVectors.add(dateVector);
        dateVector.setCompressedMethod(1);
        BasicFloatVector valVector = new BasicFloatVector(val);
        valVector.setCompressedMethod(1);
        colVectors.add(valVector);

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,FLOAT])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        BasicFloatVector val1 = (BasicFloatVector) conn.run("rand(10.0f,"+n+")  join take(float(),"+n+")");
        val1.setCompressedMethod(Vector.COMPRESS_LZ4);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,FLOAT])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressFloatDelta() throws Exception {
        int n=50000;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        float[] val = new float[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = rand.nextFloat();
        }
        List<Vector> colVectors = new ArrayList<>();
        BasicDateVector dateVector = new BasicDateVector(time);
        colVectors.add(dateVector);
        dateVector.setCompressedMethod(1);
        BasicFloatVector valVector = new BasicFloatVector(val);
        valVector.setCompressedMethod(2);
        colVectors.add(valVector);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`val,[DATE,FLOAT])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }


    @Test
    public void testCompressInt() throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        int[] val = new int[n];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n; i++) {
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
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        BasicIntVector val1 = (BasicIntVector) conn.run("rand(-1000..1000,"+n/2+") join take(int(),"+n/2+")");
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,INT])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null delta
        val1.setCompressedMethod(Vector.COMPRESS_DELTA);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,INT])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }


    @Test
    public void testCompressShort()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[2*n];
        short[] val = new short[2*n];

        int baseTime = Utils.countDays(1599,1,1);
        for (int i = 0; i < 2*n; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = (short) i;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicShortVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,SHORT])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        BasicShortVector val1 = (BasicShortVector) conn.run("rand(-100h..1000h,"+n+") join take(short(),"+n+") ");
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,SHORT])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null delta
        val1.setCompressedMethod(Vector.COMPRESS_DELTA);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,SHORT])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressTime() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("minute");
        colNames.add("second");
        colNames.add("timestamp");
        colNames.add("month");
        colNames.add("time");
        colNames.add("nanotime");
        colNames.add("nanotimestamp");
        colNames.add("datetime");
        int[] date = new int[100000];
        int[] minute = new int[100000];
        int[] second = new int[100000];
        long[] timestamp = new long[100000];
        int[] month = new int[100000];
        int[] time = new int[100000];
        long[] nanotime = new long[100000];
        long[] nanotimestamp = new long[100000];
        int[] datetime = new int[100000];
        int baseDate = Utils.countDays(2000,1,1);
        int baseMinute = Utils.countMinutes(13, 8);
        int baseSecond = Utils.countSeconds(4, 5, 23);
        long basicTimestamp = Utils.countMilliseconds(2013,1,25,10,0,0,1);
        int basicMonth = Utils.countMonths(2013,1);
        int basicTime = Utils.countMilliseconds(10,0,0,1);
        long basicNanoTime = Utils.countNanoseconds(LocalTime.now());
        long basicNanoTimestamp = Utils.countDTNanoseconds(LocalDateTime.now());
        int basicDateTime = Utils.countDTSeconds(2013,1,25,10,0,0);
        for (int i = 0; i < 100000; i++) {
            if (i % 2 == 0) {
                date[i] = baseDate + (i % 15);
                minute[i] = baseMinute + ( i % 300);
                second[i] = baseSecond + (i % 1800);
                timestamp[i] = basicTimestamp - (i % 5000);
                month[i]=basicMonth+ (i % 15);
                time[i]=basicTime+ (i % 15);
                nanotime[i]=basicNanoTime+ (i % 15);
                nanotimestamp[i]=basicNanoTimestamp+ (i % 15);
                datetime[i]=basicDateTime+ (i % 15);
            } else {
                date[i] = baseDate - (i % 15);
                minute[i] = baseMinute - ( i % 300);
                second[i] = baseSecond - (i % 1800);
                timestamp[i] = basicTimestamp + (i % 5000);
                month[i]=basicMonth - (i % 15);
                time[i]=basicTime-(i % 15);
                nanotime[i]=basicNanoTime-(i % 15);
                nanotimestamp[i]=basicNanoTimestamp-(i % 15);
                datetime[i]=basicDateTime-(i % 15);
            }
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(date));
        colVectors.add(new BasicMinuteVector(minute));
        colVectors.add(new BasicSecondVector(second));
        colVectors.add(new BasicTimestampVector(timestamp));
        colVectors.add(new BasicMonthVector(month));
        colVectors.add(new BasicTimeVector(time));
        colVectors.add(new BasicNanoTimeVector(nanotime));
        colVectors.add(new BasicNanoTimestampVector(nanotimestamp));
        colVectors.add(new BasicDateTimeVector(datetime));
        BasicTable table = new BasicTable(colNames, colVectors);

        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`minute`second`timestamp`month`time`nanotime`nanotimestamp`datetime,[DATE,MINUTE,SECOND,TIMESTAMP,MONTH," +
                "TIME,NANOTIME,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        int n=5000000;
        BasicDateVector datev = (BasicDateVector) conn.run("2012.10.01 +1.."+n+" join take(date(),"+n+")");
        BasicMonthVector monthv = (BasicMonthVector) conn.run("2012.06M +1.."+n+" join take(month(),"+n+")");
        BasicTimeVector timev = (BasicTimeVector) conn.run("13:30:10.008 +1.."+n+" join take(time(),"+n+")");
        BasicMinuteVector minutev = (BasicMinuteVector) conn.run("13:30m +1.."+n+" join take(minute(),"+n+")");
        BasicSecondVector secondv = (BasicSecondVector) conn.run("13:30:10 +1.."+n+" join take(second(),"+n+")");
        BasicTimestampVector timestampv = (BasicTimestampVector) conn.run("2012.06.13 13:30:10.008 +1.."+n+" join take(timestamp(),"+n+")");
        BasicNanoTimeVector nanotimev = (BasicNanoTimeVector) conn.run("13:30:10.008007006 +1.."+n+" join take(nanotime(),"+n+")");
        BasicNanoTimestampVector nanotimestampv = (BasicNanoTimestampVector) conn.run("2012.06.13 13:30:10.008007006 +rand(1.."+n+" ,"+n+") join take(nanotimestamp(),"+n+")");
        BasicDateTimeVector datetimev = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + rand(1.."+n+" ,"+n+") join take(datetime(),"+n+")");
        colVectors = new ArrayList<>();
        colVectors.add(datev);
        colVectors.add(minutev);
        colVectors.add(secondv);
        colVectors.add(timestampv);
        colVectors.add(monthv);
        colVectors.add(timev);
        colVectors.add(nanotimev);
        colVectors.add(nanotimestampv);
        colVectors.add(datetimev);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`minute`second`timestamp`month`time`nanotime`nanotimestamp`datetime,[DATE,MINUTE,SECOND,TIMESTAMP,MONTH," +
                "TIME,NANOTIME,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null detal
        datev.setCompressedMethod(2);
        minutev.setCompressedMethod(2);
        timestampv.setCompressedMethod(2);
        secondv.setCompressedMethod(2);
        monthv.setCompressedMethod(2);
        nanotimev.setCompressedMethod(2);
        nanotimestampv.setCompressedMethod(2);
        datetimev.setCompressedMethod(2);
        timev.setCompressedMethod(2);
        colVectors = new ArrayList<>();
        colVectors.add(datev);
        colVectors.add(minutev);
        colVectors.add(secondv);
        colVectors.add(timestampv);
        colVectors.add(monthv);
        colVectors.add(timev);
        colVectors.add(nanotimev);
        colVectors.add(nanotimestampv);
        colVectors.add(datetimev);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`minute`second`timestamp`month`time`nanotime`nanotimestamp`datetime,[DATE,MINUTE,SECOND,TIMESTAMP,MONTH," +
                "TIME,NANOTIME,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressTime_before1970() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("timestamp");
        colNames.add("month");
        colNames.add("nanotimestamp");
        colNames.add("datetime");
        int[] date = new int[100000];
        long[] timestamp = new long[100000];
        int[] month = new int[100000];
        long[] nanotimestamp = new long[100000];
        int[] datetime = new int[100000];
        int baseDate = Utils.countDays(1970,1,1);
        long basicTimestamp = Utils.countMilliseconds(1970,1,1,0,0,0,0);
        int basicMonth = Utils.countMonths(1970,1);
        long basicNanoTimestamp = Utils.countDTNanoseconds(LocalDateTime.of(1970,1,1,0,0));
        int basicDateTime = Utils.countDTSeconds(1970,1,1,0,0,0);
        for (int i = 0; i < 100000; i++) {
            date[i] = baseDate - i;
            timestamp[i] = basicTimestamp -i;
            month[i]=basicMonth - i;
            nanotimestamp[i]=basicNanoTimestamp-i;
            datetime[i]=basicDateTime-i;
        }
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(date));
        colVectors.add(new BasicTimestampVector(timestamp));
        colVectors.add(new BasicMonthVector(month));
        colVectors.add(new BasicNanoTimestampVector(nanotimestamp));
        colVectors.add(new BasicDateTimeVector(datetime));
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`timestamp`month`nanotimestamp`datetime,[DATE,TIMESTAMP,MONTH,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        int n=500000;
        BasicDateVector datev = (BasicDateVector) conn.run("1970.01.01 -(1.."+n+") join take(date(),"+n+")");
        BasicMonthVector monthv = (BasicMonthVector) conn.run("1970.01M -(1.."+n+") join take(month(),"+n+")");
        BasicTimestampVector timestampv = (BasicTimestampVector) conn.run("1970.01.01 00:00:00.000 -(1.."+n+") join take(timestamp(),"+n+")");
        BasicNanoTimestampVector nanotimestampv = (BasicNanoTimestampVector) conn.run("nanotimestamp(0) -rand(1.."+n+" ,"+n+") join take(nanotimestamp(),"+n+")");
        BasicDateTimeVector datetimev = (BasicDateTimeVector) conn.run("1970.01.01 00:00:00 - rand(1.."+n+" ,"+n+") join take(datetime(),"+n+")");
        colVectors = new ArrayList<>();
        colVectors.add(datev);
        colVectors.add(timestampv);
        colVectors.add(monthv);
        colVectors.add(nanotimestampv);
        colVectors.add(datetimev);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`timestamp`month`nanotimestamp`datetime,[DATE,TIMESTAMP,MONTH,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null detal
        datev.setCompressedMethod(2);
        timestampv.setCompressedMethod(2);
        monthv.setCompressedMethod(2);
        nanotimestampv.setCompressedMethod(2);
        datetimev.setCompressedMethod(2);
        colVectors = new ArrayList<>();
        colVectors.add(datev);
        colVectors.add(timestampv);
        colVectors.add(monthv);
        colVectors.add(nanotimestampv);
        colVectors.add(datetimev);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(100000:0,`date`timestamp`month`nanotimestamp`datetime,[DATE,TIMESTAMP,MONTH,NANOTIMESTAMP,DATETIME]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }


    static void compareBasicTable(BasicTable table, BasicTable newTable) throws Exception {
        assertEquals("rows not equal", table.rows(), newTable.rows());
        assertEquals("cols not equal", table.columns(), newTable.columns());
        int count = 0;
        int cols = table.columns();
        for (int i = 0; i < cols; i++) {
            AbstractVector v1 = (AbstractVector) table.getColumn(i);
            AbstractVector v2 = (AbstractVector) newTable.getColumn(i);
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

    @Test
    public void testCompressCharlz4() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int n = 100000;
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000, 1, 1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);

        }
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        BasicByteVector val = (BasicByteVector) conn.run("rand(['f','3',NULL]," + n + ")");
        val.setCompressedMethod(1);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,CHAR])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressCharDelta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int n=100000;
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        BasicByteVector val = (BasicByteVector) conn.run("rand(['f','3',NULL]," + n + ")");
        val.setCompressedMethod(2);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,CHAR])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
    }

    @Test
    public void testCompressBool()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        int baseTime = Utils.countDays(1555,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicBooleanVector val = (BasicBooleanVector) conn.run("rand([true,false],"+n+")");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,BOOL])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        val = (BasicBooleanVector) conn.run("rand([true,false,NULL],"+n+")");
        val.setCompressedMethod(1);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,BOOL])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);

    }

    @Test(expected = RuntimeException.class)
    public void testCompressBoolDelta()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        int baseTime = Utils.countDays(1555,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicBooleanVector val = (BasicBooleanVector) conn.run("rand([true,false],"+n+")");
        val.setCompressedMethod(2);
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,BOOL])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
    }

    @Test
    public void testCompressString() throws Exception {
        BasicTable newT;
        int n=10000;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        String[] val = new String[n];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = "i"+i;
        }

        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicStringVector(val));
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,STRING]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        BasicStringVector val1 = (BasicStringVector) conn.run("take(`d`d`y`t``,"+n+")");
        val1.setCompressedMethod(1);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,STRING])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressStringDelta() throws Exception {
        BasicTable newT;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        BasicStringVector val1 = (BasicStringVector) conn.run("take(`d`d`y`t``,100000)");
        val1.setCompressedMethod(2);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,STRING])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
    }

    @Test
    public void testCompressSymbolDelta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        String[] val = new String[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = "i"+i;
        }
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        BasicSymbolVector val1 = new BasicSymbolVector(Arrays.asList(val));
        val1.setCompressedMethod(1);
        colVectors.add(val1);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,SYMBOL]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val1= (BasicSymbolVector) conn.run("rand(`d`bv`f``,100000)");
        val1.setCompressedMethod(1);
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,SYMBOL]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null deta
//        colVectors = new ArrayList<>();
//        colVectors.add(new BasicDateVector(time));
//        val1= (BasicSymbolVector) conn.run("rand(`d`bv`f``,100000)");
//        val1.setCompressedMethod(2);
//        colVectors.add(val1);
//        table = new BasicTable(colNames, colVectors);
//        arg = Arrays.asList(table);
//        conn.run("t = table(1000:0,`date`val,[DATE,SYMBOL]);" +
//                "share t as st");
//        count = (BasicInt) conn.run("tableInsert{st}", arg);
//        assertEquals(100000, count.getInt());
//        newT = (BasicTable) conn.run("select * from st");
//        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressUUID() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int n=1000000;
        int[] time = new int[n];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicUuidVector val = (BasicUuidVector) conn.run("rand([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid('5d212a78-cc48-e3b1-4235-b4d91473ee89'),uuid()],"+n+")");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,UUID]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressUUIDdelta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int n=1000000;
        int[] time = new int[n];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicUuidVector val = (BasicUuidVector) conn.run("rand([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid('5d212a78-cc48-e3b1-4235-b4d91473ee89'),uuid()],"+n+")");
        List<Vector> colVectors = new ArrayList<>();
        //delta
        val.setCompressedMethod(2);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,UUID]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressIPADDRdelta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicIPAddrVector val = (BasicIPAddrVector) conn.run("rand([ipaddr('192.168.0.1'),ipaddr('192.168.0.10'),ipaddr()],100000)");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(2);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,IPADDR]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
    }

    @Test
    public void testCompressIPADDR() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicIPAddrVector val = (BasicIPAddrVector) conn.run("rand([ipaddr('192.168.0.1'),ipaddr('192.168.0.10'),ipaddr()],100000)");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(1);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,IPADDR]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressINT128Delta() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicInt128Vector val = (BasicInt128Vector) conn.run("rand([int128('e1671797c52e15f763380b45e841ec32'),int128('e1671797c52e15f763380b45e841ec82'),int128()],100000)");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(2);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,INT128]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
    }


    @Test
    public void testCompressINT128() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicInt128Vector val = (BasicInt128Vector) conn.run("rand([int128('e1671797c52e15f763380b45e841ec32'),int128('e1671797c52e15f763380b45e841ec82'),int128()],100000)");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(1);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,INT128]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressDATAHOUR() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[100000];
        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicDateHourVector val = (BasicDateHourVector) conn.run("rand(datehour('2012.06.13T13')+1..10 join datehour(),100000)");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(1);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,DATEHOUR]);" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //DELTA
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(2);
        colVectors.add(val);
        table = new BasicTable(colNames, colVectors);
        arg = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,DATEHOUR]);" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", arg);
        assertEquals(100000, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test
    public void testCompressComplex()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        int baseTime = Utils.countDays(1555,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicComplexVector val = (BasicComplexVector) conn.run("complex(rand(1..10 join int(),"+n+"),rand(1..10 join int(),"+n+"))");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,COMPLEX])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressComplexdelta()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[n];
        int baseTime = Utils.countDays(1555,1,1);
        for (int i = 0; i < n; i++) {
            time[i] = baseTime + (i % 15);
        }
        BasicComplexVector val = (BasicComplexVector) conn.run("complex(rand(1..10 join int(),"+n+"),rand(1..10 join int(),"+n+"))");
        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        val.setCompressedMethod(2);
        colVectors.add(val);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,COMPLEX])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }
/*
    @Test
    public void testCompressDuration()throws Exception {
        int n=1000000;
        Random rand = new Random();
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[2*n];
        int baseTime = Utils.countDays(1599,1,1);
        for (int i = 0; i < 2*n; i++) {
            time[i] = baseTime + (i % 15);
        }
        //include null
        BasicDurationVector val1 = (BasicDurationVector) conn.run("\n" +
                "duration(rand([\"20H\",\"20y\"],10))");
        ArrayList<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,DURATION])" +
                "share t as st");
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        BasicTable newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
        //include null delta
        val1.setCompressedMethod(Vector.COMPRESS_DELTA);
        colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(val1);
        table = new BasicTable(colNames, colVectors);
        args = Arrays.asList(table);
        conn.run("t = table(1000:0,`date`val,[DATE,DURATION])" +
                "share t as st");
        count = (BasicInt) conn.run("tableInsert{st}", args);
        assertEquals(2*n, count.getInt());
        newT = (BasicTable) conn.run("select * from st");
        compareBasicTable(table, newT);
    }
*/
    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}
