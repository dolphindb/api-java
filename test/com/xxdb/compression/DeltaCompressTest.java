package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.LittleEndianDataOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeltaCompressTest {

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
    public void testLocalBlockCompressor() {
        int unitLength = 2, testPoints = 10;

        ByteBuffer originalData = ByteBuffer.allocate(unitLength * testPoints);
        for (int i = 0; i < testPoints; i++) {
            originalData.putShort((short) i);
        }
        originalData.flip();

        DeltaOfDeltaBlockEncoder encoder = new DeltaOfDeltaBlockEncoder(unitLength);
        DeltaOfDeltaBlockDecoder decoder = new DeltaOfDeltaBlockDecoder(unitLength);

        long[] compressedData = encoder.compress(originalData, testPoints * unitLength);
        ByteBuffer finalData = ByteBuffer.allocate(unitLength * testPoints);
        System.out.println(decoder.decompress(compressedData, finalData));

        finalData.flip();
        for (int i = 0; i < testPoints; i++) {
            System.out.println(i + 1 + ": " + finalData.getShort());
        }

    }

    @Test
    public void testLocalCompressor() throws IOException {
        int unitLength = 8, testPoints = 300000;

        ByteBuffer originalData = ByteBuffer.allocate(unitLength * testPoints);
        for (int i = 0; i < testPoints; i++) {
            originalData.putDouble(1.5);
        }
        originalData.flip();

        DeltaOfDeltaEncoder encoder = new DeltaOfDeltaEncoder();
        DeltaOfDeltaDecoder decoder = new DeltaOfDeltaDecoder();

        ByteBuffer compressedData = encoder.compress(originalData, testPoints, unitLength, testPoints * unitLength);
        compressedData.flip();
        ExtendedDataInput inputStream = decoder.decompress(new BigEndianDataInputStream(new ByteArrayInputStream(compressedData.array())),
                compressedData.capacity() - compressedData.limit(), unitLength, testPoints, false);
        System.out.println("Rows:" + inputStream.readInt());
        inputStream.readInt();
        for (int i = 0; i < testPoints; i++) {
            System.out.println(inputStream.readDouble());
        }
    }

    @Test
    public void testCompressTable() throws Exception {
        int testpoints = 10;
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("val");
        int[] time = new int[testpoints];
        double[] val = new double[testpoints];

        int baseTime = Utils.countDays(2000,1,1);
        for (int i = 0; i < testpoints; i++) {
            time[i] = baseTime + (i % 15);
            val[i] = testpoints * 1.5;
        }


        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicDoubleVector(val));

        BasicTable table = new BasicTable(colNames, colVectors);

        LittleEndianDataOutputStream output = new LittleEndianDataOutputStream(new FileOutputStream("/home/ydli/Documents/Compression/testTable"));
        BasicTableCompressor.compressBasicTable(table, 2, output, true);
        output.flush();
        output.close();

        LittleEndianDataInputStream input = new LittleEndianDataInputStream(new FileInputStream("/home/ydli/Documents/Compression/testTable"));
        BasicTable newT = new BasicTable(input);

        assertEquals(newT.rows(), table.rows());
        System.out.println(newT.getColumn(1).getDataType());
        System.out.println(newT.getColumn(1).get(1).getNumber().floatValue());

        for (int i = 0; i < testpoints; i++) {
            assertEquals(newT.getColumn(0).get(i).getNumber().intValue(), table.getColumn(0).get(i).getNumber().intValue());
            assertEquals(newT.getColumn(1).get(i).getNumber().longValue(), table.getColumn(1).get(i).getNumber().longValue());
        }

    }

    @Test
    public void testCompressBasicTable() throws Exception {
        List<String> colNames = new ArrayList<>();
        colNames.add("date");
        colNames.add("mestamp");
        colNames.add("val1");
        colNames.add("val2");
        colNames.add("val3");
        int[] time = new int[100000];
        long[] timestamp = new long[100000];
        int[] val1 = new int[100000];
        long[] val2 = new long[100000];
        double[] val3 = new double[100000];

        int baseTime = Utils.countDays(2000,1,1);
        int baseTimestamp = Utils.countSeconds(2000, 1, 1, 6, 0, 0);
        for (int i = 0; i < 100000; i++) {
            time[i] = baseTime + (i % 15);
            timestamp[i] = baseTimestamp + (i % 30);
            val1[i] = 100000- i * 2;
            val2[i] = 100000 + i * 23;
            val3[i] = 3.15;
        }


        List<Vector> colVectors = new ArrayList<>();
        colVectors.add(new BasicDateVector(time));
        colVectors.add(new BasicTimestampVector(timestamp));
        colVectors.add(new BasicIntVector(val1));
        colVectors.add(new BasicLongVector(val2));
        colVectors.add(new BasicDoubleVector(val3));

        BasicTable table = new BasicTable(colNames, colVectors);

        LittleEndianDataOutputStream output = new LittleEndianDataOutputStream(new FileOutputStream("/home/ydli/Documents/Compression/testTable"));
        BasicTableCompressor.compressBasicTable(table, 2, output, true);
        output.flush();
        output.close();

        LittleEndianDataInputStream input = new LittleEndianDataInputStream(new FileInputStream("/home/ydli/Documents/Compression/testTable"));
        BasicTable newT = new BasicTable(input);

        assertEquals(newT.rows(), table.rows());

        for (int i = 0; i < 500; i++) {
            System.out.println(newT.getRowJson(65000 + i));
            System.out.println(table.getRowJson(65000 + i));
        }

        for (int i = 0; i < 100000; i++) {
            assertEquals("Row: " + i + " Col: " + 0 ,
                    newT.getColumn(0).get(i).getNumber().intValue(),
                    table.getColumn(0).get(i).getNumber().intValue());
            assertEquals("Row: " + i + " Col: " + 1,
                    newT.getColumn(1).get(i).getNumber().longValue(),
                    table.getColumn(1).get(i).getNumber().longValue());
            assertEquals("Row: " + i + " Col: " + 2,
                    newT.getColumn(2).get(i).getNumber().intValue(),
                    table.getColumn(2).get(i).getNumber().longValue());
            assertEquals("Row: " + i + " Col: " + 3,
                    newT.getColumn(3).get(i).getNumber().longValue(),
                    table.getColumn(3).get(i).getNumber().longValue());
            assertEquals("Row: " + i + " Col: " + 4,
                    newT.getColumn(4).get(i).getNumber().longValue(),
                    table.getColumn(4).get(i).getNumber().longValue());
        }

    }

//    @Test
//    public void testCompareServerCompress() throws Exception {
//
//        List<String> colNames = new ArrayList<>();
//        colNames.add("date");
//        colNames.add("val");
//        int[] time = new int[100000];
//        int[] val = new int[100000];
//
//        int baseTime = Utils.countDays(2000,1,1);
//        for (int i = 0; i < 100000; i++) {
//            time[i] = baseTime + (i % 15);
//            val[i] = 100000 - i;
//        }
//
//
//        List<Vector> colVectors = new ArrayList<>();
//        colVectors.add(new BasicDateVector(time));
//        colVectors.add(new BasicIntVector(val));
//
//        BasicTable table = new BasicTable(colNames, colVectors);
//
//        DeltaOfDeltaEncoder encoder = new DeltaOfDeltaEncoder();
//        for (int i = 0; i < 1; i++) {
//            Vector col = table.getColumn(i);
//            int unitLength = 0;
//            if (col.getDataType() == Entity.DATA_TYPE.DT_DATE || col.getDataType() == Entity.DATA_TYPE.DT_INT ) {
//                unitLength = 4;
//            }
//            int elementCount = col.rows();
//            ByteBuffer in = ByteBuffer.allocate(elementCount * unitLength);
//            for (int j = 0; j < elementCount; j++) {
//                in.putInt(col.get(j).getNumber().intValue());
//            }
//            in.flip();
//            DataOutput out = encoder.compress(in, elementCount, unitLength, elementCount * unitLength, true);
//        }
//
//        DataInput input = new LittleEndianDataInputStream(new FileInputStream("/home/ydli/Documents/Compression/clientCompressed"));
//        int length = input.readInt();
//        System.out.println("Row " + length);
//        input.readInt();
//        DeltaOfDeltaDecoder decoder = new DeltaOfDeltaDecoder();
//        ExtendedDataInput decompressIn = decoder.decompress(input, length, 4, 100000, false);
//        System.out.println(decompressIn.readInt());
//        decompressIn.readInt();
//        for (int i = 0; i < 100000; i++) {
//            assertEquals("At row " + i, decompressIn.readInt(), time[i]);
//        }
//    }


    @After
    public void tearDown() throws Exception {
        conn.close();
    }


}
