package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.List;

import static org.junit.Assert.*;

public class BasicArrayVectorTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test(expected = Exception.class)
    public void TestBasicSringArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(STRING[], 0, 20)\n" +
                "for(i in 1..20000){\n" +
                "\ta.append!([string(1..100 join NULL)])\n" +
                "};a");
    }
//
//    @Test(expected = Exception.class)
//    public void TestBasicSymbolArrayVector() throws Exception {
//        DBConnection conn = new DBConnection();
//        conn.connect(HOST, PORT);
//        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(SYMBOL[], 0, 20)\n" +
//                "for(i in 1..20000){\n" +
//                "\ta.append!([symbol(string(1..100 join NULL))])\n" +
//                "};a");
//    }

    @Test
    public void TestBasicIntArrayVector() throws Exception {
         DBConnection conn = new DBConnection(false,false,true);
         conn.connect(HOST, PORT);
         BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(INT[], 0, 20)\n" +
                 "for(i in 1..20000){\n" +
                 "\ta.append!([1..100 join NULL])\n" +
                 "};a");
         BasicIntVector BV= (BasicIntVector) conn.run("1..100 join NULL");
         for (int i=0;i<20000;i++){
             assertEquals(BV.getString(),obj.getVectorValue(i).getString());
         }
         assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,obj.getDataType());
         assertEquals(20000,obj.rows());
         conn.close();
    }

    @Test
    public void TestBasicLongArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(LONG[], 0, 20)\n" +
                "for(i in 1..20000){\n" +
                "\ta.append!([1..100 join NULL])\n" +
                "};a");
        BasicLongVector BV= (BasicLongVector) conn.run("long(1..100 join NULL)");
        for (int i=0;i<20000;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        obj.Append(new BasicLongVector(new long[]{15,89,3749,4293}));
        assertEquals(20001,obj.rows());
        assertEquals("[15,89,3749,4293]",obj.getVectorValue(20000).getString());
        assertEquals(Entity.DATA_TYPE.DT_LONG_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasicShortArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 20)\n" +
                "for(i in 1..20000){\n" +
                "\ta.append!([1..100 join NULL])\n" +
                "};a");
        BasicShortVector BV= (BasicShortVector) conn.run("short(1..100 join NULL)");
        for (int i=0;i<20000;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        assertEquals(Entity.DATA_TYPE.DT_SHORT_ARRAY,obj.getDataType());
        obj.Append(new BasicShortVector(new short[]{3,7,32,10}));
        assertEquals(20001,obj.rows());
        assertEquals("[3,7,32,10]",obj.getVectorValue(20000).getString());
        conn.close();
    }


    @Test
    public void TestBasic_Float_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 20)\n" +
                "for(i in 1..20000){\n" +
                "\ta.append!([(1..100+0.7) join NULL])\n" +
                "};a");
        BasicFloatVector BV= (BasicFloatVector) conn.run("float((1..100+0.7) join NULL)");
        for (int i=0;i<20000;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        obj.Append(new BasicFloatVector(new float[]{(float) 17.92, (float) 16.62, (float) 15.66, (float) 9.07}));
        assertEquals(20001,obj.rows());
        assertEquals("[17.92000008,16.62000084,15.65999985,9.06999969]",obj.getVectorValue(20000).getString());
        assertEquals(Entity.DATA_TYPE.DT_FLOAT_ARRAY,obj.getDataType());
        conn.close();

    }

    @Test
    public void TestBasic_DOUBLE_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 20)\n" +
                "for(i in 1..20000){\n" +
                "\ta.append!([(1..100+0.7) join NULL])\n" +
                "};a");
        BasicDoubleVector BV= (BasicDoubleVector) conn.run("double((1..100+0.7) join NULL)");
        for (int i=0;i<20000;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        obj.Append(new BasicDoubleVector(new double[]{2.20,3.17,4.20}));
        assertEquals(20001,obj.rows());
        assertEquals("[2.2,3.17,4.2]",obj.getVectorValue(20000).getString());
        assertEquals(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,obj.getDataType());
        conn.close();

    }

    @Test
    public void TestBasic_bool_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="a = array(BOOL[], 0, 20)\n" +
                "a.append!([true false,true,])\n;a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[true,false]",obj.getVectorValue(0).getString());
        assertEquals("[true]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        assertEquals(Entity.DATA_TYPE.DT_BOOL_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_char_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="a = array(CHAR[], 0, 20)\n" +
                "a.append!([char(1..2),char(0),char()]);a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1,2]",obj.getVectorValue(0).getString());
        assertEquals("[0]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        assertEquals(Entity.DATA_TYPE.DT_BYTE_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_date_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="a = array(DATE[], 0, 20)\n" +
                "a.append!([date(1..2),date(0),date()]);a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1970.01.02,1970.01.03]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicDateVector(new int[]{638,3729,2924}));
        assertEquals(4,obj.rows());
        assertEquals("[1971.10.01,1980.03.18,1978.01.03]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_DATE_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_month_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(MONTH[], 0, 20)\n" +
                "a.append!([month(1..2),month(0),month()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[0000.02M,0000.03M]",obj.getVectorValue(0).getString());
        assertEquals("[0000.01M]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicMonthVector(new int[]{12,32,15}));
        assertEquals(4,obj.rows());
        assertEquals("[0001.01M,0002.09M,0001.04M]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_MONTH_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_time_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(TIME[], 0, 20)\n" +
                "a.append!([time(1..2),time(0),time()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[00:00:00.001,00:00:00.002]",obj.getVectorValue(0).getString());
        assertEquals("[00:00:00.000]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicTimeVector(new int[]{342,4759849,82947836}));
        assertEquals(4,obj.rows());
        assertEquals("[00:00:00.342,01:19:19.849,23:02:27.836]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_TIME_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_minute_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(MINUTE[], 0, 20)\n" +
                "a.append!([minute(1..2),minute(0),minute()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[00:01m,00:02m]",obj.getVectorValue(0).getString());
        assertEquals("[00:00m]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicMinuteVector(new int[]{89,485,824}));
        assertEquals(4,obj.rows());
        assertEquals("[01:29m,08:05m,13:44m]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_MINUTE_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasic_second_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(SECOND[], 0, 20)\n" +
                "a.append!([second(1..2),second(0),second()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[00:00:01,00:00:02]",obj.getVectorValue(0).getString());
        assertEquals("[00:00:00]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicSecondVector(new int[]{2353,543,675}));
        assertEquals(4,obj.rows());
        assertEquals("[00:39:13,00:09:03,00:11:15]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_SECOND_ARRAY,obj.getDataType());
        conn.close();
    }
    @Test
    public void TestBasic_datetime_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DATETIME[], 0, 20)\n" +
                "a.append!([datetime(1..2),datetime(0),datetime()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1970.01.01T00:00:01,1970.01.01T00:00:02]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:00]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicDateTimeVector(new int[]{37489,43535,54325}));
        assertEquals(4,obj.rows());
        assertEquals("[1970.01.01T10:24:49,1970.01.01T12:05:35,1970.01.01T15:05:25]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_DATETIME_ARRAY,obj.getDataType());
        conn.close();
    }
    @Test
    public void TestBasic_timestamp_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "\n" +
                "a = array(TIMESTAMP[], 0, 20)\n" +
                "a.append!([timestamp(1..2),timestamp(0),timestamp()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:00.000]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicTimestampVector(new long[]{463859,55933,5903825}));
        assertEquals(4,obj.rows());
        assertEquals("[1970.01.01T00:07:43.859,1970.01.01T00:00:55.933,1970.01.01T01:38:23.825]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY,obj.getDataType());
       // assertEquals("1970.01.01T00:00:00.000",obj.getVectorValue(1).get(0).getTemporal());
        conn.close();
    }
    @Test
    public void TestBasic_nanotime_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(NANOTIME[], 0, 20)\n" +
                "a.append!([nanotime(1..2),nanotime(0),nanotime()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[00:00:00.000000001,00:00:00.000000002]",obj.getVectorValue(0).getString());
        assertEquals("[00:00:00.000000000]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicNanoTimeVector(new long[]{374835639940L,64684840L,3746294L}));
        assertEquals(4,obj.rows());
        assertEquals("[00:06:14.835639940,00:00:00.064684840,00:00:00.003746294]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIME_ARRAY,obj.getDataType());
        conn.close();
    }
    @Test
    public void TestBasic_nanotimestamp_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(NANOTIMESTAMP[], 0, 20)\n" +
                "a.append!([nanotimestamp(1..2),nanotimestamp(0),nanotimestamp()])\n" +
                ";a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1970.01.01T00:00:00.000000001,1970.01.01T00:00:00.000000002]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:00.000000000]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicNanoTimestampVector(new long[]{3642749047937L,483626549294L}));
        assertEquals(4,obj.rows());
        assertEquals("[1970.01.01T01:00:42.749047937,1970.01.01T00:08:03.626549294]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIMESTAMP_ARRAY,obj.getDataType());
        conn.close();
    }


    @Test
    public void TestBasic_datehourArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DATEHOUR[], 0, 20)\n" +
                "a.append!([datehour(1..2),datehour(0),datehour()])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        assertEquals("[1970.01.01T01,1970.01.01T02]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
        obj.Append(new BasicDateHourVector(new int[]{36284,47292,4722,8493}));
        assertEquals(4,obj.rows());
        assertEquals("[1974.02.20T20,1975.05.25T12,1970.07.16T18,1970.12.20T21]",obj.getVectorValue(3).getString());
        assertEquals(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,obj.getDataType());
        conn.close();
    }
    @Test
    public void TestBasic_decimal32_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL32(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal32Vector(new double[] {0.0,-123.00432,132.204234,100.0},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasic_decimal32_ArrayVector_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL32(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal32Vector(new double[] {0.0,-123.00432,132.204234,100.0},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasic_decimal64_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL64(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal64Vector(new double[] {0.0,-123.00432,132.204234,100.0},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasic_decimal64_ArrayVector_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL64(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal64Vector(new double[] {0.0,-123.00432,132.204234,100.0},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasic_decimal128_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL128(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal128Vector(new String[] {"0.0","-123.00432","132.204234","100.0"},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasic_decimal128_ArrayVector_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        String script="\n" +
                "a = array(DECIMAL128(4)[],0)\n" +
                "a.append!([[1.11111,2],[1.000001,3],[34.1,2,111.0],[]])\n" +
                "a";
        BasicArrayVector obj = (BasicArrayVector)conn.run(script);
        System.out.println(obj.getString());
        assertEquals("[1.1111,2.0000]",obj.getVectorValue(0).getString());
        assertEquals("[1.0000,3.0000]",obj.getVectorValue(1).getString());
        assertEquals("[34.1000,2.0000,111.0000]",obj.getVectorValue(2).getString());
        assertEquals("[]",obj.getVectorValue(3).getString());

        obj.Append(new BasicDecimal128Vector(new String[] {"0.0","-123.00432","132.204234","100.0"},4));
        assertEquals(5,obj.rows());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",obj.getVectorValue(4).getString());
        conn.close();
    }
    @Test
    public void TestBasicIntArrayVector_allNULL() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector) conn.run("a = array(INT[], 0, 10000)\n" +
                "for(i in 1..10000){\n" +
                "\tx = int()\n" +
                "\ta.append!(x)\n" +
                "}\n" +
                "a");
        BasicIntVector BV= (BasicIntVector) conn.run("[int()]");
        for (int i=0;i<20;i++){
            assertEquals(BV.getString(),obj.getVectorValue(i).getString());
        }

        conn.close();
    }

      @Test
    public void Test_new_BasicArrayVector_BOOL() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();

        Vector v=new BasicBooleanVector(2);
        v.set(0,new BasicBoolean(true));
        v.set(1,new BasicBoolean(false));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[true,false]",obj.getVectorValue(0).getString());
        assertEquals("[true,false]",obj.getVectorValue(1).getString());
          conn.close();

    }

    @Test
    public void Test_new_BasicArrayVector_INT() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        int num=1000;
        Vector v=new BasicIntVector(num);
        for (int j=0;j<num;j++) {
            for (int i = 0; i < j; i++) {
                v.set(i, new BasicInt(i));
            }
            l.add(j,v);
        }
        BasicArrayVector obj = new BasicArrayVector(l);
        for (int j=0;j<num;j++) {
            for (int i = 0; i < j; i++) {
                v.set(i, new BasicInt(i));
            }
            assertEquals(v.getString(),obj.getVectorValue(j).getString());
        }

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        for (int j=0;j<num;j++) {
            for (int i = 0; i < j; i++) {
                v.set(i, new BasicInt(i));
            }
            assertEquals(v.getString(),res.getVectorValue(j).getString());
        }
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_LONG() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicLongVector(2);
        v.set(0,new BasicLong(1));
        v.set(1,new BasicLong(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1,2]",obj.getVectorValue(0).getString());
        assertEquals("[1,2]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1,2]",res.getVectorValue(0).getString());
        assertEquals("[1,2]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_SHORT() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicShortVector(2);
        v.set(0,new BasicShort((short) 1));
        v.set(1,new BasicShort((short) 2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1,2]",obj.getVectorValue(0).getString());
        assertEquals("[1,2]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1,2]",res.getVectorValue(0).getString());
        assertEquals("[1,2]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_FLOAT() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicFloatVector(2);
        v.set(0,new BasicFloat(1.0f));
        v.set(1,new BasicFloat(2.0f));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1,2]",obj.getVectorValue(0).getString());
        assertEquals("[1,2]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1,2]",res.getVectorValue(0).getString());
        assertEquals("[1,2]",res.getVectorValue(1).getString());
    }

    @Test
    public void Test_new_BasicArrayVector_DOUBLE() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDoubleVector(2);
        v.set(0,new BasicDouble(1));
        v.set(1,new BasicDouble(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1,2]",obj.getVectorValue(0).getString());
        assertEquals("[1,2]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1,2]",res.getVectorValue(0).getString());
        assertEquals("[1,2]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_DATE() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDateVector(2);
        v.set(0,new BasicDate(LocalDate.of(2022,1,1)));
        v.set(1,new BasicDate(LocalDate.of(1970,1,1)));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[2022.01.01,1970.01.01]",obj.getVectorValue(0).getString());
        assertEquals("[2022.01.01,1970.01.01]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[2022.01.01,1970.01.01]",res.getVectorValue(0).getString());
        assertEquals("[2022.01.01,1970.01.01]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_MINUTE() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicMinuteVector(2);
        v.set(0,new BasicMinute(1));
        v.set(1,new BasicMinute(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[00:01m,00:02m]",obj.getVectorValue(0).getString());
        assertEquals("[00:01m,00:02m]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[00:01m,00:02m]",res.getVectorValue(0).getString());
        assertEquals("[00:01m,00:02m]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_second() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicSecondVector(2);
        v.set(0,new BasicSecond(1));
        v.set(1,new BasicSecond(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[00:00:01,00:00:02]",obj.getVectorValue(0).getString());
        assertEquals("[00:00:01,00:00:02]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[00:00:01,00:00:02]",res.getVectorValue(0).getString());
        assertEquals("[00:00:01,00:00:02]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_datetime() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDateTimeVector(2);
        v.set(0,new BasicDateTime(1));
        v.set(1,new BasicDateTime(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1970.01.01T00:00:01,1970.01.01T00:00:02]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:01,1970.01.01T00:00:02]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1970.01.01T00:00:01,1970.01.01T00:00:02]",res.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:01,1970.01.01T00:00:02]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_datehour() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDateHourVector(2);
        v.set(0,new BasicDateHour((short) 1));
        v.set(1,new BasicDateHour((short) 2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1970.01.01T01,1970.01.01T02]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T01,1970.01.01T02]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1970.01.01T01,1970.01.01T02]",res.getVectorValue(0).getString());
        assertEquals("[1970.01.01T01,1970.01.01T02]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_timestamp() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicTimestampVector(2);
        v.set(0,new BasicTimestamp(1));
        v.set(1,new BasicTimestamp(2));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002]",obj.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002]",res.getVectorValue(0).getString());
        assertEquals("[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_nanotimestamp() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        long time = System.currentTimeMillis();
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicNanoTimestampVector(2);
        v.set(0,new BasicNanoTimestamp(time));
        v.set(1,new BasicNanoTimestamp(time));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals(time,((Scalar)obj.getVectorValue(0).get(0)).getNumber());
        assertEquals(time,((Scalar)obj.getVectorValue(0).get(1)).getNumber());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals(time,((Scalar)res.getVectorValue(0).get(0)).getNumber());
        assertEquals(time,((Scalar)res.getVectorValue(0).get(1)).getNumber());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_nanotime() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicNanoTimeVector(2);
        v.set(0,new BasicNanoTime(LocalTime.of(1,1,1,1323433)));
        v.set(1,new BasicNanoTime(LocalTime.of(1,1,2,1323433)));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals(LocalTime.of(1,1,1,1323433),((Scalar)obj.getVectorValue(0).get(0)).getTemporal());
        assertEquals(LocalTime.of(1,1,1,1323433),((Scalar)obj.getVectorValue(1).get(0)).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),((Scalar)obj.getVectorValue(0).get(1)).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),((Scalar)obj.getVectorValue(1).get(1)).getTemporal());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals(LocalTime.of(1,1,1,1323433),((Scalar)res.getVectorValue(0).get(0)).getTemporal());
        assertEquals(LocalTime.of(1,1,1,1323433),((Scalar)res.getVectorValue(1).get(0)).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),((Scalar)res.getVectorValue(0).get(1)).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),((Scalar)res.getVectorValue(1).get(1)).getTemporal());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_bool() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicBooleanVector(2);
        v.set(0,new BasicBoolean(true));
        v.set(1,new BasicBoolean(false));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[true,false]",obj.getVectorValue(0).getString());
        assertEquals("[true,false]",obj.getVectorValue(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[true,false]",res.getVectorValue(0).getString());
        assertEquals("[true,false]",res.getVectorValue(1).getString());
        conn.close();
    }

    @Test
    public void Test_new_BasicArrayVector_UUID() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicUuidVector(2);
        v.set(0,new BasicUuid(1,3));
        v.set(1,new BasicUuid(2,3));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals(new BasicUuid(1,3).getString(),obj.getVectorValue(0).get(0).getString());
        assertEquals(new BasicUuid(2,3).getString(),obj.getVectorValue(0).get(1).getString());
        assertEquals(new BasicUuid(1,3).getString(),obj.getVectorValue(1).get(0).getString());
        assertEquals(new BasicUuid(2,3).getString(),obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals(new BasicUuid(1,3).getString(),res.getVectorValue(0).get(0).getString());
        assertEquals(new BasicUuid(2,3).getString(),res.getVectorValue(0).get(1).getString());
        assertEquals(new BasicUuid(1,3).getString(),res.getVectorValue(1).get(0).getString());
        assertEquals(new BasicUuid(2,3).getString(),res.getVectorValue(1).get(1).getString());
        conn.close();
    }


    @Test
    public void Test_new_BasicArrayVector_ipaddr() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicIPAddrVector(2);
        v.set(0,new BasicIPAddr(15645,564353));
        v.set(1,new BasicIPAddr(24635,34563));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals(new BasicIPAddr(15645,564353).getString(),obj.getVectorValue(0).get(0).getString());
        assertEquals(new BasicIPAddr(24635,34563).getString(),obj.getVectorValue(0).get(1).getString());
        assertEquals(new BasicIPAddr(15645,564353).getString(),obj.getVectorValue(1).get(0).getString());
        assertEquals(new BasicIPAddr(24635,34563).getString(),obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals(new BasicIPAddr(15645,564353).getString(),res.getVectorValue(0).get(0).getString());
        assertEquals(new BasicIPAddr(24635,34563).getString(),res.getVectorValue(0).get(1).getString());
        assertEquals(new BasicIPAddr(15645,564353).getString(),res.getVectorValue(1).get(0).getString());
        assertEquals(new BasicIPAddr(24635,34563).getString(),res.getVectorValue(1).get(1).getString());
        conn.close();
    }


    @Test
    public void Test_new_BasicArrayVector_int128() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicInt128Vector(2);
        v.set(0,new BasicInt128(15645,564353));
        v.set(1,new BasicInt128(24635,34563));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals(new BasicInt128(15645,564353).getString(),obj.getVectorValue(0).get(0).getString());
        assertEquals(new BasicInt128(24635,34563).getString(),obj.getVectorValue(0).get(1).getString());
        assertEquals(new BasicInt128(15645,564353).getString(),obj.getVectorValue(1).get(0).getString());
        assertEquals(new BasicInt128(24635,34563).getString(),obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals(new BasicInt128(15645,564353).getString(),res.getVectorValue(0).get(0).getString());
        assertEquals(new BasicInt128(24635,34563).getString(),res.getVectorValue(0).get(1).getString());
        assertEquals(new BasicInt128(15645,564353).getString(),res.getVectorValue(1).get(0).getString());
        assertEquals(new BasicInt128(24635,34563).getString(),res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal32() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal32Vector(2,4);
        v.set(0,new BasicDecimal32(15645.00,2));
        v.set(1,new BasicDecimal32(24635.00001,4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal32_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal32Vector(2,4);
        v.set(0,new BasicDecimal32(15645.00,2));
        v.set(1,new BasicDecimal32(24635.00001,4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal64() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal64Vector(2,4);
        v.set(0,new BasicDecimal64(15645.00,2));
        v.set(1,new BasicDecimal64(24635.00001,4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal64_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal64Vector(2,4);
        v.set(0,new BasicDecimal64(15645.00,2));
        v.set(1,new BasicDecimal64(24635.00001,4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal128() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal128Vector(2,4);
        v.set(0,new BasicDecimal128("15645.00",2));
        v.set(1,new BasicDecimal128("24635.00001",4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());
        System.out.println("'11111'");

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());
        System.out.println("'2222'");

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test
    public void Test_new_BasicArrayVector_decimal128_compress_true() throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicDecimal128Vector(2,4);
        v.set(0,new BasicDecimal128("15645.00",2));
        v.set(1,new BasicDecimal128("24635.00001",4));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        System.out.println(obj.getString());

        assertEquals("15645.0000",obj.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",obj.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",obj.getVectorValue(1).get(1).getString());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");

        assertEquals("15645.0000",res.getVectorValue(0).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(0).get(1).getString());
        assertEquals("15645.0000",res.getVectorValue(1).get(0).getString());
        assertEquals("24635.0000",res.getVectorValue(1).get(1).getString());
        conn.close();
    }
    @Test(expected = Exception.class)
    public void Test_new_BasicArrayVector_string() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicStringVector(2);
        v.set(0,new BasicString("true"));
        v.set(1,new BasicString(""));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
    }

    @Test(expected = Exception.class)
    public void Test_new_BasicArrayVector_symbol() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v=new BasicSymbolVector(2);
        v.set(0,new BasicString("true"));
        v.set(1,new BasicString(""));
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
    }

    @Test
    public void test_arrayvector_diff_length_compress()throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT,"admin","123456");
        conn.run("share table(100:0,`id`arryInt`arrayDouble`arrayDate,[INT,INT[],DOUBLE[],DATE[]]) as trades");
        List<Vector> l = new ArrayList<Vector>();
        int time=1000;
        l.add(0,new BasicDoubleVector(new double[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicDoubleVector(i);
            v= (Vector) conn.run("double(1.."+i+")");
            l.add(i,v);
        }
        BasicArrayVector arrayDouble = new BasicArrayVector(l);

        l = new ArrayList<Vector>();
        l.add(0,new BasicIntVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicIntVector(i);
            v= (Vector) conn.run("int(1.."+i+")");
            l.add(i,v);
        }
        BasicArrayVector arryInt = new BasicArrayVector(l);

        l = new ArrayList<Vector>();
        l.add(0,new BasicDateVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicDateVector(i);
            v= (Vector) conn.run("date(1.."+i+")");

            l.add(i,v);
        }
        BasicArrayVector arryDate = new BasicArrayVector(l);

        List<String> colNames=new ArrayList<>();
        colNames.add(0,"id");
        colNames.add(1,"arryInt");
        colNames.add(2,"arrayDouble");
        colNames.add(3,"arrayDate");

        List<Vector> cols=new ArrayList<>();
        cols.add(0, (BasicIntVector)conn.run("1.."+time+""));
        cols.add(1,arryInt);
        cols.add(2,arrayDouble);
        cols.add(3,arryDate);
        BasicTable bt =new BasicTable(colNames,cols);
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{trades}", args);
        BasicTable res = (BasicTable) conn.run("select * from trades");
        for (int i=0;i<time;i++){
            assertEquals(arryInt.getVectorValue(i).getString(),((BasicArrayVector)bt.getColumn("arryInt")).getVectorValue(i).getString());
            assertEquals(arrayDouble.getVectorValue(i).getString(),((BasicArrayVector)bt.getColumn("arrayDouble")).getVectorValue(i).getString());
            assertEquals(arryDate.getVectorValue(i).getString(),((BasicArrayVector)bt.getColumn("arrayDate")).getVectorValue(i).getString());
        }
        conn.close();
    }

    @Test
    public void test_arrayvector_diff_length_withoutcompress()throws Exception {
        DBConnection conn = new DBConnection(false,false,false);
        conn.connect(HOST, PORT,"admin","123456");
        conn.run("share table(100:0,`id`arryInt`arrayDouble`arrayDate,[INT,INT[],DOUBLE[],DATE[]]) as trades");
        List<Vector> l = new ArrayList<Vector>();
        int time=1000;
        l.add(0,new BasicDoubleVector(new double[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicDoubleVector(i);
            v= (Vector) conn.run("double(1.."+i+")");
            l.add(i,v);
        }
        BasicArrayVector arrayDouble = new BasicArrayVector(l);

        l = new ArrayList<Vector>();
        l.add(0,new BasicIntVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicIntVector(i);
            v= (Vector) conn.run("int(1.."+i+")");
            l.add(i,v);
        }
        BasicArrayVector arryInt = new BasicArrayVector(l);

        l = new ArrayList<Vector>();
        l.add(0,new BasicDateVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicDateVector(i);
            v= (Vector) conn.run("date(1.."+i+")");

            l.add(i,v);
        }
        BasicArrayVector arryDate = new BasicArrayVector(l);

        List<String> colNames=new ArrayList<>();
        colNames.add(0,"id");
        colNames.add(1,"arryInt");
        colNames.add(2,"arrayDouble");
        colNames.add(3,"arrayDate");

        List<Vector> cols=new ArrayList<>();
        cols.add(0, (BasicIntVector)conn.run("1.."+time+""));
        cols.add(1,arryInt);
        cols.add(2,arrayDouble);
        cols.add(3,arryDate);
        BasicTable bt =new BasicTable(colNames,cols);
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{trades}", args);
        BasicTable res = (BasicTable) conn.run("select * from trades");
        for (int i=0;i<time;i++){
            assertEquals(arryInt.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arryInt")).getVectorValue(i).getString());
            assertEquals(arrayDouble.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arrayDouble")).getVectorValue(i).getString());
            assertEquals(arryDate.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arrayDate")).getVectorValue(i).getString());
        }
        conn.close();
    }

    @Test
    public void testCompresstable_include_arrayvector()throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect(HOST, PORT);
        String script="\n" +
                "\n" +
                "s=array(INT[],0,10);\n" +
                "for(i in 0:10000)\n" +
                "{\n" +
                "s.append!([1..4])\n" +
                "}\n" +
                "t=table(1..10000 as id ,s as arr) ;share t as st_tmp;\n" +
                "dbName='dfs://testarray'\n" +
                "login(`admin,`123456)\n" +
                "\tif(existsDatabase(dbName)){\n" +
                "\t\tdropDatabase(dbName)\n" +
                "\t}\n" +
                "\tdb=database(dbName, VALUE, 1..2,,'TSDB')\n" +
                "\tpt=createPartitionedTable(db,t,`pt,`id,,`id).append!(t)";
        conn.run(script);
        BasicIntVector arr= (BasicIntVector) conn.run("1..4");
        BasicTable bt= (BasicTable) conn.run("select * from loadTable('dfs://testarray',`pt)");
        for (int i=0;i<10000;i++) {
            assertEquals(arr.getString(), ((BasicArrayVector) bt.getColumn("arr")).getVectorValue(i).getString());
        }
        conn.close();
    }
    @Test
    public void test_BasicArrayVector_complex() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v = new BasicComplexVector(2);
        Scalar value = new BasicComplex(1.1,2.5);
        Scalar value2 = new BasicComplex(2.6,7.9);
        v.set(0,value);
        v.set(1,value2);
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[1.1+2.5i,2.6+7.9i]",obj.get(0).getString());
        assertEquals("[1.1+2.5i,2.6+7.9i]",obj.get(1).getString());
        Map<String,Entity> map = new HashMap<>();
        map.put("complexVector",obj);
        conn.upload(map);
        BasicArrayVector bav = (BasicArrayVector) conn.run("complexVector");
        assertEquals(new BasicComplex(1.1,2.5),bav.getVectorValue(0).get(0));
        assertEquals(new BasicComplex(2.6,7.9),bav.getVectorValue(0).get(1));
        assertEquals(new BasicComplex(1.1,2.5),bav.getVectorValue(1).get(0));
        assertEquals(new BasicComplex(2.6,7.9),bav.getVectorValue(1).get(1));
    }
    @Test
    public void test_BasicArrayVector_point() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        List<Vector> l = new ArrayList<>();
        Vector v = new BasicPointVector(2);
        Scalar p1 = new BasicPoint(0.8,1.9);
        Scalar p2 = new BasicPoint(4.7,6.2);
        v.set(0,p1);
        v.set(1,p2);
        l.add(0,v);
        l.add(1,v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertEquals("[(0.8, 1.9),(4.7, 6.2)]",obj.get(0).getString());
        assertEquals("[(0.8, 1.9),(4.7, 6.2)]",obj.get(1).getString());
        Map<String,Entity> map = new HashMap<>();
        map.put("complexVector",obj);
        conn.upload(map);
        BasicArrayVector bav = (BasicArrayVector) conn.run("complexVector");
        assertEquals(new BasicPoint(0.8,1.9),bav.getVectorValue(0).get(0));
        assertEquals(new BasicPoint(4.7,6.2),bav.getVectorValue(0).get(1));
        assertEquals(new BasicPoint(0.8,1.9),bav.getVectorValue(1).get(0));
        assertEquals(new BasicPoint(4.7,6.2),bav.getVectorValue(1).get(1));
    }
    @Test
    public void test_Function_get() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        int[] index = new int[]{0,1,2};
        Vector v=new BasicBooleanVector(3);
        v.set(0,new BasicBoolean(true));
        v.set(1,new BasicBoolean(false));
        v.set(2,new BasicBoolean(true));
        BasicArrayVector obj = new BasicArrayVector(index,v);
        assertEquals("[true]",obj.getVectorValue(1).getString());
        assertEquals("[false]",obj.getVectorValue(2).getString());
        assertTrue(obj.getString(1).contains("true"));
        assertNotNull(obj.getSubVector(index));
        assertFalse(obj.isNull(0));
        assertEquals("[true]",obj.get(1).getString());
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,obj.get(1).getDataCategory());
        assertEquals("interface com.xxdb.data.Entity",obj.getElementClass().toString());
        assertEquals("ARRAY",obj.getDataCategory().toString());
        try{
            obj.getUnitLength();
        }catch(RuntimeException re){
            assertEquals("BasicArrayVector.getUnitLength() not supported.",re.getMessage());
        }
    }

    @Test
    public void test_Function_set() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v = new BasicBooleanVector(2);
        v.set(0, new BasicBoolean(true));
        v.set(1, new BasicBoolean(false));
        l.add(0, v);
        l.add(1, v);
        BasicArrayVector obj = new BasicArrayVector(l);
        obj.setNull(1);
        Scalar value = new BasicBoolean(true);
        try {
            obj.set(1, value);
        } catch (RuntimeException re) {
            assertEquals("BasicArrayVector.set not supported.", re.getMessage());
        }
    }
    @Test
    public void test_Function() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        Vector v = new BasicBooleanVector(2);
        v.set(0, new BasicBoolean(true));
        v.set(1, new BasicBoolean(false));
        l.add(0, v);
        l.add(1, v);
        BasicArrayVector obj = new BasicArrayVector(l);
        assertNull(obj.combine(v));
        Scalar value = new BasicBoolean(true);
        assertEquals(0,obj.asof(value));
        AbstractExtendedDataOutputStream out = new LittleEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        try{
            obj.serialize(0,1,out);
        }catch(RuntimeException re){
            assertEquals("BasicAnyVector.serialize not supported.",re.getMessage());
        }
    }

    @Test
    public void test_serialize() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        List<Vector> l = new ArrayList<Vector>();
        int time=1000;
        l = new ArrayList<Vector>();
        l.add(0,new BasicDateVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicDateVector(i);
            v= (Vector) conn.run("date(1.."+i+")");

            l.add(i,v);
        }
        BasicArrayVector arryDate = new BasicArrayVector(l);
        assertEquals("[1970.01.02,1970.01.03,1970.01.04,1970.01.05,1970.01.06,1970.01.07,1970.01.08,1970.01.09,1970.01.10,1970.01.11,1970.01.12,1970.01.13,1970.01.14,1970.01.15,1970.01.16,1970.01.17,1970.01.18]",arryDate.getString(17));
        ByteBuffer bb = ByteBuffer.allocate(time);
        System.out.println(bb.remaining());
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(25,15);
        System.out.println(arryDate.serialize(0,1,71532,numElementAndPartial,bb));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicArrayVector_AppendScalar() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector bav = (BasicArrayVector) conn.run("j = arrayVector(9 13 16 18,14:10:50+0..17);j;");
        assertEquals(Entity.DATA_TYPE.DT_SECOND_ARRAY,bav.getDataType());
        bav.Append(new BasicSecond(51072));
    }

    @Test
    public void test_BasicArrayVector_AppendBasicIntVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector bav = (BasicArrayVector) conn.run("c = arrayVector(1 6 14 15,take(21..75,15));c;");
        int size = bav.rows();
        assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,bav.getDataType());
        bav.Append(new BasicIntVector(new int[]{78,92,15}));
        assertEquals(size+1,bav.rows());
    }

    @Test
    public void test_BasicArrayVector_AppendBasicBooleanVector() throws Exception {
        BasicBooleanVector bbv = new BasicBooleanVector(new boolean[]{true,true,true,false,false,true,false,false});
        BasicArrayVector bav = new BasicArrayVector(new int[]{2,4,8},bbv);
        assertEquals(3,bav.rows());
        bav.Append(new BasicBooleanVector(new boolean[]{false,true,false,true,true}));
        assertEquals(4,bav.rows());
        assertEquals("[false,true,false,true,true]",bav.getVectorValue(3).getString());
    }

    @Test
    public void test_BasicArrayVector_AppendBasicByteVector() throws Exception {
        BasicByteVector bbv = new BasicByteVector(new byte[]{71,32,44,39,42,122,95});
        BasicArrayVector bav = new BasicArrayVector(new int[]{1,4,7},bbv);
        System.out.println(bav.getString());
        bav.Append(new BasicByteVector(new byte[]{37,62,55}));
        assertEquals(4,bav.rows());
        assertEquals("['%','>','7']",bav.getVectorValue(3).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicArrayVector_String(){
        BasicArrayVector bav = new BasicArrayVector(Entity.DATA_TYPE.DT_STRING_ARRAY,3);
    }

    @Test
    public void test_BasicArrayVector_getSubVector_Null(){
        BasicArrayVector bav = new BasicArrayVector(new int[]{1,2,3,4},new BasicIntVector(new int[]{1,2,3,4}));
        assertNull(bav.getSubVector(new int[0]));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicArrayVector_add() throws Exception {
        BasicArrayVector bav = new BasicArrayVector(new int[]{1,2,3,4},new BasicIntVector(new int[]{1,2,3,4}));
        bav.add(new BasicInt(5));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicArrayVector_addRange(){
        BasicArrayVector bav = new BasicArrayVector(new int[]{1,2,3,4},new BasicIntVector(new int[]{2,4,6,8}));
        bav.addRange(new Object[]{new BasicInt(9),new BasicInt(17)});
    }

}
