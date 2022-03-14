package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.Test;

import javax.print.DocFlavor;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class BasicArrayVectorTest {
    private static final String HOST = "localhost";
    private static final Integer PORT = 8848;

    @Test
    public void TestBasicIntArrayVector() throws Exception {
         DBConnection conn = new DBConnection();
         conn.connect(HOST, PORT);
         BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(INT[], 0, 20)\n" +
                 "for(i in 1..20){\n" +
                 "\ta.append!([1..100])\n" +
                 "};a");
         BasicIntVector BV= (BasicIntVector) conn.run("1..100");
         for (int i=0;i<20;i++){
             assertEquals(BV.getString(),obj.getVectorValue(i).getString());
         }
         assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,obj.getDataType());
         assertEquals(20,obj.rows());
         conn.close();
    }

    @Test
    public void TestBasicLongArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(LONG[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100])\n" +
                "};a");
        BasicLongVector BV= (BasicLongVector) conn.run("long(1..100)");
        for (int i=0;i<20;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        assertEquals(Entity.DATA_TYPE.DT_LONG_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasicShortArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(SHORT[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100])\n" +
                "};a");
        BasicShortVector BV= (BasicShortVector) conn.run("short(1..100)");
        for (int i=0;i<20;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        assertEquals(Entity.DATA_TYPE.DT_SHORT_ARRAY,obj.getDataType());

        conn.close();
    }


    @Test
    public void TestBasic_Float_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(FLOAT[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100+0.7])\n" +
                "};a");
        BasicFloatVector BV= (BasicFloatVector) conn.run("float(1..100+0.7)");
        for (int i=0;i<20;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
        assertEquals(Entity.DATA_TYPE.DT_FLOAT_ARRAY,obj.getDataType());
        conn.close();

    }

    @Test
    public void TestBasic_DOUBLE_ArrayVector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT);
        BasicArrayVector obj = (BasicArrayVector)conn.run("a = array(DOUBLE[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100+0.7])\n" +
                "};a");
        BasicDoubleVector BV= (BasicDoubleVector) conn.run("double(1..100+0.7)");
        for (int i=0;i<20;i++){
            for (int j=0;j<100;j++) {
                assertEquals(BV.get(j), obj.getVectorValue(i).get(j));
            }
        }
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
        assertEquals("[0001.02M,0001.03M]",obj.getVectorValue(0).getString());
        assertEquals("[0001.01M]",obj.getVectorValue(1).getString());
        assertEquals("[]",obj.getVectorValue(2).getString());
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
        assertEquals(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,obj.getDataType());
        conn.close();
    }

    @Test
    public void TestBasicIntArrayVectorNULL() throws Exception {
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
    public void Test_new_BasicArrayVector() throws Exception {
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
        Vector v=new BasicIntVector(2);
        v.set(0,new BasicInt(1));
        v.set(1,new BasicInt(2));
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
        DBConnection conn = new DBConnection();
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
        DBConnection conn = new DBConnection();
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
        assertEquals(time,obj.getVectorValue(0).get(0).getNumber());
        assertEquals(time,obj.getVectorValue(0).get(1).getNumber());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals(time,res.getVectorValue(0).get(0).getNumber());
        assertEquals(time,res.getVectorValue(0).get(1).getNumber());
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
        assertEquals(LocalTime.of(1,1,1,1323433),obj.getVectorValue(0).get(0).getTemporal());
        assertEquals(LocalTime.of(1,1,1,1323433),obj.getVectorValue(1).get(0).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),obj.getVectorValue(0).get(1).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),obj.getVectorValue(1).get(1).getTemporal());

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals(LocalTime.of(1,1,1,1323433),res.getVectorValue(0).get(0).getTemporal());
        assertEquals(LocalTime.of(1,1,1,1323433),res.getVectorValue(1).get(0).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),res.getVectorValue(0).get(1).getTemporal());
        assertEquals(LocalTime.of(1,1,2,1323433),res.getVectorValue(1).get(1).getTemporal());
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
        assertEquals("[true,]",obj.getVectorValue(0).getString());
        assertEquals("[true,]",obj.getVectorValue(1).getString());
        conn.close();
       /* Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[true,]",res.getVectorValue(0).getString());
        assertEquals("[true,]",res.getVectorValue(1).getString());*/
    }

    @Test
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
        assertEquals("[true,]",obj.getVectorValue(0).getString());
        assertEquals("[true,]",obj.getVectorValue(1).getString());

      /*  Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("arrayvector", obj);
        conn.upload(map);
        BasicArrayVector res= (BasicArrayVector) conn.run("arrayvector");
        assertEquals("[true,]",res.getVectorValue(0).getString());
        assertEquals("[true,]",res.getVectorValue(1).getString());*/
    }

    @Test
    public void testCompresstable_include_arrayvector()throws Exception {
        DBConnection conn = new DBConnection(false,false,true);
        conn.connect("localhost", 8848);
        String script="\n" +
                "\n" +
                "s=array(INT[],0,10);\n" +
                "for(i in 0:10000)\n" +
                "{\n" +
                "s.append!([1..4])\n" +
                "}\n" +
                "t=table(1..10000 as id ,s as arr) ;share t as st;\n" +
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

}
