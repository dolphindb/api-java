package com.xxdb;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.io.Double2;
import com.xxdb.io.LittleEndianDataOutputStream;
import com.xxdb.io.Long2;
import com.xxdb.io.ProgressListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.Date;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.xxdb.comm.SqlStdEnum.*;
import static org.junit.Assert.*;

public class DBConnectionTest {


    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");

    static String[] host_list= bundle.getString("HOSTS").split(",");
    static String controller_host = bundle.getString("CONTROLLER_HOST");
    static int controller_port = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));

    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();
    private double load = -1.0;
    public int getConnCount() throws IOException {
        return ((BasicInt) conn.run("(exec connectionNum from rpc(getControllerAlias(),getClusterPerf) where port = getNodePort())[0]")).getInt();
    }
    static void compareBasicTable(BasicTable table, BasicTable newTable)
    {
        Assert.assertEquals(table.rows(), newTable.rows());
        Assert.assertEquals(table.columns(), newTable.columns());
        int cols = table.columns();
        for (int i = 0; i < cols; i++)
        {
            AbstractVector v1 = (AbstractVector)table.getColumn(i);
            AbstractVector v2 = (AbstractVector)newTable.getColumn(i);
            if (!v1.equals(v2))
            {
                for (int j = 0; j < table.rows(); j++)
                {
                    int failCase = 0;
                    AbstractScalar e1 = (AbstractScalar)table.getColumn(i).get(j);
                    AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                    if (e1.equals(e2) == false)
                    {
                        System.out.println("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                        failCase++;
                    }
                    Assert.assertEquals(0, failCase);
                }

            }
        }

    }
    @Before
    public void setUp() throws IOException {
        conn = new DBConnection(false,false,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
       conn.close();
    }

//    @Rule
//    public ExpectedException thrown= ExpectedException.none();
    @Test
    public void Test_DBConnection_1() throws IOException {
    DBConnection conn = new DBConnection(false,false,false,false,DolphinDB);
    boolean re = conn.connect(HOST,PORT,100,false);
    Assert.assertEquals(true,re);
    }
    @Test
    public void Test_DBConnection_Urgent_true() throws IOException {
        DBConnection conn = new DBConnection(false,false,false,false,true);
        boolean re = conn.connect(HOST,PORT,100,false);
        Assert.assertEquals(true,re);
    }
    @Test
    public void Test_DBConnection_Urgent_false() throws IOException {
        DBConnection conn = new DBConnection(false,false,false,false,false);
        boolean re = conn.connect(HOST,PORT,100,false);
        Assert.assertEquals(true,re);
    }
    //@Test
    public void daimaTest_Connect_timeout_1() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,111,1,true);
    }
    @Test
    public void Test_Connect_2() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,100,false);
        Assert.assertEquals(true,re);
    }
    @Test
    public void Test_Connect_3() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,ipports);
        Assert.assertEquals(true,re);
    }
    //@Test
    public void Test_Connect_4() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,null,null,ipports);
        Assert.assertEquals(true,re);
    }
    @Test
    public void Test_Connect_5() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,"","",ipports);
        Assert.assertEquals(true,re);
    }

    @Test
    public void Test_Connect_initialScript() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,"a=1",ipports);
        Assert.assertEquals(true,re);
        BasicInt re1 = (BasicInt)conn.run("a");
        Assert.assertEquals(1,re1.getInt());
    }

    @Test
    public void Test_Connect_initialScript_1() throws IOException {
        DBConnection conn = new DBConnection();
        boolean re = conn.connect(HOST,PORT,"admin","123456","a=1",ipports);
        Assert.assertEquals(true,re);
        BasicInt re1 = (BasicInt)conn.run("a");
        Assert.assertEquals(1,re1.getInt());
    }
    @Test
    public void testCharScalar() throws Exception {
        BasicByte scalar = (BasicByte) conn.run("'a'");
        assertEquals('a', ((BasicByte) scalar).getByte());
        assertEquals(0,((BasicByte) scalar).compareTo(scalar));
        assertEquals("'a'",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(97,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_BYTE,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-128,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testDurationScalar() throws Exception {
        String[] unitSyms = {"ns", "us", "ms", "s", "m", "H", "d", "w", "M", "y"};
        String[] DURATION =  {"NS", "US", "MS", "SECOND","MINUTE", "HOUR", "DAY", "WEEK","MONTH","YEAR"};
        for (int i=0;i<unitSyms.length;i++) {
            BasicDuration scalar =new BasicDuration(Entity.DURATION.valueOf(DURATION[i]),-5);
            BasicDuration scalar1 = (BasicDuration) conn.run("-5"+unitSyms[i]);
            assertEquals(scalar1, scalar);
            assertEquals(-5, ((BasicDuration) scalar).getDuration());
            assertEquals(Entity.DURATION.valueOf(DURATION[i]), ((BasicDuration) scalar).getUnit());
            assertEquals("-5"+unitSyms[i], scalar.getString());
            assertEquals(-5, scalar.getNumber().intValue());
            assertFalse(scalar.isMatrix());
            assertFalse(scalar.isNull());
            scalar.setNull();
            assertTrue(scalar.isNull());
            assertEquals(Entity.DATA_CATEGORY.SYSTEM, scalar.getDataCategory());
            assertEquals(Entity.DATA_TYPE.DT_DURATION, scalar.getDataType());
            assertEquals(0, scalar.hashBucket(1));
            BasicDuration scalar2 = (BasicDuration) conn.run("-4"+unitSyms[i]);
            assertFalse(scalar.equals(scalar2));
            //   ssertEquals(425918570, scalar.hashCode());
            try {
                scalar.getTemporal();
            } catch (Exception e) {
                assertTrue(e.getMessage().equals("Imcompatible data type"));
            }
        }
    }

    @Test
    public void testShortScalar() throws Exception {
        BasicShort scalar = (BasicShort) conn.run("11h");
        assertEquals(11, ((BasicShort) scalar).getShort());
        assertEquals(0,((BasicShort) scalar).compareTo(scalar));
        assertEquals("11",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(11,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_SHORT,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-32768,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testIntScalar() throws Exception {
        BasicInt scalar = (BasicInt) conn.run("6");
        assertEquals(6,  scalar.getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("6",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(6,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_INT,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testLongScalar() throws Exception {
        BasicLong scalar = (BasicLong) conn.run("22l");
        assertEquals(22,  scalar.getLong());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("22",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(22,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_LONG,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testDateScalar() throws Exception {
        BasicDate scalar = (BasicDate) conn.run("2013.06.13");
        assertEquals(15869, ((BasicDate) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("2013.06.13",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(15869,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DATE,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(2));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testMonthScalar() throws Exception {
        BasicMonth scalar = (BasicMonth) conn.run("2012.06M");
        assertEquals(24149, ((BasicMonth) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("2012.06M",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(24149,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_MONTH,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }
    @Test
    public  void testMonthScalar_1() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        BasicMonth data = (BasicMonth)conn.run("month(0)");
        System.out.println(data.getString());
        assertEquals("0000.01M",data.getString());

    }

    @Test
    public void testTimeScalar() throws Exception {
        BasicTime scalar = (BasicTime) conn.run("13:30:10.008");
        assertEquals(48610008, ((BasicTime) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("13:30:10.008",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(48610008,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_TIME,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testMinuteScalar() throws Exception {
        BasicMinute scalar = (BasicMinute) conn.run("13:30m");
        assertEquals(810, ((BasicMinute) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("13:30m",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(810,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_MINUTE,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testSecondScalar() throws Exception {
        BasicSecond scalar = (BasicSecond) conn.run("13:30:10");
        assertEquals(48610, ((BasicSecond) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("13:30:10",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(48610,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_SECOND,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testDateTimeScalar() throws Exception {
        BasicDateTime scalar = (BasicDateTime) conn.run("2012.06.13 13:30:10");
        assertEquals(1339594210, ((BasicDateTime) scalar).getInt());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("2012.06.13T13:30:10",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1339594210,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DATETIME,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testTimestampScalar() throws Exception {
        BasicTimestamp scalar = (BasicTimestamp) conn.run("2012.06.13 13:30:10.008");
        assertEquals(1339594210008l, ((BasicTimestamp) scalar).getLong());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("2012.06.13T13:30:10.008",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1339594210008l,scalar.getNumber().longValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_TIMESTAMP,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testNanoTimeScalar() throws Exception {
        BasicNanoTime scalar = (BasicNanoTime) conn.run("13:30:10.008007006");
        assertEquals(48610008007006l, ((BasicNanoTime) scalar).getLong());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("13:30:10.008007006",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(-431849122,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIME,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testNanoTimeStampScalar() throws Exception {
        BasicNanoTimestamp scalar = (BasicNanoTimestamp) conn.run("2012.06.13 13:30:10.008007006");
        assertEquals(1339594210008007006l, ((BasicNanoTimestamp) scalar).getLong());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("2012.06.13T13:30:10.008007006",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1339594210008007006l,scalar.getNumber().longValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIMESTAMP,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testStringScalar() throws Exception {
        BasicString scalar = (BasicString) conn.run("`IBM");
        assertEquals("IBM", ((BasicString) scalar).getString());
        assertEquals(0, scalar.compareTo(scalar));
        assertEquals("IBM",scalar.getString());
        assertTrue(scalar.equals(scalar));
        try {
            scalar.getNumber().intValue();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.LITERAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_STRING,scalar.getDataType());
        assertEquals(0,scalar.hashBucket(1));
        assertEquals(0,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
    }

    @Test
    public void testBooleanScalar() throws Exception {
        BasicBoolean scalar = (BasicBoolean) conn.run("true");
        assertEquals(true, scalar.getBoolean());
        assertEquals(0,scalar.compareTo(scalar));
        assertEquals("true",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_BOOL,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-128,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testFloatScalar() throws Exception {
        BasicFloat scalar = (BasicFloat) conn.run("1.2f");
        assertEquals(1.2, ((BasicFloat) scalar).getFloat(), 2);
        assertEquals(0,scalar.compareTo(scalar));
        assertEquals("1.20000005",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1.2f,scalar.getNumber().floatValue(),2);
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.FLOATING,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_FLOAT,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-8388609,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testDoubleScalar() throws Exception {
        BasicDouble scalar = (BasicDouble) conn.run("1.22");
        assertEquals(1.22, ((BasicDouble) scalar).getDouble(), 2);
        assertEquals(0,scalar.compareTo(scalar));
        assertEquals("1.22",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(1.22,scalar.getNumber().doubleValue(),2);
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.FLOATING,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DOUBLE,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(1048576,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testUuidScalar() throws Exception {
        BasicUuid scalar = (BasicUuid) conn.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')");
        assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee87", ((BasicUuid) scalar).getString());
        assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee87",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.BINARY,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_UUID,scalar.getDataType());
        assertEquals(0,scalar.hashBucket(1));
        assertEquals(0,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
        try {
            scalar.getNumber();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testDateHourScalar() throws Exception {
        BasicDateHour scalar = (BasicDateHour) conn.run("datehour(2012.06.13 13:30:10)");
        assertEquals(372109, ((BasicDateHour) scalar).getInt());
        assertEquals(0,scalar.compareTo(scalar));
        assertEquals("2012.06.13T13",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertEquals(372109,scalar.getNumber().intValue());
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DATEHOUR,scalar.getDataType());
        assertEquals(-1,scalar.hashBucket(1));
        assertEquals(-2147483648,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testIpAddrScalar() throws Exception {
        BasicIPAddr scalar = (BasicIPAddr) conn.run("ipaddr('192.168.1.13')");
        assertEquals("192.168.1.13", ((BasicIPAddr) scalar).getString());
        assertEquals("192.168.1.13",scalar.getString());
        assertTrue(scalar.equals(scalar));
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.BINARY,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_IPADDR,scalar.getDataType());
        assertEquals(0,scalar.hashBucket(1));
        assertEquals(0,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
        try {
            scalar.getNumber();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testInt128Scalar() throws Exception {
        BasicInt128 scalar = (BasicInt128) conn.run("int128('e1671797c52e15f763380b45e841ec32')");
        assertEquals("e1671797c52e15f763380b45e841ec32", ((BasicInt128) scalar).getString());
        assertEquals("e1671797c52e15f763380b45e841ec32",scalar.getString());
        assertTrue(scalar.equals(scalar));
        try {
            scalar.getNumber();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
        assertFalse(scalar.isMatrix());
        assertFalse(scalar.isNull());
        scalar.setNull();
        assertTrue(scalar.isNull());
        assertEquals(Entity.DATA_CATEGORY.BINARY,scalar.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_INT128,scalar.getDataType());
        assertEquals(0,scalar.hashBucket(1));
        assertEquals(0,scalar.hashCode());
        assertEquals("null",scalar.getJsonString());
        try {
            scalar.getTemporal();
        } catch (Exception e) {
            assertTrue(e.getMessage().equals("Imcompatible data type"));
        }
    }

    @Test
    public void testStringVector() throws Exception {
        BasicStringVector vector = (BasicStringVector) conn.run("`IBM`GOOG`YHOO");
        int size = vector.rows();
        assertEquals(3, size);
        BasicStringVector vectornull = (BasicStringVector) conn.run("[string(),string(),string()]");
        size = vectornull.rows();
        for (int i=0;i<3;i++){
            assertEquals("",vectornull.getString(i));
        }
        assertEquals(3, size);
        String[] l = {null,null};
        BasicStringVector v=new BasicStringVector(l);
    }

    @Test
    public void testFunctionDef() throws IOException {
        Entity obj = conn.run("def(a,b){return a+b}");
        assertEquals(Entity.DATA_TYPE.DT_FUNCTIONDEF, obj.getDataType());
        Entity AnonymousDef = conn.run("each(def(a,b):a+b, 1..10, 2..11);");
        int length = AnonymousDef.rows();
        assertEquals(10, length);
        StringBuilder sb = new StringBuilder();
        sb.append("a=100;");
        sb.append("g=add{a*a};");
        sb.append("g(8);");
        BasicInt res = (BasicInt) conn.run(sb.toString());
        assertEquals(10008, res.getInt());
        StringBuilder sb1 = new StringBuilder();
        sb1.append("def f(x):x pow 2 + 3*x + 4.0;");
        sb1.append("f(2);");
        BasicDouble lambda = (BasicDouble) conn.run(sb1.toString());
        assertEquals(14.0, lambda.getDouble(), 2);
        StringBuilder sb2 = new StringBuilder();
        sb2.append("g=def(a){return def(b): a pow b};");
        sb2.append("g(10)(5);");
        BasicDouble closed = (BasicDouble) conn.run(sb2.toString());
        assertEquals(100000.0, closed.getDouble(), 2);
        StringBuilder sb4 = new StringBuilder();
        sb4.append("x=[9,6,8];");
        sb4.append("def wage(x){if(x<=8) return 10*x; else return 20*x-80};");
        sb4.append("each(wage,x);");
        BasicIntVector each = (BasicIntVector) conn.run(sb4.toString());
        assertEquals(100, each.getInt(0));
        StringBuilder sb5 = new StringBuilder();
        sb5.append("t = table(1 2 3 as id, 4 5 6 as value,`IBM`MSFT`GOOG as name);");
        sb5.append("loop(max, t.values());");
        BasicAnyVector loop = (BasicAnyVector) conn.run(sb5.toString());
        assertEquals("3", loop.getEntity(0).getString());
    }


 /*   @Test(expected = AssertionError.class)

    public void testScriptOutOfRange() throws IOException {
        conn.run("rand(1..10,10000000000000);");
    }*/

    @Test
    public void testBoolVector() throws IOException {
        BasicBooleanVector vector = (BasicBooleanVector) conn.run("rand(1b 0b true false,10)");
        int size = vector.rows();
        assertEquals(10, size);
        byte[] l = {};
        BasicBooleanVector v=new BasicBooleanVector(l);
    }

    @Test
    public void testCharVector() throws IOException {
        BasicByteVector vector = (BasicByteVector) conn.run("rand('d' '1' '@',10)");
        int size = vector.rows();
        assertEquals(10, size);
        byte[] l = {};
        BasicByteVector v=new BasicByteVector(l);
    }


    @Test
    public void testDurationVector() throws Exception {
        BasicDurationVector Vector1 = (BasicDurationVector) conn.run("-5s:0s");
        BasicDurationVector Vector2 = (BasicDurationVector) conn.run("pair(-5s,0s)");
        BasicDurationVector Vector = new BasicDurationVector(2);
        Vector.set(0,new BasicDuration(Entity.DURATION.SECOND,-5));
        Vector.set(1,new BasicDuration(Entity.DURATION.SECOND,1));
        assertEquals(Vector1.get(1),Vector2.get(1) );
        assertEquals(Vector1.get(0), Vector.get(0));
        assertNotEquals(Vector1.get(1), Vector.get(1));
        assertFalse(Vector.isNull(1));
        Vector.setNull(1);
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,Vector.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DURATION,Vector.getDataType());
        assertEquals(BasicDuration.class,Vector.getElementClass());
        assertEquals(2,Vector.rows());
        try {
            Vector1.getSubVector(new int[]{0, 1});
        } catch (RuntimeException e) {}

        try {
            Vector1.asof(new BasicDuration(Entity.DURATION.SECOND,1));
        } catch (RuntimeException e) {}

        try {
            Vector1.combine(Vector);
        } catch (RuntimeException e) {}
        File f = new File("testDurationVector.txt");
        FileOutputStream fos = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        LittleEndianDataOutputStream dataStream = new LittleEndianDataOutputStream(bos);
        Vector.write(dataStream);
        bos.flush();
        dataStream.close();
        fos.close();
    }
    @Test
    public void testSymbolVector() throws IOException {
        BasicStringVector vector = (BasicStringVector) conn.run("rand(`IBM`MSFT`GOOG`BIDU,10)");
        int size = vector.rows();
        assertEquals(10, size);
        String[] l = {null,null};
        BasicStringVector v=new BasicStringVector(l);
    }

    @Test
    public void testIntegerVector() throws IOException {
        BasicIntVector vector = (BasicIntVector) conn.run("rand(10000,1000000)");
        int size = vector.rows();
        assertEquals(1000000, size);
        int[] l = {};
        BasicIntVector v=new BasicIntVector(l);
    }

    @Test
    public void testDoubleVector() throws IOException {
        BasicDoubleVector vector = (BasicDoubleVector) conn.run("rand(10.0,10)");
        int size = vector.rows();
        assertEquals(10, size);
        double[] l = {};
        BasicDoubleVector v=new BasicDoubleVector(l);
    }

    @Test
    public void testFloatVector() throws IOException {
        BasicFloatVector vector = (BasicFloatVector) conn.run("rand(10.0f,10)");
        int size = vector.rows();
        assertEquals(10, size);
        float[] l = {};
        BasicFloatVector v=new BasicFloatVector(l);
    }

    @Test
    public void testLongVector() throws IOException {
        BasicLongVector vector = (BasicLongVector) conn.run("rand(0l..11l,10)");
        int size = vector.rows();
        assertEquals(10, size);
        long[] l = {};
        BasicLongVector v=new BasicLongVector(l);
    }

    @Test
    public void testShortVector() throws IOException {
        BasicShortVector vector = (BasicShortVector) conn.run("rand(1h..22h,10)");
        int size = vector.rows();
        assertEquals(10, size);
        short[] l = {};
        BasicShortVector v=new BasicShortVector(l);
    }

    @Test
    public void testDateVector() throws IOException {
        BasicDateVector vector = (BasicDateVector) conn.run("2012.10.01 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicDateVector v=new BasicDateVector(l);
    }

    @Test
    public void testMonthVector() throws IOException {
        BasicMonthVector vector = (BasicMonthVector) conn.run("2012.06M +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicMonthVector v=new BasicMonthVector(l);
    }

    @Test
    public void testTimeVector() throws IOException {
        BasicTimeVector vector = (BasicTimeVector) conn.run("13:30:10.008 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicTimeVector v=new BasicTimeVector(l);
    }

    @Test
    public void testMinuteVector() throws IOException {
        BasicMinuteVector vector = (BasicMinuteVector) conn.run("13:30m +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicMinuteVector v=new BasicMinuteVector(l);
    }

    @Test
    public void testSecondVector() throws IOException {
        BasicSecondVector vector = (BasicSecondVector) conn.run("13:30:10 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicSecondVector v=new BasicSecondVector(l);
    }

    @Test
    public void testTimeStampVector() throws IOException {
        BasicTimestampVector vector = (BasicTimestampVector) conn.run("2012.06.13 13:30:10.008 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        long[] l = {};
        BasicTimestampVector v=new BasicTimestampVector(l);
    }

    @Test
    public void testNanoTimeVector() throws IOException {
        BasicNanoTimeVector vector = (BasicNanoTimeVector) conn.run("13:30:10.008007006 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        long[] l = {};
        BasicNanoTimeVector v=new BasicNanoTimeVector(l);
        
    }

    @Test
    public void testNanoTimeStampVector() throws IOException {
        BasicNanoTimestampVector vector = (BasicNanoTimestampVector) conn.run("2012.06.13 13:30:10.008007006 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
        long[] l = {};
        BasicNanoTimestampVector v=new BasicNanoTimestampVector(l);
    }

    @Test
    public void testDateTimeVector() throws IOException {

        BasicDateTimeVector vector = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicDateTimeVector v=new BasicDateTimeVector(l);
    }

    @Test
    public void testUuidVector() throws IOException {
        BasicUuidVector vector = (BasicUuidVector) conn.run("take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),10)");
        int size = vector.rows();
        assertEquals(10, size);
        Long2[] l = {};
        BasicUuidVector v=new BasicUuidVector(l);
    }

    @Test
    public void testDateHourVector() throws IOException {
        BasicDateHourVector vector = (BasicDateHourVector) conn.run("datehour('2012.06.13T13')+1..10");
        int size = vector.rows();
        assertEquals(10, size);
        int[] l = {};
        BasicDateHourVector v=new BasicDateHourVector(l);
    }

    @Test
    public void testIpAddrVector() throws IOException {
        BasicIPAddrVector vector = (BasicIPAddrVector) conn.run("rand(ipaddr('192.168.0.1'),10)");
        int size = vector.rows();
        assertEquals(10, size);
        Long2[] l = {};
        BasicIPAddrVector v=new BasicIPAddrVector(l);
    }

    @Test
    public void testInt128Vector() throws IOException {
        BasicInt128Vector vector = (BasicInt128Vector) conn.run("rand(int128('e1671797c52e15f763380b45e841ec32'),10)");
        int size = vector.rows();
        assertEquals(10, size);
        Long2[] l = {};
        BasicInt128Vector v=new BasicInt128Vector(l);
    }

    @Test
    public void testIntMatrix() throws IOException {
        BasicIntMatrix matrix = (BasicIntMatrix) conn.run("1..6$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testIntMatrixWithLabel() throws IOException {
        BasicIntMatrix matrix = (BasicIntMatrix) conn.run("cross(add,1..5,1..10)");
        assertEquals(5, matrix.rows());
        assertEquals(10, matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testTable() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("n=20000\n");
        sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
        sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
        sb.append("select qty,price from mytrades where sym==`IBM;");
        BasicTable table = (BasicTable) conn.run(sb.toString());

        Integer q = ((BasicInt) table.getColumn("qty").get(0)).getInt();
        assertTrue(table.rows() > 0);
        assertTrue(q > 10);
    }

    @Test
    public void testLongMatrix() throws IOException {
        BasicLongMatrix matrix = (BasicLongMatrix) conn.run("1l..6l$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testLongMatrixWithLabel() throws IOException {
        BasicLongMatrix matrix = (BasicLongMatrix) conn.run("cross(add,1l..5l,1l..10l)");
        assertEquals(5, matrix.rows());
        assertEquals(10, matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testShortMatrix() throws IOException {
        BasicShortMatrix matrix = (BasicShortMatrix) conn.run("1h..6h$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testShortMatrixWithLabel() throws IOException {
        BasicShortMatrix matrix = (BasicShortMatrix) conn.run("short(cross(add,1h..5h,1h..10h))");
        assertEquals(5, matrix.rows());
        assertEquals(10, matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testSymbolMatrix() throws IOException {
        BasicStringMatrix matrix = (BasicStringMatrix) conn.run("matrix(`SYMBOL,2,2, ,`T)");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
    }

    @Test(expected = IOException.class)
    public void testSymbolMatrixWithLabel() throws IOException {
            BasicStringMatrix matrix = (BasicStringMatrix) conn.run("cross(add,matrix(`SYMBOL,2,2, ,`T),matrix(`SYMBOL,2,2, ,`T))");
            assertEquals(0, matrix.rows());
            assertEquals(0, matrix.columns());
    }

    @Test
    public void testDoubleMatrix() throws IOException {
        BasicDoubleMatrix matrix = (BasicDoubleMatrix) conn.run("2.1 1.1 2.6$1:3");
        assertEquals(1, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testDoubleMatrixWithLabel() throws IOException {
        BasicDoubleMatrix matrix = (BasicDoubleMatrix) conn.run("cross(pow,2.1 5.0 4.88,1.0 9.6 5.2)");
        assertEquals(3, matrix.rows());
        assertEquals(3, matrix.columns());
        assertEquals("2.1", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testFloatMatrix() throws IOException {
        BasicFloatMatrix matrix = (BasicFloatMatrix) conn.run("2.1f 1.1f 2.6f$1:3");
        assertEquals(1, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testFloatMatrixWithLabel() throws IOException {
        BasicFloatMatrix matrix = (BasicFloatMatrix) conn.run("float(cross(pow,2.1f 5.0f 4.88f,1.0f 9.6f 5.2f))");

        assertEquals(3, matrix.rows());
        assertEquals(3, matrix.columns());
        assertEquals("2.0999999", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testBooleanMatrix() throws IOException {
        BasicBooleanMatrix matrix = (BasicBooleanMatrix) conn.run("rand(true false,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testBooleanMatrixWithLabel() throws IOException {
        BasicBooleanMatrix matrix = (BasicBooleanMatrix) conn.run("bool(cross(add,true false,false true))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("true", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testByteMatrix() throws IOException {
        BasicByteMatrix matrix = (BasicByteMatrix) conn.run("rand('q' '1' '*',6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testByteMatrixWithLabel() throws IOException {
        BasicByteMatrix matrix = (BasicByteMatrix) conn.run("cross(add,true false,false true)");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("true", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testDateHourMatrix() throws IOException {
        BasicDateHourMatrix matrix = (BasicDateHourMatrix) conn.run("rand(datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008]),6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testDateHourMatrixWithLabel() throws IOException {
        BasicDateHourMatrix matrix = (BasicDateHourMatrix) conn.run("datehour(cross(add,2012.06.15 15:32:10.158 2012.06.15 15:32:10.158,2012.06.15 17:30:10.008 2012.06.15 15:32:10.158))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("2012.06.15T15:32:10.158", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testMinuteMatrix() throws IOException {
        BasicMinuteMatrix matrix = (BasicMinuteMatrix) conn.run("rand(13:30m 16:19m,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testMinuteMatrixWithLabel() throws IOException {
        BasicMinuteMatrix matrix = (BasicMinuteMatrix) conn.run("minute(cross(add,13:30m 13:15m,14:30m 13:20m))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("13:30m", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testSecondMatrix() throws IOException {
        BasicSecondMatrix matrix = (BasicSecondMatrix) conn.run("rand(13:30:12 13:30:10,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testSecondMatrixWithLabel() throws IOException {
        BasicSecondMatrix matrix = (BasicSecondMatrix) conn.run("second(cross(add,13:30:12 13:30:10,13:30:12 13:30:10))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("13:30:12", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testMonthMatrix() throws IOException {
        BasicMonthMatrix matrix = (BasicMonthMatrix) conn.run("rand(2015.06M 2012.09M,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testMonthMatrixWithLabel() throws IOException {
        BasicMonthMatrix matrix = (BasicMonthMatrix) conn.run("month(cross(add,2015.06M 2012.09M,2015.06M 2012.09M))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("2015.06M", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testNanoTimeMatrix() throws IOException {
        BasicNanoTimeMatrix matrix = (BasicNanoTimeMatrix) conn.run("rand(17:30:10.008007006 17:35:10.008007006,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testNanoTimeMatrixWithLabel() throws IOException {
        BasicNanoTimeMatrix matrix = (BasicNanoTimeMatrix) conn.run("nanotime(cross(add,17:30:10.008007006 17:35:10.008007006,17:30:10.008007006 17:35:10.008007006))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("17:30:10.008007006", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testNanoTimestampMatrix() throws IOException {
        BasicNanoTimestampMatrix matrix = (BasicNanoTimestampMatrix) conn.run("rand(2014.06.13T13:30:10.008007006 2012.06.13T13:30:10.008007006,6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testNanoTimestampMatrixWithLabel() throws IOException {
        BasicNanoTimestampMatrix matrix = (BasicNanoTimestampMatrix) conn.run("nanotimestamp(cross(add,2012.06.13T13:30:10.008007006 2012.06.14T13:30:10.008007006,2012.06.13T13:30:10.008007006))");
        assertEquals(2, matrix.rows());
        assertEquals(1, matrix.columns());
        assertEquals("2012.06.13T13:30:10.008007006", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testDateMatrix() throws IOException {
        BasicDateMatrix matrix = (BasicDateMatrix) conn.run("rand(date([2013.06.13,2014.06.13]),6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testDateMatrixWithLabel() throws IOException {
        BasicDateMatrix matrix = (BasicDateMatrix) conn.run("date(cross(add,2013.06.13 2015.03.13,2012.06.15 2012.06.19))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("2013.06.13", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testDateTimeMatrix() throws IOException {
        BasicDateTimeMatrix matrix = (BasicDateTimeMatrix) conn.run("rand(datetime([2012.06.13T13:30:10,2012.06.13T16:30:10]),6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testDateTimeMatrixWithLabel() throws IOException {
        BasicDateTimeMatrix matrix = (BasicDateTimeMatrix) conn.run("datetime(cross(add,2012.06.13T13:30:10 2019.06.13T16:30:10,2013.06.13T13:30:10 2014.06.13T16:30:10))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("2012.06.13T13:30:10", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testTimeMatrix() throws IOException {
        BasicTimeMatrix matrix = (BasicTimeMatrix) conn.run("rand(time([13:31:10.008,12:30:10.008]),6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testTimeMatrixWithLabel() throws IOException {
        BasicTimeMatrix matrix = (BasicTimeMatrix) conn.run("time(cross(add,13:31:10.008 12:30:10.008,13:31:10.008 12:30:10.008))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("13:31:10.008", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testTimestampMatrix() throws IOException {
        BasicTimestampMatrix matrix = (BasicTimestampMatrix) conn.run("rand(timestamp([2012.06.13T13:30:10.008,2014.06.13T13:30:10.008]),6)$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testTimestampMatrixWithLabel() throws IOException {
        BasicTimestampMatrix matrix = (BasicTimestampMatrix) conn.run("timestamp(cross(add,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008))");
        assertEquals(2, matrix.rows());
        assertEquals(2, matrix.columns());
        assertEquals("2012.06.13T13:30:10.008", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testBasicTableSerialize() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("n=20000\n");
        sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
        sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
        sb.append("select qty,price from mytrades where sym==`IBM;");
        BasicTable table = (BasicTable) conn.run(sb.toString());

        File f = new File("F:\\tmp\\test.dat");
        FileOutputStream fos = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        LittleEndianDataOutputStream dataStream = new LittleEndianDataOutputStream(bos);
        table.write(dataStream);
        bos.flush();
        dataStream.close();
        fos.close();
    }

   /* @Test
    public void testBasicTableDeserialize() throws IOException {

        File f = new File("F:\\tmp\\test.dat");
        FileInputStream fis = new FileInputStream("F:\\tmp\\test.dat");
        BufferedInputStream bis = new BufferedInputStream(fis);
        LittleEndianDataInputStream dataStream = new LittleEndianDataInputStream(bis);
        short flag = dataStream.readShort();
        BasicTable table = new BasicTable(dataStream);
    }*/

    @Test
    public void testDictionary() throws IOException {
        BasicDictionary dict = (BasicDictionary) conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        assertEquals(3, dict.rows());
    }

    @Test
    public void testDictionaryUpload() throws IOException {
        Entity dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("dict", dict);
        conn.upload(map);
        Entity dict1 = conn.run("dict");
        assertEquals(3, dict1.rows());
    }
    @Test
    public void testDurationUpload() throws IOException {
        Entity duration = conn.run("duration(3XNYS)");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("duration", duration);
        conn.upload(map);
        Entity duration1 = conn.run("duration");
        assertEquals("3XNYS", duration1.getString());
    }

    @Test
    public void testFunction() throws IOException {
        List<Entity> args = new ArrayList<Entity>(1);
        double[] array = {1.5, 2.5, 7};
        BasicDoubleVector vec = new BasicDoubleVector(array);
        args.add(vec);
        Scalar result = (Scalar) conn.run("sum", args);
        assertEquals(11, ((BasicDouble) result).getDouble(), 2);
    }

    @Test
    public void testFunction1() throws IOException {
        Map<String, Entity> vars = new HashMap<String, Entity>();
        BasicDoubleVector vec = new BasicDoubleVector(3);
        vec.setDouble(0, 1.5);
        vec.setDouble(1, 2.5);
        vec.setDouble(2, 7);
        vars.put("a", vec);
        conn.upload(vars);
        Entity result = conn.run("accumulate(+,a)");
        assertEquals(11, ((BasicDoubleVector) result).getDouble(2), 1);
    }

    @Test
    public void testAnyVector() throws IOException, Exception {
        BasicAnyVector result = (BasicAnyVector) conn.run("(1, 2, (1,3, 5),(0.9, 0.8))");
        assertEquals(1, ((Scalar)result.get(0)).getNumber().intValue());

        result = (BasicAnyVector) conn.run("eachRight(def(x,y):x+y,1,(1,2,3.6))");
        assertEquals("2", result.get(0).getString());
    }

    @Test
    public void testAnyVector_Duration_Upload() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity bdv = conn.run("[duration(3XNYS),duration(3XNYS)]");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("duration", bdv);
        conn.upload(map);
        Entity duration1 = conn.run("duration");
        assertEquals("(3XNYS,3XNYS)", duration1.getString());
    }
    @Test
    public void testSet() throws IOException {
        BasicSet result = (BasicSet) conn.run("set(1+3*1..100)");
        assertEquals(Entity.DATA_TYPE.DT_INT, result.getDataType());
        assertEquals(Entity.DATA_FORM.DF_SET, result.getDataForm());

    }

    @Test
    public void testSetUpload() throws IOException {
        Entity set = conn.run("set(1+3*1..100)");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("set", set);
        conn.upload(map);
        Entity sets = conn.run("set");
        assertEquals(Entity.DATA_TYPE.DT_INT, sets.getDataType());
        assertEquals(Entity.DATA_FORM.DF_SET, sets.getDataForm());
    }

    @Test
    public void testChart() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("dates=(2012.01.01..2016.07.31)[def(x):weekday(x) between 1:5]\n");
        sb.append("chartData=each(cumsum,reshape(rand(10000,dates.size()*5)-4500, dates.size():5))\n");
        sb.append("chartData.rename!(dates, \"Strategy#\"+string(1..5))\n");
        sb.append("plot(chartData,,[\"Cumulative Pnls of Five Strategies\",\"date\",\"pnl\"],LINE)");
        BasicChart chart = (BasicChart) conn.run(sb.toString());
        assertTrue(chart.getTitle().equals("Cumulative Pnls of Five Strategies"));
        assertTrue(chart.isChart());
    }

    @Test
    public void testScalarUpload() throws IOException {
        Map<String, Entity> map = new HashMap<String, Entity>();
        BasicInt a = (BasicInt) conn.run("1");
        map.put("a", a);
        BasicFloat f = (BasicFloat) conn.run("1.1f");
        map.put("f", f);
        BasicDouble d = (BasicDouble) conn.run("1.1");
        map.put("d", d);
        BasicLong l = (BasicLong) conn.run("1l");
        map.put("l", l);
        BasicShort s = (BasicShort) conn.run("1h");
        map.put("s", s);
        BasicBoolean b = (BasicBoolean) conn.run("true");
        map.put("b", b);
        BasicByte c = (BasicByte) conn.run("'a'");
        map.put("c", c);
        BasicString str = (BasicString) conn.run("`hello");
        map.put("str", str);
        BasicDate date = (BasicDate) conn.run("2013.06.13");
        map.put("date", date);
        BasicMonth month = (BasicMonth) conn.run("2012.06M");
        map.put("month", month);
        BasicTime time = (BasicTime) conn.run("13:30:10.008");
        map.put("time", time);
        BasicMinute minute = (BasicMinute) conn.run("13:30m");
        map.put("minute", minute);
        BasicSecond second = (BasicSecond) conn.run("13:30:10");
        map.put("second", second);
        BasicDateTime dateTime = (BasicDateTime) conn.run("2012.06.13 13:30:10");
        map.put("dateTime", dateTime);
        BasicTimestamp timestamp = (BasicTimestamp) conn.run("2012.06.13 13:30:10.008");
        map.put("timestamp", timestamp);
        BasicNanoTime nanoTime = (BasicNanoTime) conn.run("13:30:10.008007006");
        map.put("nanoTime", nanoTime);
        BasicNanoTimestamp nanoTimestamp = (BasicNanoTimestamp) conn.run("2012.06.13T13:30:10.008007006");
        map.put("nanoTimestamp", nanoTimestamp);
        BasicUuid uuid = (BasicUuid) conn.run("uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\")");
        map.put("uuid", uuid);
        BasicDateHour dateHour = (BasicDateHour) conn.run("datehour(\"2012.06.13T13\")");
        map.put("dateHour", dateHour);
        BasicIPAddr ipAddr = (BasicIPAddr) conn.run("ipaddr(\"192.168.1.13\")");
        map.put("ipAddr", ipAddr);
        BasicInt128 int128 = (BasicInt128) conn.run("int128(\"e1671797c52e15f763380b45e841ec32\")");
        map.put("int128", int128);
        conn.upload(map);
        BasicInt scalarInt = (BasicInt) conn.run("a");
        assertEquals(1, scalarInt.getInt());
        BasicFloat scalarFloat = (BasicFloat) conn.run("f");
        assertEquals(1.1, scalarFloat.getFloat(), 2);
        BasicDouble scalarDouble = (BasicDouble) conn.run("d");
        assertEquals(1.1, scalarDouble.getDouble(), 2);
        BasicLong scalarLong = (BasicLong) conn.run("l");
        assertEquals(1, scalarLong.getLong());
        BasicShort scalarShort = (BasicShort) conn.run("s");
        assertEquals(1, scalarShort.getShort());
        BasicBoolean scalarBool = (BasicBoolean) conn.run("b");
        assertEquals(true, scalarBool.getBoolean());
        BasicByte scalarChar = (BasicByte) conn.run("c");
        assertEquals('a', scalarChar.getByte());
        BasicString scalarStr = (BasicString) conn.run("str");
        assertEquals("hello", scalarStr.getString());
        BasicDate scalarDate = (BasicDate) conn.run("date");
        assertEquals(15869, scalarDate.getInt());
        BasicMonth scalarMonth = (BasicMonth) conn.run("month");
        assertEquals(24149, scalarMonth.getInt());
        BasicTime scalarTime = (BasicTime) conn.run("time");
        assertEquals(48610008, scalarTime.getInt());
        BasicMinute scalarMinute = (BasicMinute) conn.run("minute");
        assertEquals(810, scalarMinute.getInt());
        BasicSecond scalarSecond = (BasicSecond) conn.run("second");
        assertEquals(48610, scalarSecond.getInt());
        BasicDateTime scalarDateTime = (BasicDateTime) conn.run("dateTime");
        assertEquals(1339594210, scalarDateTime.getInt());
        BasicTimestamp scalarTimeStamp = (BasicTimestamp) conn.run("timestamp");
        assertEquals(1339594210008l, scalarTimeStamp.getLong());
        BasicNanoTimestamp scalarNanoTimeStamp = (BasicNanoTimestamp) conn.run("nanoTimestamp");
        assertEquals(1339594210008007006l, scalarNanoTimeStamp.getLong());
        BasicUuid scalarUuid = (BasicUuid) conn.run("uuid");
        assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee87", scalarUuid.getString());
        BasicDateHour scalarDateHour = (BasicDateHour) conn.run("dateHour");
        assertEquals(372109, scalarDateHour.getInt());
        BasicIPAddr scalarIPaddr = (BasicIPAddr) conn.run("ipAddr");
        assertEquals("192.168.1.13", scalarIPaddr.getString());
        BasicInt128 scalarInt182 = (BasicInt128) conn.run("int128");
        assertEquals("e1671797c52e15f763380b45e841ec32", scalarInt182.getString());
    }

    @Test
    public void testVectorUpload() throws IOException {
        Map<String, Entity> map = new HashMap<String, Entity>();
        BasicBooleanVector boolv = (BasicBooleanVector) conn.run("rand(1b 0b true false,10)");
        BasicByteVector bytev = (BasicByteVector) conn.run("rand('d' '1' '@',10)");
        BasicStringVector stringv = (BasicStringVector) conn.run("take(`IBM`MSFT`GOOG`BIDU,10)");
        BasicIntVector intv = (BasicIntVector) conn.run("rand(1..10,10000)");
        BasicDoubleVector doublev = (BasicDoubleVector) conn.run("take(10.0,10)");
        BasicFloatVector floatV = (BasicFloatVector) conn.run("take(10.0f,10)");
        BasicLongVector longv = (BasicLongVector) conn.run("rand(1l..11l,10)");
        BasicShortVector shortv = (BasicShortVector) conn.run("take(22h,10)");
        BasicDateVector datev = (BasicDateVector) conn.run("2012.10.01 +1..10");
        BasicMonthVector monthv = (BasicMonthVector) conn.run("2012.06M +1..10");
        BasicTimeVector timev = (BasicTimeVector) conn.run("13:30:10.008 +1..10");
        BasicMinuteVector minutev = (BasicMinuteVector) conn.run("13:30m +1..10");
        BasicSecondVector secondv = (BasicSecondVector) conn.run("13:30:10 +1..10");
        BasicTimestampVector timestampv = (BasicTimestampVector) conn.run("2012.06.13 13:30:10.008 +1..10");
        BasicNanoTimeVector nanotimev = (BasicNanoTimeVector) conn.run("13:30:10.008007006 +1..10");
        BasicNanoTimestampVector nanotimestampv = (BasicNanoTimestampVector) conn.run("2012.06.13 13:30:10.008007006 +1..10");
        BasicDateTimeVector datetimev = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
        BasicUuidVector uuidv = (BasicUuidVector) conn.run("take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),98)");
        BasicDateHourVector datehourv = (BasicDateHourVector) conn.run("datehour('2012.06.13T13')+1..10");
        BasicIPAddrVector ipaddrv = (BasicIPAddrVector) conn.run("rand(ipaddr('192.168.0.1'),10)");
        BasicInt128Vector int128v = (BasicInt128Vector) conn.run("rand(int128('e1671797c52e15f763380b45e841ec32'),10)");
        map.put("boolv", boolv);
        map.put("bytev", bytev);
        map.put("stringv", stringv);
        map.put("intv", intv);
        map.put("doublev", doublev);
        map.put("floatV", floatV);
        map.put("longv", longv);
        map.put("shortv", shortv);
        map.put("datev", datev);
        map.put("monthv", monthv);
        map.put("timev", timev);
        map.put("minutev", minutev);
        map.put("secondv", secondv);
        map.put("timestampv", timestampv);
        map.put("nanotimev", nanotimev);
        map.put("nanotimestampv", nanotimestampv);
        map.put("datetimev", datetimev);
        map.put("uuidv", uuidv);
        map.put("datehourv", datehourv);
        map.put("ipaddrv", ipaddrv);
        map.put("int128v", int128v);
        conn.upload(map);
        BasicBooleanVector boolvRes = (BasicBooleanVector) conn.run("boolv");
        BasicByteVector bytevRes = (BasicByteVector) conn.run("bytev");
        BasicStringVector stringvRes = (BasicStringVector) conn.run("stringv");
        BasicIntVector intvRes = (BasicIntVector) conn.run("intv");
        BasicDoubleVector doublevRes = (BasicDoubleVector) conn.run("doublev");
        BasicFloatVector floatVRes = (BasicFloatVector) conn.run("floatV");
        BasicLongVector longvRes = (BasicLongVector) conn.run("longv");
        BasicShortVector shortvRes = (BasicShortVector) conn.run("shortv");
        BasicDateVector datevRes = (BasicDateVector) conn.run("datev");
        BasicMonthVector monthvRes = (BasicMonthVector) conn.run("monthv");
        BasicTimeVector timevRes = (BasicTimeVector) conn.run("timev");
        BasicMinuteVector minutevRes = (BasicMinuteVector) conn.run("minutev");
        BasicSecondVector secondvRes = (BasicSecondVector) conn.run("secondv");
        BasicTimestampVector timestampvRes = (BasicTimestampVector) conn.run("timestampv");
        BasicNanoTimeVector nanotimevRes = (BasicNanoTimeVector) conn.run("nanotimev");
        BasicNanoTimestampVector nanotimestampvRes = (BasicNanoTimestampVector) conn.run("nanotimestampv");
        BasicDateTimeVector datetimevRes = (BasicDateTimeVector) conn.run("datetimev");
        BasicUuidVector uuidvRes = (BasicUuidVector) conn.run("uuidv");
        BasicDateHourVector datehourvRes = (BasicDateHourVector) conn.run("datehourv");
        BasicIPAddrVector ipaddrvRes = (BasicIPAddrVector) conn.run("ipaddrv");
        BasicInt128Vector int128vRes = (BasicInt128Vector) conn.run("int128v");
        assertEquals(10, boolvRes.rows());
        assertEquals(10, bytevRes.rows());
        assertEquals(10000, intvRes.rows());
        assertEquals(10, stringvRes.rows());
        assertEquals(10, boolvRes.rows());
        assertEquals(10, doublevRes.rows());
        assertEquals(10, floatVRes.rows());
        assertEquals(10, longvRes.rows());
        assertEquals(10, shortvRes.rows());
        assertEquals(10, datevRes.rows());
        assertEquals(10, monthvRes.rows());
        assertEquals(10, timevRes.rows());
        assertEquals(10, minutevRes.rows());
        assertEquals(10, secondvRes.rows());
        assertEquals(10, timestampvRes.rows());
        assertEquals(10, nanotimevRes.rows());
        assertEquals(10, nanotimestampvRes.rows());
        assertEquals(10, datetimevRes.rows());
        assertEquals(98, uuidvRes.rows());
        assertEquals(10, datehourvRes.rows());
        assertEquals(10, ipaddrvRes.rows());
        assertEquals(10, int128vRes.rows());
    }

    @Test
    public void testMatrixUpload() throws IOException {
        Entity a = conn.run("cross(+, 1..5, 1..5)");
        Entity b = conn.run("1..25$5:5");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("a", a);
        map.put("b", b);
        conn.upload(map);
        Entity matrix = conn.run("a+b");
        assertEquals(5, matrix.rows());
        assertEquals(5, matrix.columns());
        assertTrue(((BasicIntMatrix) matrix).get(0, 0).getString().equals("3"));

        Entity matrixDoubleCross = conn.run("cross(pow,2.1 5.0 4.88,1.0 9.6 5.2)");
        Entity matrixDouble = conn.run("1..9$3:3");
        map.put("matrixDoubleCross", matrixDoubleCross);
        map.put("matrixDouble", matrixDouble);
        conn.upload(map);
        Entity matrixDoubleRes = conn.run("matrixDoubleCross + matrixDouble");
        assertEquals(3, matrixDoubleRes.rows());
        assertEquals(3, matrixDoubleRes.columns());
        assertTrue(((BasicDoubleMatrix) matrixDoubleRes).get(0, 0).getString().equals("3.1"));

        Entity matrixFloatCross = conn.run("cross(pow,2.1f 5.0f 4.88f,1.0f 9.6f 5.2f)");
        Entity matrixFloat = conn.run("take(2.33f,9)$3:3");
        map.put("matrixFloatCross", matrixFloatCross);
        map.put("matrixFloat", matrixFloat);
        conn.upload(map);
        Entity matrixFloatRes = conn.run("matrixFloatCross + matrixFloat");
        assertEquals(3, matrixFloatRes.rows());
        assertEquals(3, matrixFloatRes.columns());
        System.out.println(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString());
        assertTrue(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString().equals("4.42999983"));

        Entity matrixlc = conn.run("cross(+, 1l..6l, -6l..-1l)");
        Entity matrixl = conn.run("1l..36l$6:6");
        map.put("matrixlc", matrixlc);
        map.put("matrixl", matrixl);
        conn.upload(map);
        Entity matrixlRes = conn.run("matrixlc+matrixl");
        assertEquals(6, matrixlRes.rows());
        assertEquals(6, matrixlRes.columns());
        assertTrue(((BasicLongMatrix) matrixlRes).get(0, 0).getString().equals("-4"));

        Entity matrixBoolCross = conn.run("bool(cross(add,true false,false true))");
        Entity matrixBool = conn.run("true true false false$2:2");
        map.put("matrixBoolCross", matrixBoolCross);
        map.put("matrixBool", matrixBool);
        conn.upload(map);
        Entity matrixBoolRes = conn.run("matrixBoolCross+matrixBool");
        assertEquals(2, matrixBoolRes.rows());
        assertEquals(2, matrixBoolRes.columns());
        assertTrue(((BasicByteMatrix) matrixBoolRes).get(0, 0).getString().equals("2"));

        Entity matrixDateHourCross = conn.run("datehour(cross(add,2012.06.15 15:32:10.158 2012.06.15 15:32:10.158,2012.06.15 17:30:10.008 2012.06.15 15:32:10.158))");
        Entity matrixDateHour = conn.run("take(datehour([2012.06.15 15:32:10.158]),4)$2:2");
        map.put("matrixDateHourCross", matrixDateHourCross);
        map.put("matrixDateHour", matrixDateHour);
        conn.upload(map);
        Entity matrixDateHourRes = conn.run("datehour(matrixDateHourCross+matrixDateHour)");
        assertEquals(2, matrixBoolRes.rows());
        assertEquals(2, matrixBoolRes.columns());
        assertTrue(((BasicDateHourMatrix) matrixDateHourRes).get(0, 0).getString().equals("+55468.01.20T21"));

        Entity matrixMinuteCross = conn.run("minute(cross(add,13:30m 13:15m 13:17m,0 1 -1))");
        Entity matrixMinute = conn.run("take(13:30m,9)$3:3");
        map.put("matrixMinuteCross", matrixMinuteCross);
        map.put("matrixMinute", matrixMinute);
        conn.upload(map);
        Entity matrixMinuteCrossRes = conn.run("matrixMinuteCross");
        Entity matrixMinuteRes = conn.run("matrixMinute");
        assertEquals(3, matrixMinuteCrossRes.rows());
        assertEquals(3, matrixMinuteCrossRes.columns());
        assertEquals(3, matrixMinuteRes.rows());
        assertEquals(3, matrixMinuteRes.columns());
        assertTrue(((BasicMinuteMatrix) matrixMinuteRes).get(0, 0).getString().equals("13:30m"));
        assertTrue(((BasicMinuteMatrix) matrixMinuteCrossRes).get(2, 2).getString().equals("13:16m"));

        Entity matrixSecondCross = conn.run("second(cross(add,13:30:12 13:30:10,1 10))");
        Entity matrixSecond = conn.run("take(13:30:10,4)$2:2");
        map.put("matrixSecondCross", matrixSecondCross);
        map.put("matrixSecond", matrixSecond);
        conn.upload(map);
        Entity matrixSecondRes = conn.run("matrixSecondCross+matrixSecond");
        assertEquals(2, matrixSecondRes.rows());
        assertEquals(2, matrixSecondRes.columns());
        assertTrue(((BasicIntMatrix) matrixSecondRes).get(0, 0).getString().equals("97223"));

        Entity matrixMonthCross = conn.run("month(cross(add,2016.06M 2015.06M 2015.07M ,0 1 -1))");
        Entity matrixMonth = conn.run("take(2017.06M,9)$3:3");
        map.put("matrixMonthCross", matrixMonthCross);
        map.put("matrixMonth", matrixMonth);
        conn.upload(map);
        Entity matrixMonthRes = conn.run("month(matrixMonthCross+matrixMonth)");
        assertEquals(3, matrixMonthRes.rows());
        assertEquals(3, matrixMonthRes.columns());
        assertTrue(((BasicMonthMatrix) matrixMonthRes).get(0, 0).getString().equals("4033.11M"));

        Entity matrixNanoTimeCross = conn.run("nanotime(cross(add,17:30:10.008007006 17:35:10.008007006,1 10))");
        Entity matrixNanoTime = conn.run("take(17:30:10.008007006,4)$2:2");
        map.put("matrixNanoTimeCross", matrixNanoTimeCross);
        map.put("matrixNanoTime", matrixNanoTime);
        conn.upload(map);
        Entity matrixNanoTimeCrossRes = conn.run("matrixNanoTimeCross");
        Entity matrixNanoTimeRes = conn.run("matrixNanoTime");
        assertEquals(2, matrixNanoTimeCross.rows());
        assertEquals(2, matrixNanoTimeCross.columns());
        assertEquals(2, matrixNanoTime.rows());
        assertEquals(2, matrixNanoTime.columns());
        assertTrue(((BasicNanoTimeMatrix) matrixNanoTimeRes).get(0, 0).getString().equals("17:30:10.008007006"));
        assertTrue(((BasicNanoTimeMatrix) matrixNanoTimeCrossRes).get(1, 1).getString().equals("17:35:10.008007016"));

        Entity matrixNanotsCross = conn.run("nanotimestamp(cross(add,2012.06.13T13:30:10.008007006 2012.06.14T13:30:10.008007006,100 102 63))");
        Entity matrixNanots = conn.run("take(2012.06.13T13:30:10.008007006,6)$2:3");
        map.put("matrixNanotsCross", matrixNanotsCross);
        map.put("matrixNanots", matrixNanots);
        conn.upload(map);
        Entity matrixNanotsRes = conn.run("nanotimestamp(matrixNanotsCross+matrixNanots)");
        assertEquals(2, matrixNanotsRes.rows());
        assertEquals(3, matrixNanotsRes.columns());
        assertTrue(((BasicNanoTimestampMatrix) matrixNanotsRes).get(0, 1).getString().equals("2054.11.25T03:00:20.016014114"));

        Entity matrixDateCross = conn.run("date(cross(add,2013.06.13 2015.03.13,2 -2 6))");
        Entity matrixDate = conn.run("take(1998.06.13,6)$2:3");
        map.put("matrixDateCross", matrixDateCross);
        map.put("matrixDate", matrixDate);
        conn.upload(map);
        Entity matrixDateRes = conn.run("date(matrixDate+matrixDateCross)");
        assertEquals(2, matrixDateRes.rows());
        assertEquals(3, matrixDateRes.columns());
        assertTrue(((BasicDateMatrix) matrixDateRes).get(0, 1).getString().equals("2041.11.21"));

        Entity matrixDateTimeCross = conn.run("datetime(cross(add,2012.06.13T13:30:10 2019.06.13T16:30:10,0 5 9 -9))");
        Entity matrixDateTime = conn.run("take(2012.06.13T13:30:10,8)$2:4");
        map.put("matrixDateTimeCross", matrixDateTimeCross);
        map.put("matrixDateTime", matrixDateTime);
        conn.upload(map);
        Entity matrixDateTimeRes = conn.run("datetime(matrixDateTimeCross+matrixDateTime)");
        assertEquals(2, matrixDateTimeRes.rows());
        assertEquals(4, matrixDateTimeRes.columns());
        assertTrue(((BasicDateTimeMatrix) matrixDateTimeRes).get(1, 1).getString().equals("1925.10.18T23:32:09"));

        Entity matrixTimeCross = conn.run("time(cross(add,13:31:10.008 12:30:10.008,1 2 3 -4))");
        Entity matrixTime = conn.run("take(1900.06.13T13:30:10,8)$4:2");
        map.put("matrixTimeCross", matrixTimeCross);
        map.put("matrixTime", matrixTime);
        conn.upload(map);
        Entity matrixTimeCrossRes = conn.run("matrixTimeCross");
        Entity matrixTimeRes = conn.run("matrixTime");
        assertEquals(2, matrixTimeCross.rows());
        assertEquals(4, matrixTimeCross.columns());
        assertEquals(4, matrixTime.rows());
        assertEquals(2, matrixTime.columns());
        assertTrue(((BasicTimeMatrix) matrixTimeCrossRes).get(1, 1).getString().equals("12:30:10.010"));

        Entity matrixTimeStampCross = conn.run("timestamp(cross(add,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008,12 23 64))");
        Entity matrixTimeStamp = conn.run("take(2015.06.14T13:30:10.008,6)$2:3");
        map.put("matrixTimeStampCross", matrixTimeStampCross);
        map.put("matrixTimeStamp", matrixTimeStamp);
        conn.upload(map);
        Entity matrixTimeStampRes = conn.run("timestamp(matrixTimeStampCross+matrixTimeStamp)");
        assertEquals(2, matrixTimeStampRes.rows());
        assertEquals(3, matrixTimeStampRes.columns());
        assertTrue(((BasicTimestampMatrix) matrixTimeStampRes).get(1, 0).getString().equals("2059.11.25T03:00:20.028"));

    }

    @Test
    public void testStringMatrixUpload() throws IOException {
        HashMap<String, Entity> map = new HashMap<String, Entity>();
        Entity matrixString = conn.run("matrix(`SYMBOL,2,4, ,`T)");
        map.put("matrixString", matrixString);
        conn.upload(map);
        Entity matrixStringRes = conn.run("matrixString");
        assertEquals(2, matrixStringRes.rows());
        assertEquals(4, matrixStringRes.columns());
    }

    @Test
    public void testShortMatrixUpload() throws IOException {
        HashMap<String, Entity> map = new HashMap<String, Entity>();
        Entity matrixShort = conn.run("1h..36h$6:6");
        map.put("matrixShort", matrixShort);
        conn.upload(map);
        Entity matrixShortRes = conn.run("matrixShort");
        assertEquals(6, matrixShortRes.rows());
        assertEquals(6, matrixShortRes.columns());
        map = new HashMap<String, Entity>();
        Entity matrixShortCross = conn.run("cross(+, 1h..6h, -6h..-1h)");
        map.put("matrixShort", matrixShort);
        map.put("matrixShortCross", matrixShortCross);
        conn.upload(map);
        assertEquals(6, matrixShortCross.rows());
        assertEquals(6, matrixShortCross.columns());
        assertTrue(((BasicIntMatrix) matrixShortCross).get(0, 0).getString().equals("-5"));
    }

    @Test
    public void testUserDefineFunction() throws IOException {
        conn.run("def f(a,b) {return a+b};");
        List<Entity> args = new ArrayList<Entity>(2);
        BasicInt arg = new BasicInt(1);
        BasicInt arg2 = new BasicInt(2);
        args.add(arg);
        args.add(arg2);
        BasicInt result = (BasicInt) conn.run("f", args);
        assertEquals(3, result.getInt());
    }

    @Test
    public void testFunctionIntMatrix() throws Exception {
        int nrow = 5;
        int ncol = 5;
        List<int[]> data = new ArrayList<int[]>();
        for (int i = 0; i < ncol; ++i) {
            int[] array = IntStream.range(i * nrow, i * nrow + nrow).toArray();
            data.add(array);
        }
        BasicIntMatrix matrix = new BasicIntMatrix(nrow, ncol, data);
        BasicIntVector lables = new BasicIntVector(IntStream.range(1, nrow + 1).toArray());
        matrix.setRowLabels(lables);
        lables = new BasicIntVector(IntStream.range(1, ncol + 1).toArray());
        matrix.setColumnLabels(lables);

        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicIntVector vector = (BasicIntVector) conn.run("flatten", args);
        assertEquals(4, vector.getInt(4));
    }

    @Test
    public void testFunctionDoubleMatrix() throws Exception {
        int nrow = 5;
        int ncol = 5;
        List<double[]> data = new ArrayList<double[]>();
        for (int i = 0; i < ncol; ++i) {
            double[] array = DoubleStream.iterate(i * nrow, n -> n + 1).limit(nrow).toArray();
            data.add(array);
        }
        BasicDoubleMatrix matrix = new BasicDoubleMatrix(nrow, ncol, data);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicDoubleVector vector = (BasicDoubleVector) conn.run("flatten", args);
        Double re = vector.getDouble(4);
        assertEquals(3.0, re, 1);
    }

    @Test
    public void testFunctionStrMatrix() throws Exception {
        List<String[]> data = new ArrayList<String[]>();
        String[] array = new String[]{"test1", "test2", "test3"};
        data.add(array);
        array = new String[]{"test4", "test5", "test6"};
        data.add(array);

        BasicStringMatrix matrix = new BasicStringMatrix(3, 2, data);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicStringVector vector = (BasicStringVector) conn.run("flatten", args);
        String re = vector.getString(4);
        assertEquals("test5", re);
    }

    @Test
    public void Test_upload_table() throws IOException {
        BasicTable tb = (BasicTable) conn.run("table(1..100 as id,take(`aaa,100) as name)");
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_uploaded", (Entity) tb);
        conn.upload(upObj);
        BasicTable table = (BasicTable) conn.run("table_uploaded");
        assertEquals(100, table.rows());
        assertEquals(2, table.columns());
    }

    @Test
    public void testTableUpload() throws IOException {
        conn.run("try{" +
                "undef(`t1,SHARED);" +
                "undef(`t2,SHARED)" +
                "}" +
                "catch(ex){}");
        List<String> colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("value");
        colNames.add("x");

        List<Vector> cols = new ArrayList<Vector>();

        int[] intArray = new int[]{1, 2, 3, 4, 3};
        BasicIntVector vec = new BasicIntVector(intArray);
        cols.add(vec);

        double[] doubleArray = new double[]{7.8, 4.6, 5.1, 9.6, 0.1};
        BasicDoubleVector vecDouble = new BasicDoubleVector(doubleArray);
        cols.add(vecDouble);

        intArray = new int[]{5, 4, 3, 2, 1};
        vec = new BasicIntVector(intArray);
        cols.add(vec);

        BasicTable t1 = new BasicTable(colNames, cols);

        colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("qty");
        colNames.add("x");

        cols = new ArrayList<Vector>();
        intArray = new int[]{3, 1};
        vec = new BasicIntVector(intArray);
        cols.add(vec);

        short[] shortArray = new short[]{500, 800};
        BasicShortVector vecShort = new BasicShortVector(shortArray);
        cols.add(vecShort);

        doubleArray = new double[]{66.0, 88.0};
        vecDouble = new BasicDoubleVector(doubleArray);
        cols.add(vecDouble);
        BasicTable t2 = new BasicTable(colNames, cols);
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("t1", t1);
        map.put("t2", t2);
        conn.upload(map);
        conn.upload(map);
        BasicTable table = (BasicTable) conn.run("lj(t1, t2, `id)");

        assertEquals(5, table.rows());

    }

    @Test
    public void TestShareTable() throws IOException {
        conn.run("share table(1 2 as id ,3 4 as val) as sharedT1");
        BasicTable tb = (BasicTable) conn.run("select count(*) from sharedT1");
        BasicInt res = (BasicInt) conn.run("exec count(*) from sharedT1");
        assertEquals(1, tb.rows());
        assertEquals(2, res.getInt());

    }

    @Test
    public void test_partialFunction() throws IOException {
        conn.run("share table(1..50 as id) as sharedTable");
        int[] intArray = new int[]{30, 40, 50};
        List<Entity> args = Arrays.asList(new BasicIntVector(intArray));
        conn.run("tableInsert{sharedTable}", args);
        BasicTable re = (BasicTable) conn.run("sharedTable");
        assertEquals(53, re.rows());
    }

    @Test
    public void test_tableInsertPartialFunction() throws IOException {

        String sql = "v=1..5;table(2019.01.01 12:00:00.000+v as OPDATE, `sd`d`d`d`d as OPMODE, take(`ss,5) as tsymbol, 4+v as tint, 3+v as tlong, take(true,5) as tbool, 2.5+v as tfloat)";
        BasicTable data = (BasicTable) conn.run(sql);
        List<Entity> args = Arrays.asList(data);
        conn.run("tb=table(100:0,`OPDATE`OPMODE`tsymbol`tint`tlong`tbool`tfloat,[TIMESTAMP,STRING,SYMBOL,INT,LONG,BOOL,FLOAT])");
        BasicInt re = (BasicInt) conn.run("tableInsert{tb}", args);
        assertEquals(5, re.getInt());
    }


    @Test
    public void testUUID() throws IOException {
        String uuidStr = "92274dfe-d589-4598-84a3-c381592fdf3f";
        BasicUuid a = BasicUuid.fromString(uuidStr);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(a);
        BasicString re = (BasicString) conn.run("string", args);
        assertEquals("92274dfe-d589-4598-84a3-c381592fdf3f", re.getString());
    }

    @Test
    public void testIPADDR_V6() throws IOException {
        String ipv6Str = "aba8:f04:e12c:e0aa:b967:f4bf:481c:d400";
        BasicIPAddr b = BasicIPAddr.fromString(ipv6Str);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(b);
        BasicIPAddr reip = (BasicIPAddr) conn.run("ipaddr", args);
        BasicString re = (BasicString) conn.run("string", args);
        assertEquals("aba8:f04:e12c:e0aa:b967:f4bf:481c:d400", re.getString());
    }

    @Test
    public void testIPADDR_V4() throws IOException {
        String ipv4Str = "192.168.1.142";
        BasicIPAddr b = BasicIPAddr.fromString(ipv4Str);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(new BasicString(ipv4Str));
        BasicIPAddr reip = (BasicIPAddr) conn.run("ipaddr", args);
        assertEquals(ipv4Str, reip.getString());
    }

    @Test
    public void Test_ReLogin() throws IOException {
        //conn.login("admin", "123456", false);
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10);	t = table(1..100 as id);db.createPartitionedTable(t,'t1', 'id')");
        conn.run("logout()");
        try {
            conn.run("exec count(*) from loadTable('dfs://db1','t1')");
            BasicInt re = (BasicInt) conn.run("exec count(*) from loadTable('dfs://db1','t1')");
        } catch (IOException ex) {
            assertTrue(ServerExceptionUtils.isNotLogin(ex.getMessage()));
        }
    }
    @Test
    public void test_DBConnection_not_login() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(controller_host,controller_port);
        String re = null;
        try{
            conn.run("getGroupList();");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("getGroupList() => Only administrators execute function getGroupList' script: 'getGroupList();"));
    }
    @Test
    public void TestPartitionTable() throws IOException, InterruptedException {
        //createPartitionTable
        StringBuilder sb = new StringBuilder();
        sb.append("n=10000;");
        sb.append("t = table(rand(1 2 3,n)as id,rand(1..10,n)as val);");
        sb.append("if(existsDatabase('dfs://db1')){ dropDatabase('dfs://db1')}");
        sb.append("db = database('dfs://db1',VALUE ,1 2);");
        sb.append("pt = db.createPartitionedTable(t,`pt,`id).append!(t);");
        conn.run(sb.toString());
        BasicLong res = (BasicLong) conn.run("exec count(*) from pt");
        assertEquals(true, res.getLong() > 0);
        //addValuePartitions
        sb.append("addValuePartitions(db,3);");
        sb.append("pt.append!(t);");
        conn.run(sb.toString());
        BasicInt res3 = (BasicInt) conn.run("size(exec count(dfsPath) from pnodeRun(getAllChunks) where dfsPath like \"/db1/3%\" group by chunkId);");
        assertEquals(1, res3.getInt());
        //dropPartition
        conn.run("dropPartition(db,"+"\"/"+"3"+"/\""+",`pt);");
        BasicBoolean res4 = (BasicBoolean) conn.run("existsPartition('dfs://db1/3');");
        assertFalse(res4.getBoolean());
        //addColumn
        sb.append("addColumn(pt,[\"x\", \"y\"],[INT, INT]);");
        sb.append("t1 = table(rand(1 2 3 4,n) as id,rand(1..10,n) as val,rand(1..5,n) as x,rand(1..10,n) as y );");
        sb.append("pt.append!(t1);");
        conn.run(sb.toString());
        conn.run("pnodeRun(purgeCacheEngine)");
        Thread.sleep(5000);
        BasicLong res_x = (BasicLong) conn.run("exec count(*) from pt where x=1");
        assertEquals(true, res_x.getLong() > 0);
        //PartitionTableJoin
        sb.append("t2 = table(1 as id,2 as val);");
        sb.append("pt2 = db.createPartitionedTable(t2,`pt2,`id).append!(t2);");
        conn.run(sb.toString());
        BasicLong resej = (BasicLong) conn.run("exec count(*) from ej(pt,pt2,`id);");
        BasicLong respt = (BasicLong) conn.run("exec count(*) from pt;");
        assertEquals(true, resej.getLong() < respt.getLong());
    }

    @Test
    public void testConcurrent() {
        StringBuilder sb = new StringBuilder();
        sb.append("t=table(take(1..10, 10) as ID, take(1,10) as x);");
        sb.append("db=database(\"\", RANGE,  0 5 10);");
        sb.append("pt=db.createPartitionedTable(t, `pt, `ID);");
        sb.append("pt.append!(t)");
    }

    @Test
    public void TestConnectSuccess() {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("can not connect to dolphindb.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void TestConnectHostAndPortAreNull() {
        DBConnection conn1 = new DBConnection();
        try {
            if(!conn1.connect(null, -1, "admin", "123456"))
                throw new IllegalArgumentException("can not connect to dolphindb");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test(expected = UnknownHostException.class)
    public void TestConnectErrorHostFormat() throws IOException {
         DBConnection conn1 = new DBConnection();
         //thrown.expectMessage("fee");
         if(!conn1.connect("fee", PORT, "admin", "123456"))
             throw new UnknownHostException("can not connect to dolphindb.");
     }

    @Test(expected = IOException.class)
    public void TestConnectErrorHostValue() throws IOException {
        DBConnection conn1 = new DBConnection();
        conn1.connect("1", PORT, "admin", "123456");
        conn1.run("1+1");
    }

    @Test(expected = IOException.class)
    public  void TestConnectErrorPort() throws IOException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, 44, "admin", "123456");
        conn1.run("1+1");
    }

    @Test(expected = NullPointerException.class)
    public void TestConnectNullUserId() throws IOException {
        DBConnection conn1 = new DBConnection();
        if(!conn1.connect(HOST, PORT, null, "123456"))
            throw new NullPointerException();
    }

    @Test(expected = NullPointerException.class)
    public void TestConnectNullUserIdAndPwd() throws IOException {
        DBConnection conn1 = new DBConnection();
        if(!conn1.connect(HOST, PORT, null,""))
            throw new NullPointerException();
    }

    @Test(expected = IOException.class)
    public void TestConnectWrongPassWord() throws IOException {
        DBConnection conn1 = new DBConnection();
        if(!conn1.connect(HOST,PORT,"admin","111"))
            throw new IOException();
    }

    @Test
    public void TestCloseOnce() throws IOException, InterruptedException {
        DBConnection connClose = new DBConnection();
        //连接一次
        connClose.connect(HOST, PORT, "admin", "123456");
        Thread.sleep(5000);
        int connCount = getConnCount();
        connClose.close();
        Thread.sleep(5000);
        int connCount1 = getConnCount();
        assertEquals(connCount - 1, connCount1);
    }

    @Test
    public void TestClose() throws IOException, InterruptedException {
        //连接多次
        DBConnection connNew = new DBConnection();
        for (int i = 0; i < 5; i++) {
            try {
                if (!connNew.connect(HOST, PORT, "admin", "123456")) {
                    throw new IOException("Failed to connect to  server");
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }
        Thread.sleep(2000);
        int connCount = getConnCount();
        connNew.close();
        Thread.sleep(2000);
        int connCount1 = getConnCount();
        assertEquals(connCount - 1, connCount1);
    }

    @Test
    public void TestAddConn() throws IOException {
        List<DBConnection> connx = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            DBConnection cn = new DBConnection();
            cn.connect(HOST, PORT, "admin", "123456");
            connx.add(cn);
        }
    }

    @Test
    public void TestFetchDataInt() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(1..22486 as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(22486, data.rows());
        Assert.assertEquals(conn.run("table(1..22486 as id)").getString(),data.getString());
        EntityBlockReader vl = (EntityBlockReader) conn.run("table(long(1..22486) as id)", (ProgressListener) null, 4, 4, 20000);
        BasicTable datal = (BasicTable) vl.read();
        while (vl.hasNext()) {
            BasicTable t = (BasicTable) vl.read();
            datal = datal.combine(t);
        }
        Assert.assertEquals(22486, datal.rows());
        Assert.assertEquals(conn.run("table(long(1..22486) as id)").getString(),datal.getString());
        EntityBlockReader v2 = (EntityBlockReader) conn.run("table(short(1..22486) as id)", (ProgressListener) null, 4, 4, 20000);
        BasicTable data2 = (BasicTable) v2.read();
        while (v2.hasNext()) {
            BasicTable t = (BasicTable) v2.read();
            data2 = data2.combine(t);
        }
        Assert.assertEquals(22486, data2.rows());
        Assert.assertEquals(conn.run("table(short(1..22486) as id)").getString(),data2.getString());
    }

    @Test
    public void TestFetchDataFloat() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take([NULL,1.12,-99.0],200000) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200000, data.rows());
        Assert.assertEquals(conn.run("table(take([NULL,1.12,-99.0],200000) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataString() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take(`qq``11,200000) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200000, data.rows());
        Assert.assertEquals(conn.run("table(take(`qq``11,200000) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataChar() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take(['',' ','1'],200155) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200155, data.rows());
        Assert.assertEquals(conn.run("table(take(['',' ','1'],200155) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataBool() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take([false,true,NULL],200155) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200155, data.rows());
        Assert.assertEquals(conn.run("table(take([false,true,NULL],200155) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataTime() throws IOException {
        String []type={"time","date","month","minute","second","datetime","timestamp","nanotime","nanotimestamp","datehour"};
        for (int i=0;i<type.length;i++){
            EntityBlockReader v = (EntityBlockReader) conn.run("table(take(" + type[i] + "([1,NULL]),200155) as id)", (ProgressListener) null, 4, 4, 10000);
            BasicTable data = (BasicTable) v.read();
            while (v.hasNext()) {
                BasicTable t = (BasicTable) v.read();
                data = data.combine(t);
            }
            Assert.assertEquals(200155, data.rows());
            Assert.assertEquals(conn.run("table(take(" + type[i] + "([1,NULL]),200155) as id)").getString(), data.getString());
        }
    }

    @Test
    public void TestFetchDataUUid() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid()],200155) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200155, data.rows());
        Assert.assertEquals(conn.run("table(take([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid()],200155) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataIp() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take([ipaddr('192.168.1.13'),ipaddr()],200155) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200155, data.rows());
        Assert.assertEquals(conn.run("table(take([ipaddr('192.168.1.13'),ipaddr()],200155) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataInt128() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(take([int128('e1671797c52e15f763380b45e841ec32'),int128()],200155) as id)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(200155, data.rows());
        Assert.assertEquals(conn.run("table(take([int128('e1671797c52e15f763380b45e841ec32'),int128()],200155) as id)").getString(), data.getString());
    }

    @Test
    public void TestFetchDataPartition() throws IOException {
        String script="\n" +
                "n=1000000\n" +
                "ID=rand(100, n)\n" +
                "dates=2017.08.07..2017.08.11\n" +
                "date=rand(dates, n)\n" +
                "x=rand(10.0, n)\n" +
                "t1=table(ID, date, x);\n" +
                "dbDate = database(, VALUE, 2017.08.07..2017.08.11)\n" +
                "dbID=database(, RANGE, 0 50 100);\n" +
                "if(existsDatabase(\"dfs://compoDB\")){dropDatabase(\"dfs://compoDB\")}\n" +
                "db = database(\"dfs://compoDB\", COMPO, [dbDate, dbID])\n" +
                "pt = db.createPartitionedTable(t1, `pt, `date`ID).append!(t1)";
        conn.run(script);
        EntityBlockReader v = (EntityBlockReader) conn.run("select * from loadTable(\"dfs://compoDB\", `pt)", (ProgressListener) null, 4, 4, 10000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(1000000, data.rows());
        conn.run("dropDatabase(\"dfs://compoDB\")");
    }

    @Test(expected = IOException.class)
    public void TestFetchDataWithoutSkip() throws IOException {
        EntityBlockReader v = (EntityBlockReader)conn.run("table(1..12486 as id)",(ProgressListener) null,4,4,10000);
        BasicTable data = (BasicTable)v.read();
        //v.skipAll();
        BasicTable t1 = (BasicTable)conn.run("table(1..100 as id1)");
    }
    @Test
    public void TestFetchDataSkip() throws IOException {
        EntityBlockReader v = (EntityBlockReader)conn.run("table(1..12486 as id)",(ProgressListener) null,4,4,10000);
        BasicTable data = (BasicTable)v.read();
        v.skipAll();
        BasicTable t1 = (BasicTable)conn.run("table(1..100 as id1)");
    }

     @Test(expected = IOException.class)
     public void TestFetchSizeLessThan1892() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(1..22486 as id)", (ProgressListener) null, 4, 4, 8191);
     }

    @Test
    public void TestFetchBigData() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(1..50080000 as id)", (ProgressListener) null, 4, 4, 8200);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(50080000, data.rows());
    }
    @Test
    public void TestBigFetchSize() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(1..50000 as id)", (ProgressListener) null, 4, 4, 2000000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(50000, data.rows());
    }
    @Test
    public void TestFetchSizeEqRows() throws IOException {
        EntityBlockReader v = (EntityBlockReader) conn.run("table(1..20000 as id)", (ProgressListener) null, 4, 4, 20000);
        BasicTable data = (BasicTable) v.read();
        while (v.hasNext()) {
            BasicTable t = (BasicTable) v.read();
            data = data.combine(t);
        }
        Assert.assertEquals(20000, data.rows());
    }

    @Test
    public void TestgetRowJsonString() throws IOException {
        StringBuilder sb =new StringBuilder();
        sb.append("mytrades=table(take(`IBM`C`MS`MSFT`JPM`, 6) as sym,take(symbol(`IBM`C`MS`MSFT`JPM`), 6) as sym1)\n");
        sb.append("select * from mytrades");
        BasicTable table = (BasicTable)conn.run(sb.toString());
        String[] s = {"IBM","C","MS","MSFT","JPM",null};
        for (int i=0;i<s.length;i++) {
            String t = table.getRowJson(i);
            JSONObject json = new JSONObject().parseObject(t);
            assertEquals(s[i],json.getString("sym"));
            assertEquals(s[i],json.getString("sym1"));
        }

    }
    @Test
    public void TestgetRowJson() throws IOException {
        StringBuilder sb =new StringBuilder();
        sb.append("mytrades=table(take([1.01,5.2,0.6,-8.2,double()], 5) as v1" +
                ",[1,2,3,4,int()] as v2,take([1.01f,5.2f,0.6f,-8.2f,float()], 5)" +
                " as v3,take(['a','2',' ','#',char()],5) as v4,take([true,false,false,false,bool()],5) as v5)\n");
        sb.append("select * from mytrades");
        BasicTable table = (BasicTable)conn.run(sb.toString());
        Double[] darray ={1.01,5.2,0.6,-8.2,null};
        Float[] farray ={1.01f,5.2f,0.6f,-8.2f,null};
        String[] sarray={"a","2"," ","#","a"};
        boolean[] barray={true,false,false,false};
        for (int i =0;i<4;i++) {
            String t = table.getRowJson(i);
            JSONObject json = new JSONObject().parseObject(t);
            assertEquals(0, json.getDouble("v1").compareTo(darray[i]));
            assertEquals(0, json.getInteger("v2").compareTo(i+1));
            assertEquals(0, json.getFloat("v3").compareTo(farray[i]));
            assertEquals(0, json.getString("v4").compareTo(sarray[i]));
            assertEquals(0, json.getBoolean("v5").compareTo(barray[i]));
        }
        String t = table.getRowJson(4);
        JSONObject json = new JSONObject().parseObject(t);
        assertNull(json.getJSONObject("v1"));
        assertNull(json.getJSONObject("v2"));
        assertNull(json.getJSONObject("v3"));
        assertNull(json.getJSONObject("v4"));
        assertNull(json.getJSONObject("v5"));

    }
    @Test
    public void TestgetRowJsonTime() throws IOException {
        String[] type = {"time", "date", "month", "minute", "second", "datetime", "timestamp", "nanotime", "nanotimestamp", "datehour"};
        for (int i = 0; i < type.length; i++) {
            conn.run("t=table(take(" + type[i] + "([1,NULL]),2) as id)");
            String s = conn.run(type[i] +"(1)").toString();
            BasicTable table = (BasicTable)conn.run("select * from t ");
            String t = table.getRowJson(0);
            JSONObject json = new JSONObject().parseObject(t);
            assertEquals(0, json.getString("id").compareTo(s));
            t = table.getRowJson(1);
            json = new JSONObject().parseObject(t);
            assertNull(json.getJSONObject("id"));
        }
    }
    @Test
    public void TestgetRowJsonOtherTypes() throws IOException {
        conn.run("t=table(take([uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),uuid()],2) as val)");
        BasicTable table = (BasicTable)conn.run("select * from t ");
        String t = table.getRowJson(0);
        JSONObject json = new JSONObject().parseObject(t);
        assertEquals(0,json.getString("val").compareTo("5d212a78-cc48-e3b1-4235-b4d91473ee87"));
        t = table.getRowJson(1);
        json = new JSONObject().parseObject(t);
        assertNull(json.getJSONObject("val"));

        conn.run("t=table(take([ipaddr(\"192.168.1.13\"),ipaddr()],10) as val)");
        BasicTable table1 = (BasicTable)conn.run("select * from t ");
        t = table1.getRowJson(0);
        json = new JSONObject().parseObject(t);
        assertEquals(0,json.getString("val").compareTo("192.168.1.13"));
        t = table1.getRowJson(1);
        json = new JSONObject().parseObject(t);
        assertNull(json.getJSONObject("val"));

        conn.run("t=table(take([int128(\"e1671797c52e15f763380b45e841ec32\"),int128()],2) as val)");
        BasicTable table2 = (BasicTable)conn.run("select * from t ");
        t = table2.getRowJson(0);
        json = new JSONObject().parseObject(t);
        assertEquals(0,json.getString("val").compareTo("e1671797c52e15f763380b45e841ec32"));
        t = table2.getRowJson(1);
        json = new JSONObject().parseObject(t);
        assertNull(json.getJSONObject("val"));
    }
    @Test
    public void LongString() throws IOException {
      BasicString s= (BasicString) conn.run("t = table(1..10000000 as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,10000000) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,10000000) as name1);"+
              "t.toStdJson()");
                }

    @Test
    public void TestRunClearSessionMemory() throws IOException{
        boolean noErro=true;

        conn.tryRun("testVar=1", true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            String aa=ex.getMessage();
            System.out.println(ex.getMessage());
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.tryRun("testVar=table(1 as id,2 as val);", 4, 4, true);
        try {
            conn.run("print testVar", 4, 4);
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("\n" +
                "n=1000;" +
                "t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1)\n" +
                "testVar= t.toStdJson()");
        try {
            conn.run(" testVar", 4, 4);
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=1..100000000", true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=NULL", 4, true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=bool()", 4, 4, true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=float()", (ProgressListener) null, true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=`ghyib", (ProgressListener) null, 4, 4, true);
        try {
            conn.run("print testVar");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.run("testVar=1;a=testVar", (ProgressListener) null, 4, 4, 10000, true);
        try {
            conn.run("print a");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token a"))
                noErro=false;
        }
        assertTrue(noErro);
        conn.run("testsym=symbol(string(1..100000));testchar=`ftycdfty;t=table(1..10 as id,string(1..10) as str);testm=1..9$3:3;testp=1:8;testdic=dict(INT,DATE);testdic[1]=date(now());def f(b){a=b}", true);
        try {
            conn.run("testsym;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testsym"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testchar;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testchar"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("t;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token t"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testm;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testm"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testp;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testp"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testdic;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testdic"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("f(2);");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token f"))
                noErro=false;
        }
        assertTrue(noErro);

        conn.tryRun("testsym=char(1..100000);testchar=now();t=table(bool(1..10) as id,date(1..10) as str);testm=(1..9)$3:3;testp=1:8;testdic=dict(INT,DATE);testdic[1]=date(now());any=(1,1..10)", 4, 4, 10000, true);
        try {
            conn.run("testsym;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testsym"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testchar;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testchar"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("t;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token t"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testm;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testm"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testp;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testp"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testdic;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testdic"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("any;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token any"))
                noErro=false;
        }
        assertTrue(noErro);
        conn.run("testsym=[int(),int()];testchar=string();t=table(10:0,`id`var,[DATE,SYMBOL]);testm=matrix(INT,1,1);testp=pair(int(),int());testdic=dict(INT,DATE);any=();def f(b){}", true);
        try {
            conn.run("testsym;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testsym"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testchar;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testchar"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("t;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token t"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testm;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testm"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testp;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testp"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("testdic;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token testdic"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("any;");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token any"))
                noErro=false;
        }
        assertTrue(noErro);
        try {
            conn.run("f(2);");
        }
        catch (IOException ex){
            if(!ex.getMessage().contains("Syntax Error: [line #1] Cannot recognize the token f"))
                noErro=false;
        }
        assertTrue(noErro);
    }
    @Test
    public void testSymbolVectorUpload() throws IOException{
//        String script="share table(100:0,`val0`val1`val2`val3`val4`val5`val6`val7`val8`val9`val10`val11`val12`val13`val14`val15`val16`val17`val18`val19,[SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL]) as sharedTable";
        String script="share table(100:0,`val0`val1,[SYMBOL,SYMBOL]) as sharedTable";
        conn.run(script);
        List<String> colNames = new ArrayList<>(2);
        for (int i=0;i<2;i++){
            String s="val"+i;
            colNames.add(s);
        }
        String str[]= new String[10];
        for (int i=0;i<10;i+=5){
            str[i]="fdse";
            str[i+1]="";
            str[i+2]="tgufty";
            str[i+3]="fgew4";
            str[i+4]="ww";
        }
        BasicSymbolVector sb;
        List<Vector> cols = new ArrayList<>(2);
        for (int i=0;i<2;i++){
            sb = new BasicSymbolVector(Arrays.asList(str));
            cols.add(sb);
        }
        BasicTable table1 = new BasicTable(colNames,cols);
        List<Entity> arg = Arrays.asList(table1);
        conn.run("tableInsert{sharedTable}", arg);
        conn.close();
    }

    @Test
    public void test_BasicTable_upload() throws IOException{
        conn=new DBConnection();
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(2);
        date.setDate(1, LocalDate.now());
        date.setDate(0,LocalDate.now());
        BasicStringVector sym = new BasicStringVector(2);
        sym.setString(0,"w");
        sym.setString(1,"w");
        cols.add(date);
        cols.add(sym);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456",true)) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        String script="share table(100:0,`date`sym,[DATE,SYMBOL]) as sharedTable";
        conn.run(script);
        BasicInt i= (BasicInt) conn.run("exec count(*) from sharedTable");
        System.out.println("sum="+i);
        try {
            BasicTable t=  new BasicTable(colNames, cols);
            List<Entity> arg = Arrays.asList(t);
            conn.run("tableInsert{sharedTable}", arg);
            i= (BasicInt) conn.run("exec count(*) from sharedTable");
            assertEquals(2,i.getInt());
        }catch (IOException e){
            e.printStackTrace();
        }
        conn.close();
    }


    @Test(expected=Error.class)
    public void test_BasicTable_diff_col_Length() throws IOException{
        conn=new DBConnection();
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(2);
        date.setDate(1, LocalDate.now());
        date.setDate(0,LocalDate.now());
        BasicStringVector sym = new BasicStringVector(1);
        sym.setString(0,"w");
        // sym.setString(1,"w");
        cols.add(date);
        cols.add(sym);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456",true)) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        BasicTable t=  new BasicTable(colNames, cols);
    }

    @Test(expected=Error.class)
    public void test_BasicTable_colname_and_col_diff_Length() throws IOException{
        conn=new DBConnection();
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(2);
        date.setDate(1, LocalDate.now());
        date.setDate(0,LocalDate.now());
        cols.add(date);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456",true)) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        BasicTable t=  new BasicTable(colNames, cols);
    }

    @Test
    public void test_null_Vector_String() throws IOException {
        conn=new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script="\n" +
                "login('admin','123456');\n" +
                "dbName = \"dfs://test_null_Vector\";\n" +
                "if(existsDatabase(dbName)){dropDB(dbName)};\n" +
                "db = database(dbName,VALUE,date(1..2));\n" +
                "t=table(100:0,`date`sym,[DATE,SYMBOL]);\n" +
                "createPartitionedTable(db,t,`pt1,`date);" +
                "";
        conn.run(script);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(2);
        BasicStringVector sym = new BasicStringVector(2);
        date.setDate(0,LocalDate.now());
        date.setDate(1,LocalDate.now());
        sym.setString(0,null);
        sym.setString(1,null);
        cols.add(date);
        cols.add(sym);
        BasicTable t=  new BasicTable(colNames, cols);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(t);
        conn.run(String.format("tableInsert{loadTable('%s','pt1')}","dfs://test_null_Vector"), args);
        BasicTable re= (BasicTable) conn.run("select * from loadTable('dfs://test_null_Vector',`pt1)");
        List<Vector> excols = new ArrayList<Vector>(2);
        sym.setNull(0);
        sym.setNull(1);
        excols.add(date);
        excols.add(sym);
        BasicTable ext=  new BasicTable(colNames, cols);
        compareBasicTable(re,ext);
    }
    static long count = 0;
    @Test
    public void test_null_task_return(){
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                conn=new DBConnection();
                try {
                    conn.connect(HOST,PORT,"admin","123456");
                    conn.run("version()");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

   // @Test
    public void test_DBconnection_reconnect_false(){
        DBConnection conn_re = new DBConnection();
        try {
            conn_re.connect(HOST,0, "admin", "123456", null, false, null, false);
            conn_re.run("1+1");
            fail("no exception thrown");
        }catch(Exception ex){
            assertEquals("Couldn't send script/function to the remote host because the connection has been closed",ex.getMessage());
        }
    }

    //@Test
    public void test_DBconnection_reconnect_true() throws InterruptedException {
        DBConnection conn_re = new DBConnection();
        class reconnect extends TimerTask {
            public reconnect() {
            }

            @Override
            public void run() {
                try {
                    conn_re.connect(HOST, 0, "admin", "123456", null, false, null, true);
                    conn_re.run("1+1");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        Timer timer = new Timer("Timer");
        TimerTask rec = new reconnect();
        timer.scheduleAtFixedRate(rec, 0L, 500L);
        Thread.sleep(500L);
        timer.cancel();
    }

    //@Test
    public void test_asynchronousTask_true() throws IOException{
        DBConnection conn = new DBConnection(true);
        conn.connect(HOST,PORT,"admin","123456");
        long beforeDT = System.currentTimeMillis();
        conn.run("sleep(10000)");
        long afterDT = System.currentTimeMillis();
        assertFalse((afterDT - beforeDT) > 10000);

    }
    @Test
    public void test_asynchronousTask_false() throws IOException{
        DBConnection conn = new DBConnection(false);
        conn.connect(HOST,PORT,"admin","123456");
        long beforeDT = System.currentTimeMillis();
        conn.run("sleep(10000)");
        long afterDT = System.currentTimeMillis();
        assertFalse((afterDT - beforeDT) < 10000);

    }
    @Test
    public void test_connected() throws IOException{
        DBConnection conn = new DBConnection(false);
        assertTrue(conn.connect(HOST,PORT,"x=1 2 3"));
        assertTrue(conn.connected());
        assertFalse(conn.isBusy());
        conn.close();
        assertFalse(conn.isConnected());
    }
    @Test
    public void Test_getLocalAddress() throws IOException{
        DBConnection conn = new DBConnection(false,false);
        assertTrue(conn.connect(HOST,PORT,100));
        conn.login("admin","123456",true);
        InetAddress ip = InetAddress.getByName("127.0.0.1");
        assertEquals(ip,conn.getLocalAddress());
        System.out.println(conn.getSessionID());
    }

    @Test
    public void test_tryRun_script() throws IOException{
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT,"b=[1,2,3]",true));
        String scripts = "t0=table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);";
        conn.tryRun(scripts);
        BasicTable res = (BasicTable) conn.run("select * from t0 where a=1 and b=`x");
        assertEquals("10.8",res.getColumn(2).getString(0));
    }
    @Test
    public void test_tryRun_priority_parallelism() throws IOException {
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT,false));
        String scripts = "m=1..10$5:2;";
        conn.tryRun(scripts,1,1);
        BasicIntMatrix res = (BasicIntMatrix) conn.run("m;");
        assertEquals(10,res.getInt(4,1));
    }

    @Test
    public void test_TryRun_clearSessionMemory() throws IOException{
        DBConnection conn = new DBConnection();
        boolean noError = true;
        assertTrue(conn.connect(HOST,PORT,"admin","123456","x=5 3 6 2;"));
        conn.tryRun("testVar=1",1,1,true);
        try{
            conn.run("testVar",1,1);
        }catch(Exception e){
            String ex = e.getMessage();
            if(!ex.contains("Syntax Error: [line #1] Cannot recognize the token testVar"))
                noError = false;
            assertTrue(noError);
        }
        conn.tryRun("testm=-1;",1,1,false);
        BasicInt res = (BasicInt) conn.run("testm;",1,1);
        assertEquals(-1,res.getInt());
    }

    @Test
    public void test_TryRun_fetchSize() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();

        assertTrue(conn.connect(HOST,PORT,"admin","123456","m=matrix(1 2 3 4, 5 6 7 8, 9 10 11 12);",true));
        conn.run("x=1..10000000;");
        try {
            conn.tryRun("sum(x)", 1, 1, 8191,false);
        }catch(IOException ie){
            assertEquals("fetchSize must be greater than 8192",ie.getMessage());
        }
        long beforeDT = System.currentTimeMillis();
        conn.tryRun("x;",1,1,8192,true);
        long afterDT = System.currentTimeMillis();
        System.out.println(afterDT-beforeDT);
        Thread.sleep(5000);
        conn.run("y=1..10000000");
        long startDT = System.currentTimeMillis();
        conn.tryRun("y;",1,1,1000000,true);
        long endDT = System.currentTimeMillis();
        System.out.println(endDT-startDT);
        assertTrue((endDT-startDT)!=(afterDT-beforeDT));
    }
    @Test
    public void test_run_priority() throws IOException{
        DBConnection conn = new DBConnection(false,false);
        conn.connect(HOST,PORT);
        conn.run("t = table(1..5 as id, rand(100, 5) as price);",1);
        BasicTable res = (BasicTable) conn.run("select * from t;",2);
        assertEquals(5,res.rows());
        assertEquals(2,res.columns());
    }
    @Test
    public void test_run_clearSessionMemeory() throws IOException{
        DBConnection conn = new DBConnection(false,false);
        conn.connect(HOST,PORT);
        Boolean noError = true;
        conn.run("testVar=1",false);
        assertEquals("1",conn.run("testVar;",false).getString());
        conn.run("testM=2",true);
        try{
            conn.run("testM",true);
        }catch(Exception e){
            String ex = e.getMessage();
            if(!ex.contains("Syntax Error: [line #1] Cannot recognize the token testM"))
                noError = false;
            assertTrue(noError);
        }
    }
    @Test
    public void test_run_progressListener() throws IOException{
        DBConnection conn = new DBConnection(false,false);
        conn.connect(HOST,PORT);
        ProgressListener listener = new ProgressListener() {
            @Override
            public void progress(String message) {
                System.out.println(message);
            }
        };
        conn.run("t = table(1..5 as id, rand(100, 5) as price)", listener);
        BasicTable res = (BasicTable) conn.run("select * from t;",listener);
        assertEquals(5,res.rows());
        assertEquals(2,res.columns());
    }
    @Test
   public void test_tryUpload() throws IOException{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Boolean noError =true;
        BasicTable tb = (BasicTable) conn.run("table(1..100 as id,take(`aaa,100) as name)");
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_uploaded",tb);
        conn.tryUpload(upObj);
        BasicTable table = (BasicTable) conn.run("table_uploaded");
        assertEquals(100, table.rows());
        assertEquals(2, table.columns());

        BasicTable tb2 = (BasicTable) conn.run("t2=table(200:10, `name`id`value, [STRING,INT,DOUBLE]);t2;");
        try{
            upObj.put("Second uploaded",tb);
        }catch(RuntimeException re){
            String rex = re.getMessage();
            if(!rex.contains("is not a qualified variable name."))
                noError = false;
            assertTrue(noError);
        }
    }

@Test
public void test_SSL() throws Exception {
    DBConnection conn = new DBConnection(false,true);
    assertTrue(conn.connect(HOST,PORT));
    HashMap<String,Entity> map = new HashMap<>();
    map.put("x",conn.run("x=[1 3 6 10];"));
    map.put("y",conn.run("y=[2 1 5 3];"));
    map.put("z",conn.run("z=[3 11 8 7];"));
    map.put("table",conn.run("t=table(x,y,z);"));
    conn.upload(map);
    BasicTable res = (BasicTable) conn.run("t;");
    assertEquals(10,((Scalar)res.getColumn(0).get(3)).getNumber());
}

    @Test
    public void test_TryRun_arguments() throws Exception {
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT,"admin","123456"));
        List<Entity> arguments = new ArrayList<>();
        conn.run("a =[1 2 3];");
        arguments.add(conn.run("a;"));
        conn.run("b=[2 1 6];");
        arguments.add(conn.run("b;"));
        BasicTable res = (BasicTable) conn.tryRun("table",arguments);
        assertEquals(6,((Scalar)res.getColumn(1).get(2)).getNumber());
    }
    @Test
    public void test_TryRun_priority_parallelism() throws Exception {
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT,"admin","123456"));
        List<Entity> arguments = new ArrayList<>();
        conn.run("a =[1 2 3];");
        arguments.add(conn.run("a;"));
        conn.run("b=[2 1 6];");
        arguments.add(conn.run("b;"));
        Entity res = conn.run("table",arguments,1);
        assertEquals(2,res.columns());
        assertEquals(3,res.rows());
        List<Entity> args = new ArrayList<>();
        args.add(res);
        BasicTable res1 = (BasicTable) conn.tryRun("sum",args,1,1);
        assertEquals(9L,((Scalar)res1.getColumn(1).get(0)).getNumber());
    }

    @Test
    public void test_TryRun_args_fetchSize() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT,"admin","123456"));
        List<Entity> argus1 = new ArrayList<>();
        conn.run("x=1..10000000");
        argus1.add(conn.run("x;"));
        try {
            conn.tryRun("sum", argus1, 1, 1, 8191);
        }catch(IOException ie){
            assertEquals("fetchSize must be greater than 8192",ie.getMessage());
        }
        long beforeDT = System.currentTimeMillis();
        conn.tryRun("sum",argus1,1,1,8192);
        long afterDT = System.currentTimeMillis();
        System.out.println(afterDT-beforeDT);
        Thread.sleep(10000);
        List<Entity> argus2 = new ArrayList<>();
        conn.run("y=1..10000000");
        argus2.add(conn.run("y;"));
        long startDT = System.currentTimeMillis();
        conn.tryRun("sum",argus2,1,1,1000000);
        long endDT = System.currentTimeMillis();
        System.out.println(endDT-startDT);
        assertTrue((endDT-startDT)<(afterDT-beforeDT));
    }

    @Test
    public void test_getVersionNumber() throws IOException{
        DBConnection conn = new DBConnection();
        Class<DBConnection> dbc = DBConnection.class;
        try{
            Method method = dbc.getDeclaredMethod("getVersionNumber", String.class);
            method.setAccessible(true);
            Object obj = method.invoke(conn,"18.02 java");
            assertEquals(1802,obj);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void test_Node_Constructor() throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Object object = null;
        // 获取外部类所有的内部类
        Class<DBConnection> testDemoClass = DBConnection.class;
        Class<?>[] declaredClasses = testDemoClass.getDeclaredClasses();
        for(Class clazz : declaredClasses) {
            // 获取修饰符的整数编码
            int mod = clazz.getModifiers();
            // 返回整数编码对应的修饰符的字符串对象(private static)
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private static")) {
                // 获取内部类的名字(TestClass)，如果有过个内部类，可根据这个值分别判断
                String simpleName = clazz.getSimpleName();
                Constructor con = clazz.getConstructor(String.class,int.class,double.class);
                object = con.newInstance(HOST,PORT,1.0);
                Constructor con2 = clazz.getConstructor(String.class,int.class);
                Object obj = con2.newInstance(HOST,PORT);
//                Method method = clazz.getMethod("isEqual",clazz);
//                assertTrue((Boolean) method.invoke(object,obj));
                break;
            }
        }
    }
    @Test
    public void test_Node_Constructor2() throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Object object = null;
        // 获取外部类所有的内部类
        Class<DBConnection> testDemoClass = DBConnection.class;
        Class<?>[] declaredClasses = testDemoClass.getDeclaredClasses();
        for(Class clazz : declaredClasses) {
            // 获取修饰符的整数编码
            int mod = clazz.getModifiers();
            // 返回整数编码对应的修饰符的字符串对象(private static)
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private static")) {
                // 获取内部类的名字(TestClass)，如果有过个内部类，可根据这个值分别判断
                String simpleName = clazz.getSimpleName();
                Constructor con = clazz.getConstructor(String.class,double.class);
                object = con.newInstance(HOST+":"+PORT,1.0);
                Constructor con2 = clazz.getConstructor(String.class,int.class);
                Object obj = con2.newInstance(HOST,PORT);
//                Method method = clazz.getMethod("equals",clazz);
//                assertTrue((Boolean) method.invoke(object,obj));
                break;
            }
        }
    }

    @Test
    public void test_Node_Constructor3() throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Object object = null;
        // 获取外部类所有的内部类
        Class<DBConnection> testDemoClass = DBConnection.class;
        Class<?>[] declaredClasses = testDemoClass.getDeclaredClasses();
        for(Class clazz : declaredClasses) {
            // 获取修饰符的整数编码
            int mod = clazz.getModifiers();
            // 返回整数编码对应的修饰符的字符串对象(private static)
            String modifier = Modifier.toString(mod);
            if (modifier.contains("private static")) {
                // 获取内部类的名字(TestClass)，如果有过个内部类，可根据这个值分别判断
                String simpleName = clazz.getSimpleName();
                Constructor con = clazz.getConstructor(String.class);
                object = con.newInstance(HOST+":"+PORT);
                System.out.println(clazz.getName());
                Constructor con2 = clazz.getConstructor(String.class,int.class);
                Object obj = con2.newInstance(HOST,PORT);
////                Method method = clazz.getMethod("isEqual" +
//                        "" +
//                        "",clazz);
//                assertTrue((Boolean) method.invoke(object,obj));
                break;
            }
        }
    }

    @Test
    public void test_getRemoteLittleEndian() throws Exception{
        DBConnection conn = new DBConnection();
        assertTrue(conn.connect(HOST,PORT));
        assertTrue(conn.getRemoteLittleEndian());
        assertEquals(HOST,conn.getHostName());
        assertEquals(PORT,conn.getPort());
    }

    @Test
    public void test_append_into_partition_table() throws IOException{
        conn=new DBConnection();
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(2);
        date.setDate(1, LocalDate.now());
        date.setDate(0,LocalDate.now());
        BasicStringVector sym = new BasicStringVector(2);
        sym.setString(0,"w");
        sym.setString(1,"w");
        cols.add(date);
        cols.add(sym);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456",true)) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        String script="t = table(100:0,`date`sym,[DATE,SYMBOL]);" +
                "if(existsDatabase('dfs://test_append_pt')){\n" +
                "\tdropDB('dfs://test_append_pt')\n" +
                "}" +
                "db = database('dfs://test_append_pt',HASH,[DATE,4]);" +
                "pt = db.createPartitionedTable(t,`pt,`date);";
        conn.run(script);
        BasicInt i= (BasicInt) conn.run("exec count(*) from pt");
        System.out.println("sum="+i);
        try {
            BasicTable t=  new BasicTable(colNames, cols);
            List<Entity> arg = Arrays.asList(t);
            conn.run("append!{pt}", arg);
            i= (BasicInt) conn.run("exec count(*) from pt");
            assertEquals(2,i.getInt());
        }catch (IOException e){
            e.printStackTrace();
        }
        conn.close();
    }

    @Test
    public void test_Upload_AppendAllDataType() throws Exception {
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        bbv.Append(new BasicBoolean(false));
        System.out.println(bbv.rows());
        cols.add(bbv);
        colNames.add("cbool");
        BasicByteVector byv = new BasicByteVector(1);
        byv.add((byte) 22);
        byv.add((byte) 57);
        byv.Append(new BasicByte((byte) 13));
        System.out.println(byv.rows());
        cols.add(byv);
        colNames.add("cchar");
        BasicShortVector bsv = new BasicShortVector(1);
        bsv.add((short) 12);
        bsv.Append(new BasicShort((short) 35));
        bsv.add((short) 73);
        System.out.println(bsv.rows());
        cols.add(bsv);
        colNames.add("cshort");
        BasicIntVector biv = new BasicIntVector(1);
        biv.Append(new BasicInt(5));
        biv.add(11);
        biv.Append(new BasicInt(76));
        System.out.println(biv.rows());
        cols.add(biv);
        colNames.add("cint");
        BasicLongVector blv = new BasicLongVector(1);
        blv.add(12);
        blv.Append(new BasicLong(367));
        blv.Append(new BasicLong(23));
        System.out.println(blv.rows());
        cols.add(blv);
        colNames.add("clong");
        BasicDateVector bdv = new BasicDateVector(1);
        bdv.add(1);
        bdv.Append(new BasicDate(LocalDate.MIN));
        bdv.Append(new BasicDate(LocalDate.now()));
        System.out.println(bdv.rows());
        System.out.println(bdv.rows());
        cols.add(bdv);
        colNames.add("cdate");
        BasicMonthVector bmv = new BasicMonthVector(1);
        bmv.add(346);
        bmv.Append(new BasicMonth(2010,Month.APRIL));
        bmv.Append(new BasicMonth(2006,Month.MARCH));
        System.out.println(bmv.rows());
        cols.add(bmv);
        colNames.add("cmonth");
        BasicTimeVector btv = new BasicTimeVector(1);
        btv.add(2345);
        btv.Append(new BasicTimeVector(new int[]{46284,5839}));
        System.out.println(btv.rows());
        cols.add(btv);
        colNames.add("ctime");
        BasicMinuteVector bmiv = new BasicMinuteVector(1);
        bmiv.Append(new BasicMinuteVector(new int[]{749,904}));
        bmiv.add(432);
        System.out.println(bmiv.rows());
        cols.add(bmiv);
        colNames.add("cminute");
        BasicSecondVector bsev = new BasicSecondVector(1);
        bsev.add(17);
        bsev.Append(new BasicSecondVector(new int[]{4890,494}));
        System.out.println(bsev.rows());
        cols.add(bsev);
        colNames.add("csecond");
        BasicDateTimeVector bdtv = new BasicDateTimeVector(1);
        bdtv.Append(new BasicDateTimeVector(new int[]{49,242}));
        bdtv.add(25);
        System.out.println(bdtv.rows());
        cols.add(bdtv);
        colNames.add("cdatetime");
        BasicTimestampVector btsv = new BasicTimestampVector(1);
        btsv.Append(new BasicTimestampVector(new long[]{2839,480}));
        btsv.add(341);
        System.out.println(btsv.rows());
        cols.add(btsv);
        colNames.add("ctimestamp");
        BasicNanoTimeVector bntv = new BasicNanoTimeVector(1);
        bntv.add(521);
        bntv.Append(new BasicNanoTime(LocalTime.now()));
        bntv.Append(new BasicNanoTimeVector(new long[]{353566}));
        System.out.println(bntv.rows());
        cols.add(bntv);
        colNames.add("cnanotime");
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(1);
        bntsv.Append(new BasicNanoTimestampVector(new long[]{38297658492L}));
        bntsv.add(78900482747L);
        bntsv.Append(new BasicNanoTimestamp(LocalDateTime.MAX));
        System.out.println(bntsv.rows());
        cols.add(bntsv);
        colNames.add("cnanotimestamp");
        BasicFloatVector bfv = new BasicFloatVector(1);
        bfv.Append(new BasicFloatVector(new float[]{(float) 4580.02, (float) 394.3}));
        bfv.add((float) 5.981);
        System.out.println(bfv.rows());
        cols.add(bfv);
        colNames.add("cfloat");
        BasicDoubleVector bdbv = new BasicDoubleVector(1);
        bdbv.add(15.32);
        bdbv.Append(new BasicDoubleVector(new double[]{748.55}));
        bdbv.Append(new BasicDouble(7.17));
        System.out.println(bdbv.rows());
        cols.add(bdbv);
        colNames.add("cdouble");
        BasicStringVector bstv = new BasicStringVector(1);
        bstv.Append(new BasicStringVector(new String[]{"hello","abandon"}));
        bstv.add("lambada");
        System.out.println(bstv.rows());
        cols.add(bstv);
        colNames.add("cstring");
        BasicUuidVector buv = new BasicUuidVector(1);
        buv.add(new Long2(4758,1231));
        buv.Append(new BasicUuid(5890,943));
        buv.Append(new BasicUuidVector(new Long2[]{new Long2(9000,659)}));
        cols.add(buv);
        colNames.add("cuuid");
        System.out.println(buv.rows());
        BasicDateHourVector bdhv = new BasicDateHourVector(1);
        bdhv.Append(new BasicDateHourVector(new int[]{225,37}));
        bdhv.add(28);
        System.out.println(bdhv.rows());
        cols.add(bdhv);
        colNames.add("cdatehour");
        BasicIPAddrVector biav = new BasicIPAddrVector(1);
        biav.add(new Long2(231,489));
        biav.Append(new BasicIPAddrVector(new Long2[]{new Long2(34837,2938),new Long2(4794,95838)}));
        System.out.println(biav.rows());
        cols.add(biav);
        colNames.add("cipaddr");
        BasicInt128Vector bi128v = new BasicInt128Vector(1);
        bi128v.add(new Long2(384,390));
        bi128v.Append(new BasicInt128Vector(new Long2[]{new Long2(2719,3829),new Long2(849,49320)}));
        System.out.println(bi128v.rows());
        cols.add(bi128v);
        colNames.add("cint128");
        BasicComplexVector bcv = new BasicComplexVector(1);
        bcv.Append(new BasicComplexVector(new Double2[]{new Double2(23.04,3718.52),new Double2(37.23,25.12)}));
        bcv.add(new Double2(16.71,35.778));
        System.out.println(bcv.rows());
        cols.add(bcv);
        colNames.add("ccomplex");
        BasicPointVector bpv = new BasicPointVector(1);
        bpv.Append(new BasicPointVector(new Double2[]{new Double2(0.83,4.51),new Double2(33.16,49.71)}));
        bpv.add(new Double2(52.10,45.43));
        System.out.println(bpv.rows());
        cols.add(bpv);
        colNames.add("cpoint");
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(1,2);
        bd32v.add(35);
        bd32v.Append(new BasicDecimal32(17,2));
        bd32v.Append(new BasicDecimal32(25,2));
        System.out.println(bd32v.rows());
        cols.add(bd32v);
        colNames.add("cdecimal32");
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(1,4);
        bd64v.add(349);
        bd64v.Append(new BasicDecimal64(5372,4));
        bd64v.Append(new BasicDecimal64(2336,4));
        System.out.println(bd64v.rows());
        cols.add(bd64v);
        colNames.add("cdecimal64");

        BasicDecimal128Vector bd128v = new BasicDecimal128Vector(1,4);
        bd128v.add(new BigDecimal(349));
        bd128v.Append(new BasicDecimal128("5372",4));
        bd128v.Append(new BasicDecimal128("2336",4));
        System.out.println(bd128v.rows());
        cols.add(bd128v);
        colNames.add("cdecimal128");

        BasicTable bt = new BasicTable(colNames,cols);
        System.out.println(bt.getColumn("cdecimal128").getString());

        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",bt);
        conn.upload(map);
        BasicTable ta = (BasicTable) conn.run("testUpload;");
        assertEquals(4,ta.rows());
        assertEquals(26,ta.columns());
        conn.run("testUpload.clear!()");
        conn.tryUpload(map);
        BasicTable tua = (BasicTable) conn.run("testUpload");
        assertEquals(4,tua.rows());
        assertEquals(26,tua.columns());
        assertEquals(ta.getString(),tua.getString());
        conn.close();
    }

    @Test
    public void test_upload_WideTable() throws Exception {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        for(int i=0;i<1000;i++){
            colNames.add("id"+i);
            Vector v = null;
            if((i%6) == 0){
                v = new BasicIntVector(new int[]{i,i+5,i+10,i+15});
            }else if((i%6) == 1){
                v = new BasicDateVector(new int[]{i,i+105,i+110,i+115});
            }else if((i%6) == 2){
                v = new BasicComplexVector(new Double2[]{new Double2(i+0.1,i+0.2),new Double2(i+100.5,i-0.25),new Double2(i+1.35,i-0.75),new Double2(i+1.65,i-0.5)});
            }else if((i%6) == 3){
                v = new BasicDecimal32Vector(4,4);
                v.set(0,new BasicDecimal32(i+3,4));
                v.set(1,new BasicDecimal32(i+6,4));
                v.set(2,new BasicDecimal32(i+9,4));
                v.set(3,new BasicDecimal32(i+12,4));
            }else if((i%6) == 4){
                v = new BasicNanoTimeVector(new long[]{i+4,i+8,i+12,i+16});
            }else{
                v = new BasicByteVector(new byte[]{(byte) ('a'+i%6), (byte) ('e'+i%6), (byte) ('k'+i%6), (byte) ('q'+i%6)});
            }
            cols.add(v);
        }
        BasicTable bt = new BasicTable(colNames,cols);
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Map<String,Entity> map = new HashMap<>();
        map.put("wideTable",bt);
        conn.upload(map);
        BasicTable ua = (BasicTable) conn.run("wideTable;");
        assertEquals(1000,ua.columns());
        System.out.println(ua.getColumn("id15").getString());
    }
    @Test
    public void test_upload_WideTable_decimal128() throws Exception {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        for(int i=0;i<1000;i++){
            colNames.add("id"+i);
            Vector v = null;
            if((i%6) == 0){
                v = new BasicIntVector(new int[]{i,i+5,i+10,i+15});
            }else if((i%6) == 1){
                v = new BasicDateVector(new int[]{i,i+105,i+110,i+115});
            }else if((i%6) == 2){
                v = new BasicComplexVector(new Double2[]{new Double2(i+0.1,i+0.2),new Double2(i+100.5,i-0.25),new Double2(i+1.35,i-0.75),new Double2(i+1.65,i-0.5)});
            }else if((i%6) == 3){
                v = new BasicDecimal128Vector(4,4);
                v.set(0,new BasicDecimal128(String.valueOf(i+3),4));
                v.set(1,new BasicDecimal128(String.valueOf(i+6),4));
                v.set(2,new BasicDecimal128(String.valueOf(i+9),4));
                v.set(3,new BasicDecimal128(String.valueOf(i+12),4));
            }else if((i%6) == 4){
                v = new BasicNanoTimeVector(new long[]{i+4,i+8,i+12,i+16});
            }else{
                v = new BasicByteVector(new byte[]{(byte) ('a'+i%6), (byte) ('e'+i%6), (byte) ('k'+i%6), (byte) ('q'+i%6)});
            }
            cols.add(v);
        }
        BasicTable bt = new BasicTable(colNames,cols);
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Map<String,Entity> map = new HashMap<>();
        map.put("wideTable",bt);
        conn.upload(map);
        BasicTable ua = (BasicTable) conn.run("wideTable;");
        assertEquals(1000,ua.columns());
        System.out.println(ua.getColumn("id15").getString());
    }

    @Test
    public void test_tableInsert_decimal_arrayvector() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2`col3`col4`col5`col6, [DECIMAL32(0)[],DECIMAL32(1)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(1)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col4=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col5=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col6=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "t.tableInsert(col0,col1,col2,col3,col4,col5,col6)\n"+
                "\n" );
        BasicTable arr = (BasicTable)connection.run("t");
        System.out.println(arr.getString());
        List<Entity> ags = new ArrayList<>();
        ags.add(arr);
        connection.run("tableInsert{t}", ags);
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(7,res.columns());
        assertEquals(4,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000,3.000,99999.999],[-1.000,0.000,0.123],[1.000,3.000,99999.999],[-1.000,0.000,0.123]]",res.getColumn(2).getString());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(3).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(4).getString());
        assertEquals("[[1.0000,3.0000,99999.9999],[-1.0000,0.0000,0.1234],[1.0000,3.0000,99999.9999],[-1.0000,0.0000,0.1234]]",res.getColumn(5).getString());
        assertEquals("[[1.00000000,3.00001000,99999.99999999],[-1.00000000,0.00000000,0.12345678],[1.00000000,3.00001000,99999.99999999],[-1.00000000,0.00000000,0.12345678]]",res.getColumn(6).getString());
        System.out.println(res.getColumn(0).getString());

    }

    @Test
    public void test_tableInsert_decimal128_arrayvector() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2`col3, [DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(30)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "t.tableInsert(col0,col1,col2,col3)\n"+
                "\n" );
        BasicTable arr = (BasicTable)connection.run("t");
        System.out.println(arr.getString());
        List<Entity> ags = new ArrayList<>();
        ags.add(arr);
        connection.run("tableInsert{t}", ags);
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(4,res.columns());
        assertEquals(4,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.0000000000,3.0000100000,99999.9999999999],[-1.0000000000,0.0000000000,0.1234567890],[1.0000000000,3.0000100000,99999.9999999999],[-1.0000000000,0.0000000000,0.1234567890]]",res.getColumn(2).getString());
        assertEquals("[[1.000000000000000000000000000000,3.000010000000000339718860439552,99999.999999999978416622034208423936],[-1.000000000000000000000000000000,0.000000000000000000000000000000,0.123456788999999991885143736320],[1.000000000000000000000000000000,3.000010000000000339718860439552,99999.999999999978416622034208423936],[-1.000000000000000000000000000000,0.000000000000000000000000000000,0.123456788999999991885143736320]]",res.getColumn(3).getString());
        System.out.println(res.getColumn(0).getString());

    }
    @Test
    public void test_tableInsert_decimal_arrayvector_compress_true() throws Exception {
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2`col3`col4`col5`col6, [DECIMAL32(0)[],DECIMAL32(1)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(1)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col4=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col5=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col6=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "t.tableInsert(col0,col1,col2,col3,col4,col5,col6)\n"+
                "\n" );
        BasicTable arr = (BasicTable)connection.run("t");
        System.out.println(arr.getString());
        List<Entity> ags = new ArrayList<>();
        ags.add(arr);
        connection.run("tableInsert{t}", ags);
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(7,res.columns());
        assertEquals(4,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000,3.000,99999.999],[-1.000,0.000,0.123],[1.000,3.000,99999.999],[-1.000,0.000,0.123]]",res.getColumn(2).getString());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(3).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(4).getString());
        assertEquals("[[1.0000,3.0000,99999.9999],[-1.0000,0.0000,0.1234],[1.0000,3.0000,99999.9999],[-1.0000,0.0000,0.1234]]",res.getColumn(5).getString());
        assertEquals("[[1.00000000,3.00001000,99999.99999999],[-1.00000000,0.00000000,0.12345678],[1.00000000,3.00001000,99999.99999999],[-1.00000000,0.00000000,0.12345678]]",res.getColumn(6).getString());
        System.out.println(res.getColumn(0).getString());
    }
    @Test
    public void test_tableInsert_decimal128_arrayvector_compress_true() throws Exception {
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2`col3, [DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(30)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "t.tableInsert(col0,col1,col2,col3)\n"+
                "\n" );
        BasicTable arr = (BasicTable)connection.run("t");
        System.out.println(arr.getString());
        List<Entity> ags = new ArrayList<>();
        ags.add(arr);
        connection.run("tableInsert{t}", ags);
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(4,res.columns());
        assertEquals(4,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0],[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1],[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.0000000000,3.0000100000,99999.9999999999],[-1.0000000000,0.0000000000,0.1234567890],[1.0000000000,3.0000100000,99999.9999999999],[-1.0000000000,0.0000000000,0.1234567890]]",res.getColumn(2).getString());
        assertEquals("[[1.000000000000000000000000000000,3.000010000000000339718860439552,99999.999999999978416622034208423936],[-1.000000000000000000000000000000,0.000000000000000000000000000000,0.123456788999999991885143736320],[1.000000000000000000000000000000,3.000010000000000339718860439552,99999.999999999978416622034208423936],[-1.000000000000000000000000000000,0.000000000000000000000000000000,0.123456788999999991885143736320]]",res.getColumn(3).getString());
        System.out.println(res.getColumn(0).getString());

    }
    @Test
    public void test_insert_into_decimal_arrayvector() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2`col3`col4`col5`col6, [DECIMAL32(0)[],DECIMAL32(1)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(1)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col4=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col5=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col6=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "insert into ptt values(col0,col1,col2,col3,col4,col5,col6)\n"+
                "\n" );
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(7,res.columns());
        assertEquals(2,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000,3.000,99999.999],[-1.000,0.000,0.123]]",res.getColumn(2).getString());
        assertEquals("[[1,3,99999],[-1,0,0]]",res.getColumn(3).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(4).getString());
        assertEquals("[[1.0000,3.0000,99999.9999],[-1.0000,0.0000,0.1234]]",res.getColumn(5).getString());
        assertEquals("[[1.00000000,3.00001000,99999.99999999],[-1.00000000,0.00000000,0.12345678]]",res.getColumn(6).getString());

    }
    @Test
    public void test_insert_into_decimal128_arrayvector() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        connection.run("\n" +
                "t = table(1000:0, `col0`col1`col2, [DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(30)[]])\n" +
                "share t as ptt;\n"+
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n"+
                "insert into ptt values(col0,col1,col2)\n"+
                "\n" );
        BasicTable res = (BasicTable) connection.run("t");
        System.out.println(res.getString());
        assertEquals(3,res.columns());
        assertEquals(2,res.rows());
        assertEquals("[[1,3,99999],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,99999.9],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000000000000000000000000000000,3.000010000000000339718860439552,99999.999999999978416622034208423936],[-1.000000000000000000000000000000,0.000000000000000000000000000000,0.123456788999999991885143736320]]",res.getColumn(2).getString());
    }
    @Test
    public void TestConnectWithoutUserid() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
    }
    @Test
    public void Test_connect_EnableHighAvailability_false() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,CONTROLLER_PORT,"admin","123456",null,false);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        //DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        Thread.sleep(500);
        String e = null;
        try{
            conn1.run("a=1;\n a");
        }catch(Exception ex)
        {
            e = ex.getMessage();
            System.out.println(ex);
        }
        assertNotNull(e);
        Thread.sleep(1000);
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        conn1.run("a=1;\n a");
        assertEquals(true, conn1.isConnected());
        conn1.close();
    }
    @Test
    public void Test_connect_EnableHighAvailability_true() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,CONTROLLER_PORT,"admin","123456",null,false);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,true);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        assertEquals(true, conn1.isConnected());
    }
    //@Test //AJ-287
    public void Test_getConnection_highAvailability_false() throws SQLException, ClassNotFoundException, IOException {
        String script = "def restart(n)\n" +
                "{\n" +
                "try{\n" +
                "stopDataNode(\""+HOST+":"+PORT+"\");\n" +
                "}catch(ex)\n"+
                "{print(ex)}\n"+
                "sleep(n);\n"+
                "try{\n" +
                "stopDataNode(\""+HOST+":"+PORT+"\");\n" +
                "}catch(ex)\n"+
                "{print(ex)}\n"+
                "}\n" +
                "submitJob(\"restart\",\"restart\",restart,1000);";
        conn = new DBConnection(false, false, false);
        conn.connect(HOST, CONTROLLER_PORT, "admin", "123456",null,false,null,true);
        conn.run(script);
        DBConnection conn1 = new DBConnection(false, false, false);
        conn1.connect(HOST, PORT, "admin", "123456",null,false,null,true);
        conn1.close();
        conn.close();
    }

    @Test
    public void Test_connect_EnableHighAvailability_true_1() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,true);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        assertEquals(true, conn1.isConnected());
    }
    @Test
    public void Test_reConnect__true() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false,null,true);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        assertEquals(true, conn1.isConnected());
    }
    @Test //reConnect is not valid
    public void Test_reConnect__false() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false,null,false);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        Thread.sleep(1000);
        assertEquals(true, conn1.isConnected());
    }
    //@Test
    public void test_string_length()throws Exception{
        conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        Entity data = conn.run("select * from data1");
        //BasicString data=new BasicString("");
        Map<String, Entity> data1 = new HashMap<>();
        data1.put("da",data);
        conn.upload(data1);
    }
    //@Test
    public void test_string_length2()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");

        String d = "a";
        String dd = "";
        for(int i = 0; i < 262145; i++) {
            dd += d;
        }
        BasicString data = new BasicString(dd);
        Map<String, Entity> data1 = new HashMap<>();
        data1.put("da",data);
        conn.upload(data1);
    }
   //@Test
    public void test_BasicDBTask1223()throws Exception{
        conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("t = table(1:0, [`cint], [INT])");
        EntityBlockReader t = (EntityBlockReader)conn.run("t", (ProgressListener)null, 4, 2, 8192);
        conn.run("1");
    }


    @Test
    public void Test_Connect_SqlStdEnum_DolphinDB() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(DolphinDB);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("cumavg(1 2 3);");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());
        String e = null;
        try{
            BasicDate da = (BasicDate)conn.run("SYSDATE();");
        }catch(Exception E){
            e = E.getMessage();
        }
        assertNotNull(e);
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_DolphinDB_1() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'ddb')");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());
        String e = null;
        try{
            BasicDate da = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'ddb');");
        }catch(Exception E){
            e = E.getMessage();
        }
        assertNotNull(e);
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_DolphinDB_2() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(DolphinDB);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'ddb')");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());
        BasicDoubleVector ba1 = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'oracle');");
        System.out.println(ba1.getString());
        assertEquals("[1,1.5,2]", ba1.getString());
        String e = null;
        try{
            BasicDate da = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'ddb');");
        }catch(Exception E){
            e = E.getMessage();
        }
        assertNotNull(e);

        BasicDate da = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'oracle');");
        System.out.println(da.getString());
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime1 = formatter.format(currentTime);
        assertEquals(currentTime1.replace("-",""), da.toString().replace(".",""));

        BasicDate da1 = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'mysql');");
        System.out.println(da1.getString());
        Date currentTime2 = new Date();
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime3 = formatter2.format(currentTime2);
        assertEquals(currentTime3.replace("-",""), da1.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'oracle');");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_Oracle() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(Oracle);
        conn.connect(HOST,PORT);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("cumavg(1 2 3);");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da = (BasicDate)conn.run("SYSDATE();");
        System.out.println(da.getString());
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime1 = formatter.format(currentTime);
        assertEquals(currentTime1.replace("-",""), da.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("concat(string(1 2 3), string(4 5 6));");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_Oracle_1() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'oracle');");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'oracle');");
        System.out.println(da.getString());
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime1 = formatter.format(currentTime);
        assertEquals(currentTime1.replace("-",""), da.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'oracle');");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_Oracle_2() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(Oracle);
        conn.connect(HOST,PORT);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'ddb');");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da1 = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'mysql');");
        System.out.println(da1.getString());
        Date currentTime2 = new Date();
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime3 = formatter2.format(currentTime2);
        assertEquals(currentTime3.replace("-",""), da1.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'oracle');");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_MySQL() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(MySQL);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("cumavg(1 2 3);");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da = (BasicDate)conn.run("SYSDATE();");
        System.out.println(da.getString());
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime1 = formatter.format(currentTime);
        assertEquals(currentTime1.replace("-",""), da.toString().replace(".",""));

        String e = null;
        try{
            BasicStringVector ca = (BasicStringVector)conn.run("concat(string(1 2 3), string(4 5 6));");
        }catch(Exception E){
            e = E.getMessage();
        }
        assertNotNull(e);
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_MySQL_1() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'mysql');");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'mysql');");
        System.out.println(da.getString());
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime1 = formatter.format(currentTime);
        assertEquals(currentTime1.replace("-",""), da.toString().replace(".",""));

        String e = null;
        try{
            BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'mysql');");
        }catch(Exception E){
            e = E.getMessage();
        }
        assertNotNull(e);
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_MySQL_2() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(MySQL);
        conn.connect(HOST,PORT);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'ddb');");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da1 = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'mysql');");
        System.out.println(da1.getString());
        Date currentTime2 = new Date();
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime3 = formatter2.format(currentTime2);
        assertEquals(currentTime3.replace("-",""), da1.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'oracle');");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_SqlStdEnum_not_match() throws IOException, InterruptedException {
        String e = null;
        try{
            DBConnection conn = new DBConnection( getByName("fdfd"));
        }catch(Exception t){
            e=t.getMessage();
        }
        assertEquals(e,"No matching SqlStdEnum constant found for name: fdfd");
    }
    @Test
    public void Test_Connect_SqlStdEnum_getByName() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection( getByName("MySQL"));
        conn.connect(HOST,PORT);
        conn.connect(HOST,PORT);
        Thread.sleep(500);
        assertEquals(true, conn.isConnected());
        BasicDoubleVector ba = (BasicDoubleVector)conn.run("runSQL(\"cumavg(1 2 3)\", 'ddb');");
        System.out.println(ba.getString());
        assertEquals("[1,1.5,2]", ba.getString());

        BasicDate da1 = (BasicDate)conn.run("runSQL(\"SYSDATE()\", 'mysql');");
        System.out.println(da1.getString());
        Date currentTime2 = new Date();
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
        String currentTime3 = formatter2.format(currentTime2);
        assertEquals(currentTime3.replace("-",""), da1.toString().replace(".",""));

        BasicStringVector ca = (BasicStringVector)conn.run("runSQL(\"concat(string(1 2 3), string(4 5 6))\", 'oracle');");
        assertEquals("[14,25,36]", ca.getString());
        conn.close();
    }
    @Test
    public void Test_Connect_test() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        assertEquals(true, conn.isConnected());
        conn.close();
    }
    @Test
    public void test_DBConnection_tableInsert_illegal_string() throws IOException {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10,,'TSDB');t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]);db.createPartitionedTable(t,'t1', 'id',,'id')");
        List<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("string1");
        colName.add("symbol1");
        colName.add("blob1");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector trade_dt = new BasicIntVector(0);
        trade_dt.add(1);
        trade_dt.add(2);
        trade_dt.add(3);
        cols.add(trade_dt);
        BasicStringVector string1 = new BasicStringVector(0);
        string1.add("string1AM\000ZN/n0");
        string1.add("\000\00PL\0");
        string1.add("string1AM\0");
        cols.add(string1);
        BasicStringVector symbol1 = new BasicStringVector(0);
        symbol1.add("symbol1AM\0\0ZN");
        symbol1.add("\0symbol1AP\0PL\0");
        symbol1.add("symbol1AM\0");
        cols.add(symbol1);
        BasicStringVector blob1 = new BasicStringVector(0);
        blob1.add("blob1AM\0ZN");
        blob1.add("\0blob1AM\0ZN");
        blob1.add("blob1AMZN\0");
        cols.add(blob1);
        BasicTable tmpTable = new BasicTable(colName, cols);
        System.out.println(tmpTable.getString());
        List<Entity> args = new ArrayList<>();
        args.add(tmpTable);
        conn.run("tableInsert{loadTable(\"dfs://db1\", `t1)}", args);
        BasicTable table2 = (BasicTable) conn.run("select * from loadTable(\"dfs://db1\", `t1) ;\n");
        System.out.println(table2.getString());
        assertEquals("string1AM", table2.getColumn(1).get(0).getString());
        assertEquals("", table2.getColumn(1).get(1).getString());
        assertEquals("string1AM", table2.getColumn(1).get(2).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(0).getString());
        assertEquals("", table2.getColumn(2).get(1).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(2).getString());
        assertEquals("blob1AM", table2.getColumn(3).get(0).getString());
        assertEquals("", table2.getColumn(3).get(1).getString());
        assertEquals("blob1AMZN", table2.getColumn(3).get(2).getString());
    }
    @Test
    public void test_DBConnection_upload_illegal_string() throws IOException {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        List<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("string1");
        colName.add("symbol1");
        colName.add("blob1");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector trade_dt = new BasicIntVector(0);
        trade_dt.add(1);
        trade_dt.add(2);
        trade_dt.add(3);
        cols.add(trade_dt);
        BasicStringVector string1 = new BasicStringVector(0);
        string1.add("string1AM\000ZN/n0");
        string1.add("\000\00PL\0");
        string1.add("string1AM\0");
        cols.add(string1);
        BasicStringVector symbol1 = new BasicStringVector(0);
        symbol1.add("symbol1AM\0\0ZN");
        symbol1.add("\0symbol1AP\0PL\0");
        symbol1.add("symbol1AM\0");
        cols.add(symbol1);
        BasicStringVector blob1 = new BasicStringVector(0);
        blob1.add("blob1AM\0ZN");
        blob1.add("\0blob1AM\0ZN");
        blob1.add("blob1AMZN\0");
        cols.add(blob1);
        BasicTable tmpTable = new BasicTable(colName, cols);
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_uploaded", (Entity) tmpTable);
        conn.upload(upObj);
        BasicTable table2 = (BasicTable) conn.run("select * from table_uploaded;\n");
        System.out.println(table2.getString());
        assertEquals("string1AM", table2.getColumn(1).get(0).getString());
        assertEquals("", table2.getColumn(1).get(1).getString());
        assertEquals("string1AM", table2.getColumn(1).get(2).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(0).getString());
        assertEquals("", table2.getColumn(2).get(1).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(2).getString());
        assertEquals("blob1AM", table2.getColumn(3).get(0).getString());
        assertEquals("", table2.getColumn(3).get(1).getString());
        assertEquals("blob1AMZN", table2.getColumn(3).get(2).getString());
    }
    @Test
    public void test_connection_enableSeqNo_true() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456",
                "", true, ipports);
        connection.run("\n" +
                "dbPath = \"dfs://tableUpsert_test\"\n" +
                "if(existsDatabase(dbPath)){\n" +
                "\tdropDatabase(dbPath)\n" +
                "}\n" +
                "t = table(100:1, `id`id2`value, `INT`INT`INT)\n" +
                "db  = database(dbPath, RANGE,1 50 10000)\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id).append!(t)\n" +
                "pt = db.createPartitionedTable(t,`pt,`id).append!(t)");
        BasicTable ex = (BasicTable)connection.run("t=table( take(1..3000,3000) as id, 1..3000 as id2, 1..3000 as value);t;",null,4, 4, 0, false,"", true);
        for(int i = 0; i < 10; i++){
            connection.run("pt = loadTable(\"dfs://tableUpsert_test\",\"pt\"); pt.append!(t) ; pt1 = loadTable(\"dfs://tableUpsert_test\",\"pt1\"); pt1.append!(t) ;",null,4, 4, 0, false,"", true);
        }
        BasicTable res1 = (BasicTable) connection.run("select * from loadTable(\"dfs://tableUpsert_test\",\"pt\")");
        BasicTable res2 = (BasicTable) connection.run("select * from loadTable(\"dfs://tableUpsert_test\",\"pt1\")");
        assertEquals(30000, res1.rows());
        assertEquals(30000, res1.rows());
        connection.close();
    }

    @Test
    public void test_connection_enableSeqNo_false() throws Exception {
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456",
                "", true, ipports);
        connection.run("\n" +
                "dbPath = \"dfs://tableUpsert_test\"\n" +
                "if(existsDatabase(dbPath)){\n" +
                "\tdropDatabase(dbPath)\n" +
                "}\n" +
                "t = table(100:1, `id`id2`value, `INT`INT`INT)\n" +
                "db  = database(dbPath, RANGE,1 50 10000)\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id).append!(t)\n" +
                "pt = db.createPartitionedTable(t,`pt,`id).append!(t)");
        BasicTable ex = (BasicTable)connection.run("t=table( take(1..3000,3000) as id, 1..3000 as id2, 1..3000 as value);t;",null,4, 4, 0, false,"", false);
        for(int i = 0; i < 10; i++){
            connection.run("pt = loadTable(\"dfs://tableUpsert_test\",\"pt\"); pt.append!(t) ; pt1 = loadTable(\"dfs://tableUpsert_test\",\"pt1\"); pt1.append!(t) ;",null,4, 4, 0, false,"", false);
        }
        BasicTable res1 = (BasicTable) connection.run("select * from loadTable(\"dfs://tableUpsert_test\",\"pt\")");
        BasicTable res2 = (BasicTable) connection.run("select * from loadTable(\"dfs://tableUpsert_test\",\"pt1\")");
        assertEquals(30000, res1.rows());
        assertEquals(30000, res1.rows());
        connection.close();
    }

}