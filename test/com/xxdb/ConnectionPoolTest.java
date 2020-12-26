package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.route.PartitionedTableAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConnectionPoolTest {
    private static String dburl="dfs://demohash";
    private static String tableName="pt";
    private static DBConnectionPool pool = null;
    private static PartitionedTableAppender appender;
    private static DBConnection conn;
    private static String HOST="localhost";
    private static int PORT = 8848;
    @Before
    public void setUp() throws IOException {
       conn = new DBConnection();
       conn.connect(HOST,PORT,"admin","123456");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://demohash\")){\n" +                "\tdropDatabase(\"dfs://demohash\")\n" +                "}");
        conn.close();
    }
    @Test
    public void testHashHashstring() throws Exception {
        String script = "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",HASH,[DATETIME,10])\n" +
                "db2=database(\"\",HASH,[STRING,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.of(2020,05,06,21,01,48,200));
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setString(i, "dss1");
            sym.setString(i+1, "dss2");
            sym.setString(i+2, "dss3");
            sym.setString(i+3, "dss4");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testHashHashInt() throws Exception {
        String script = "t = table(timestamp(1..10)  as date,int(1..10) as sym)\n" +
                "db1=database(\"\",HASH,[DATETIME,10])\n" +
                "db2=database(\"\",HASH,[INT,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.of(2020,05,06,21,01,48,200));
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i+1, 23);
            sym.setInt(i+2, 325);
            sym.setInt(i+3, 11);
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testValueHashSymbol() throws Exception {
        String script = "\n" +
                "t = table(date(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",HASH,[STRING,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateVector date = new BasicDateVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setDate(i,LocalDate.now());
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setString(i, "dss1");
            sym.setString(i+1, "dss2");
            sym.setString(i+2, "dss3");
            sym.setString(i+3, "dss4");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testValueHashDateTime() throws Exception {
        String script = "\n" +
                "t = table(datetime(1..10)  as date,string(1..10) as sym)\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "db1=database(\"\",HASH,[DATETIME,10])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setDateTime(i, LocalDateTime.of(1970, 01, 01, 00, 00, 00));
            date.setDateTime(i+1, LocalDateTime.of(1970, 01, 02, 00, 00, 00));
            date.setDateTime(i+2, LocalDateTime.of(1970, 01, 03, 00, 00, 00));
            date.setDateTime(i+3, LocalDateTime.of(1970, 02, 03, 00, 00, 00));
        }
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++) {
            sym.setString(i, "8");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getInt());
    }
    @Test
    public void testRangeHashdate() throws Exception {
        String script = "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db1=database(\"\",RANGE,symbol(string(1..9)))\n" +
                "db2=database(\"\",HASH,[DATE,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setDateTime(i, LocalDateTime.of(2020, 2, 3, 23, 2));
            date.setDateTime(i+1, LocalDateTime.of(2020, 5, 3, 23, 2));
            date.setDateTime(i+2, LocalDateTime.of(2020, 6, 3, 23, 2));
            date.setDateTime(i+3, LocalDateTime.of(2020, 12, 3, 23, 2));
        }
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++) {
            sym.setString(i, "1");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testRangeRangeInt() throws Exception {
        String script = "\n" +
                "t = table(nanotimestamp(1..10)  as date,1..10 as sym)\n" +
                "db1=database(\"\",RANGE,date(2020.01.01)+0..100*5)\n" +
                "db2=database(\"\",RANGE,0 2 4 6 8 10)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicNanoTimestampVector date = new BasicNanoTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setNanoTimestamp(i,LocalDateTime.of(2020,01,01,02,05,22,000000000));
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i+1, 3);
            sym.setInt(i+2, 5);
            sym.setInt(i+3, 7);
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("exec count(*) from loadTable(\"dfs://demohash\",`pt) where date=nanotimestamp('2020.01.01T02:05:22.000000000') and sym=2");
        assertEquals(10000000,re.getInt());
    }
    @Test
    public void testRangeRangedatetime() throws Exception {
        String script = "\n" +
                "t = table(nanotimestamp(1..10)  as date,1..10 as sym)\n" +
                "db2=database(\"\",RANGE,0 2 4 6 8 10)\n" +
                "db1=database(\"\",RANGE,datetime(2020.01.01 02:05:00)+0..100*5)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicNanoTimestampVector date = new BasicNanoTimestampVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setNanoTimestamp(i, LocalDateTime.of(2020, 01, 01, 02, 05, 00, 000000000));
            date.setNanoTimestamp(i+1, LocalDateTime.of(2020, 01, 01, 02, 05, 22, 000000000));
            date.setNanoTimestamp(i+2, LocalDateTime.of(2020, 01, 01, 02, 05, 06, 000000000));
            date.setNanoTimestamp(i+3, LocalDateTime.of(2020, 01, 01, 02, 05, 50, 000000000));
        }
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i++) {
            sym.setInt(i, 1);
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("exec count(*) from loadTable(\"dfs://demohash\",`pt) where date=nanotimestamp('2020.01.01T02:05:22.000000000') and sym=2");
        assertEquals(10000000,re.getInt());
    }

    @Test
    public void testHashRangeDate() throws Exception {
        String script = "\n" +
                "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db2=database(\"\",HASH,[SYMBOL,5])\n" +
                "db1=database(\"\",RANGE,date(2020.02.02)+0..100)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setDateTime(i, LocalDateTime.of(2020,02,02,01,01,02));
            date.setDateTime(i+1, LocalDateTime.of(2020,02,03,01,01,02));
            date.setDateTime(i+2, LocalDateTime.of(2020,02,04,01,01,02));
            date.setDateTime(i+3, LocalDateTime.of(2020,02,05,01,01,02));
        }
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testHashRangeDateTime() throws Exception {
        String script = "\n" +
                "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db2=database(\"\",HASH,[SYMBOL,5])\n" +
                "db1=database(\"\",RANGE,datetime(2020.02.02T01:01:01)+0..10000*2)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setDateTime(i, LocalDateTime.of(2020,02,02,01,01,02));
            date.setDateTime(i+1, LocalDateTime.of(2020,02,02,01,01,03));
            date.setDateTime(i+2, LocalDateTime.of(2020,02,02,01,01,04));
            date.setDateTime(i+3, LocalDateTime.of(2020,02,02,01,01,05));
        }
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testValueRangesymbol() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",RANGE,symbol(string(1..9)))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.now());
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setString(i, "2");
            sym.setString(i + 1, "3");
            sym.setString(i + 2, "4");
            sym.setString(i + 3, "5");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testHashValuesymbol() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",HASH,[DATETIME,10])\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.now());
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setString(i, "2");
            sym.setString(i + 1, "3");
            sym.setString(i + 2, "4");
            sym.setString(i + 3, "5");
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testValueValuedate() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "db1=database(\"\",VALUE,date(2020.02.02)+0..100)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            date.setTimestamp(i, LocalDateTime.of(2020, 02, 02, 00, 00));
            date.setTimestamp(i+1, LocalDateTime.of(2020, 02, 03, 00, 00));
            date.setTimestamp(i+2, LocalDateTime.of(2020, 02, 04, 00, 00));
            date.setTimestamp(i+3, LocalDateTime.of(2020, 02, 05, 00, 00));
        }
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "1");
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }
    @Test
    public void testRangeValueInt() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,int(1..10) as sym)\n" +
                "db1=database(\"\",RANGE,date(now())+0..100*5)\n" +
                "db2=database(\"\",VALUE,int(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.now());
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i + 1, 2);
            sym.setInt(i + 2, 3);
            sym.setInt(i + 3, 4);
        }
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }

}

