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
    public void testHashHash() throws Exception {
        String script = "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",HASH,[DATETIME,10])\n" +
                "db2=database(\"\",HASH,[STRING,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
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
    public void testValueHash() throws Exception {
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
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
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
    public void testRangeHash() throws Exception {
        String script = "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db1=database(\"\",RANGE,date(now())+0..100*5)\n" +
                "db2=database(\"\",HASH,[SYMBOL,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setDateTime(i,LocalDateTime.now());
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
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
    public void testRangeRange() throws Exception {
        String script = "\n" +
                "t = table(nanotimestamp(1..10)  as date,1..10 as sym)\n" +
                "db1=database(\"\",RANGE,date(2020.01.01)+0..100*5)\n" +
                "db2=database(\"\",RANGE,0 5 10)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setInt(i, 2);
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("exec count(*) from loadTable(\"dfs://demohash\",`pt) where date=nanotimestamp('2020.01.01T02:05:22.000000000') and sym=2");
        assertEquals(10000000,re.getInt());
    }

    @Test
    public void testHashRange() throws Exception {
        String script = "\n" +
                "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db1=database(\"\",RANGE,date(now())+0..100*5)\n" +
                "db2=database(\"\",HASH,[SYMBOL,5])\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setDateTime(i,LocalDateTime.now());
        cols.add(date);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
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
    public void testValueRange() throws Exception {
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
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "2");
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
    public void testHashValue() throws Exception {
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
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "1");
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
    public void testValueValue() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "1");
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
    public void testRangeValue() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db1=database(\"\",RANGE,date(now())+0..100*5)\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db2,db1])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 5, true, true);
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
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "1");
        cols.add(sym);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getInt());
    }

}

