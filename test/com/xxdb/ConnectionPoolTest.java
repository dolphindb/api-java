package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.route.PartitionedTableAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class ConnectionPoolTest {
    private static String dburl="dfs://demohash";
    private static String tableName="pt";
    private static DBConnectionPool pool = null;
    private static PartitionedTableAppender appender;
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public void setUp() throws IOException {
       conn = new DBConnection();
       conn.connect(HOST,PORT,"admin","123456");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://demohash\")){\n" +"\tdropDatabase(\"dfs://demohash\")\n" +                "}");
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
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
    }
    @Test
    public void testRangeHashdate() throws Exception {
        String script = "t = table(datetime(1..10)  as date,symbol(string(1..10)) as sym)\n" +
                "db1=database(\"\",RANGE,symbol(string(1..9)))\n" +
                "db2=database(\"\",HASH,[DATE,15])\n" +
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
        for (int i =0 ;i<2500;i++) {
            date.setDateTime(i, LocalDateTime.of(2020,3,3,01,01,03));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+2500, LocalDateTime.of(2020,2,2,01,01,02));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+5000, LocalDateTime.of(2020,4,4,01,01,04));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+7500, LocalDateTime.of(2020,5,7,01,01,05));
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
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
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
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }

    @Test
    public void testValueRangeInt() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,1..10 as sym,1.02+1..10 as flt,string(1..10) as str)\n" +
                "db1=database(\"\",VALUE,date(2020.01.01)+0..10)\n" +
                "db2=database(\"\",RANGE,0 2 4 6 8 10)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(4);
        colNames.add("date");
        colNames.add("sym");
        colNames.add("flt");
        colNames.add("str");
        List<Vector> cols = new ArrayList<Vector>(4);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<10000;i++)
            date.setTimestamp(i,LocalDateTime.of(2020,01,01,02,05,22));
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i+1, 3);
            sym.setInt(i+2, 5);
            sym.setInt(i+3, 7);
        }
        BasicDoubleVector flt = new BasicDoubleVector(10000);
        for (int i =0 ;i<10000;i++) {
            flt.setDouble(i, 1.2);
        }
        cols.add(sym);
        cols.add(flt);
        BasicStringVector str = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++) {
         str.setString(i, "1.2");
        }
        cols.add(str);
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }

    @Test
    public void testRangeRangemonth() throws Exception {
        String script = "\n" +
                "t = table(nanotimestamp(1..10)  as date,1..10 as sym)\n" +
                "db2=database(\"\",RANGE,0 2 4 6 8 10)\n" +
                "db1=database(\"\",RANGE,month(2020.01M)+0..100*5)\n" +
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
            date.setNanoTimestamp(i+1, LocalDateTime.of(2020, 02, 01, 03, 05, 22, 000000000));
            date.setNanoTimestamp(i+2, LocalDateTime.of(2020, 03, 01, 04, 05, 06, 000000000));
            date.setNanoTimestamp(i+3, LocalDateTime.of(2020, 04, 01, 05, 05, 50, 000000000));
        }
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(10000);
        for (int i =0 ;i<10000;i++) {
            sym.setInt(i, 1);
        }
        cols.add(sym);
/*
        BasicTable table1 = new BasicTable(colNames,cols);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);*/
        for (int i =0 ;i<1000;i++) {
            //conn.run(String.format("tableInsert{loadTable('%s','pt')}",dburl), args);
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getInt());
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
    }

    @Test
    public void testHashRangeDate() throws Exception {
        String script = "\n" +
                "t = table(symbol(string(1..10)) as sym,datetime(1..10)  as date)\n" +
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
        colNames.add("sym");
        colNames.add("date");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicDateTimeVector date = new BasicDateTimeVector(10000);
        BasicStringVector sym = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++)
            sym.setString(i, "dss");
        cols.add(sym);
        for (int i =0 ;i<2500;i++) {
            date.setDateTime(i, LocalDateTime.of(2020,02,02,01,01,02));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+2500, LocalDateTime.of(2020,02,02,03,01,03));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+5000, LocalDateTime.of(2020,02,02,04,01,04));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+7500, LocalDateTime.of(2020,02,02,05,01,05));
        }
        cols.add(date);
        BasicTable table1 = new BasicTable(colNames,cols);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        for (int i =0 ;i<1000;i++) {
            //conn.run(String.format("tableInsert{loadTable('%s','pt')}",dburl), args);
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getInt());
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
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
        for (int i =0 ;i<2500;i++) {
            date.setDateTime(i, LocalDateTime.of(2020,02,02,01,01,02));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+2500, LocalDateTime.of(2020,02,02,01,01,03));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+5000, LocalDateTime.of(2020,02,02,01,01,04));
        } for (int i =0 ;i<2500;i++) {
            date.setDateTime(i+7500, LocalDateTime.of(2020,02,02,01,01,05));
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

        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
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
        for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i, LocalDateTime.of(2020, 02, 02, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+2500, LocalDateTime.of(2020, 02, 03, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+5000, LocalDateTime.of(2020, 02, 04, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+7500, LocalDateTime.of(2020, 02, 05, 00, 00));
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
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
    }
    @Test
    public void testValueValuemonth() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "db2=database(\"\",VALUE,string(1..10))\n" +
                "db1=database(\"\",VALUE,month(2020.02M)+0..100)\n" +
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
        for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i, LocalDateTime.of(2020, 02, 02, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+2500, LocalDateTime.of(2020, 03, 03, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+5000, LocalDateTime.of(2020, 4, 04, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+7500, LocalDateTime.of(2020, 5, 05, 00, 00));
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
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
    }
    @Test
    public void testRangeValueInt() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,int(1..10) as sym,string(1..10) as str)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",RANGE,int(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "sym", pool);
        List<String> colNames = new ArrayList<String>(3);
        colNames.add("date");
        colNames.add("sym");
        colNames.add("str");
     //   colNames.add("flt");
        List<Vector> cols = new ArrayList<Vector>(3);
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
        BasicStringVector str = new BasicStringVector(10000);
        for (int i =0 ;i<10000;i++) {
            str.setString(i,"32");
        }
        cols.add(str);
     /*   BasicDoubleVector flt = new BasicDoubleVector(10000);
        for (int i =0 ;i<10000;i+=4) {
           flt.setDouble(i,2.3);
        }
        cols.add(flt);*/

        for (int i =0 ;i<1000;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(10000000,re.getLong());
    }

    @Test
    public void testPoolSingle() throws Exception {
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,string(1..10) as sym)\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db=database(\"dfs://demohash\",VALUE,date(2020.02.02)+0..100)\n" +
                "pt = db.createPartitionedTable(t,`pt,`date)\n";
        conn.run(script);
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 1, true, true);
        appender = new PartitionedTableAppender(dburl, tableName, "date", pool);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("date");
        colNames.add("sym");
        List<Vector> cols = new ArrayList<Vector>(2);
        BasicTimestampVector date = new BasicTimestampVector(10000);
        for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i, LocalDateTime.of(2020, 02, 02, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+2500, LocalDateTime.of(2020, 02, 03, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+5000, LocalDateTime.of(2020, 02, 04, 00, 00));
        } for (int i =0 ;i<2500;i++) {
            date.setTimestamp(i+7500, LocalDateTime.of(2020, 02, 05, 00, 00));
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
        BasicTable table = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\",`pt)");
        assertEquals(date.getString(),table.getColumn("date").getString());
        assertEquals(sym.getString(),table.getColumn("sym").getString());
    }


}

