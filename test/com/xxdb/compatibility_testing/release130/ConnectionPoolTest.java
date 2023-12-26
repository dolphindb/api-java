package com.xxdb.compatibility_testing.release130;

import com.xxdb.*;
import com.xxdb.data.*;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
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
    static String[] ipports = bundle.getString("SITES").split(",");

    @Before
    public void setUp() throws IOException {
       conn = new DBConnection();
       conn.connect(HOST,PORT,"admin","123456",false);
//       conn.run("share_table = exec name from objs(true) where shared=1\n" +
//                "\tfor(t in share_table){\n" +
//                "\t\ttry{clearTablePersistence(objByName(t))}catch(ex){}\n" +
//                "\t\tundef(t, SHARED);\t\n" +
//                "\t}");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://demohash\")){\n" +"\tdropDatabase(\"dfs://demohash\")\n" +                "}");
        conn.close();
    }

    //ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability)
    //ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, String[] haSites, String initialScript,boolean compress, boolean useSSL, boolean usePython)
    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_host_error() throws IOException {
                DBConnectionPool pool1 = new ExclusiveDBConnectionPool("1",PORT,"admin","123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_host_null() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(null,PORT,"admin","123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_port_error() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,1,"admin","123456",5,false,false);
    }

    @Test(expected = IOException.class)
    public void test_DBConnectionPool_uid_error() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"error","123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_uid_null() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,null,"123456",5,false,false);
    }

    @Test(expected = IOException.class)
    public void test_DBConnectionPool_password_error() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","error",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_password_null() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin",null,5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_count_0() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",0,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_count_minus() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",-1,false,false);
    }

    @Test
    public void test_DBConnectionPool_count_nomal() throws IOException {
        conn.run("t = streamTable(10:0,`a`b,[INT,INT]);" +
                "share t as t1");
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",10,false,false);
        assertEquals(10,pool1.getConnectionCount());
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("insert into t1 values(1,1);");
            tasks.add(task);
        }
        pool1.execute(tasks);
        BasicInt a = (BasicInt)conn.run("exec count(*) from t1");
        assertEquals(100,a.getInt());
        pool1.shutdown();
    }

    @Test
    public void test_DBConnectionPool_count_1() throws IOException {
        conn.run("t = streamTable(10:0,`a`b,[INT,INT]);" +
                "share t as t1");
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",1,false,false);
        assertEquals(1,pool1.getConnectionCount());
        List<DBTask> tasks = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++){
            BasicDBTask task = new BasicDBTask("sleep(1000);");
            tasks.add(task);
        }
        pool1.execute(tasks);
        long completeTime1 = System.currentTimeMillis();
        long tcompleteTime = completeTime1 - startTime;
        assertEquals(true,tcompleteTime>10000);
        pool1.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, false, false);
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
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
        pool.shutdown();
    }
    @Test
    public void test_countLessThanZero() throws IOException{
        try {
            pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 0, true, true);
        }catch(RuntimeException re){
            assertEquals("The thread count can not be less than 0",re.getMessage());
        }
    }

    @Test
    public void test_loadBalanceFalse() throws Exception {
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
        pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, false, true);
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
        pool.shutdown();
    }

    @Test
    public void test_DBConnectionPool_haSite() throws Exception {
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, true, ipports,null,true,false,false);
        conn.run("db_path = \"dfs://test_DBConnectionPool_haSite\";\n" +
                "if(existsDatabase(db_path)){\n" +
                "        dropDatabase(db_path)\n" +
                "}\n" +
                "db = database(db_path, VALUE, 1..100);\n" +
                "t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id)");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("t = table(int(take("+i+",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);"+
                    "pt = loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1);"+
                    "pt.append!(t)");
            tasks.add(task);
        }
        tmp_pool.execute(tasks);
        tmp_pool.waitForThreadCompletion();
        BasicInt re = (BasicInt) conn.run("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1))");
        assertEquals(10000,re.getInt());
        BasicIntVector nodes_port_re = (BasicIntVector) conn.run("exec nodePort from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1) group by nodePort order by nodePort");
        BasicIntVector nodes_port_ex = (BasicIntVector) conn.run("exec value from pnodeRun(getNodePort) order by value");
        for(int i = 0;i<nodes_port_re.rows();i++){
            assertEquals(nodes_port_re.getInt(i),nodes_port_ex.getInt(i));
        }
        tmp_pool.shutdown();
    }

    @Test
    public void test_DBConnectionPool_haSite_ha_false() throws Exception {
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, false, ipports,null,true,false,false);
        conn.run("db_path = \"dfs://test_DBConnectionPool_haSite\";\n" +
                "if(existsDatabase(db_path)){\n" +
                "        dropDatabase(db_path)\n" +
                "}\n" +
                "db = database(db_path, VALUE, 1..100);\n" +
                "t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id)");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("t = table(int(take("+i+",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);"+
                    "pt = loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1);"+
                    "pt.append!(t)");
            tasks.add(task);
        }
        tmp_pool.execute(tasks);
        tmp_pool.waitForThreadCompletion();
        BasicInt re = (BasicInt) conn.run("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1))");
        assertEquals(10000,re.getInt());
        BasicIntVector nodes_port_re = (BasicIntVector) conn.run("exec nodePort from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1) group by nodePort order by nodePort");
        BasicIntVector nodes_port_ex = (BasicIntVector) conn.run("exec value from pnodeRun(getNodePort) order by value");
        for(int i = 0;i<nodes_port_re.rows();i++){
            assertEquals(nodes_port_re.getInt(i),nodes_port_ex.getInt(i));
        }
        tmp_pool.shutdown();
    }


    @Test
    public void test_DBConnectionPool_haSite_loadBalance_false() throws Exception {
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, false, false, ipports,null,true,false,false);
        conn.run("db_path = \"dfs://test_DBConnectionPool_haSite\";\n" +
                "if(existsDatabase(db_path)){\n" +
                "        dropDatabase(db_path)\n" +
                "}\n" +
                "db = database(db_path, VALUE, 1..100);\n" +
                "t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id)");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("t = table(int(take("+i+",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);"+
                    "pt = loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1);"+
                    "pt.append!(t)");
            tasks.add(task);
        }
        tmp_pool.execute(tasks);
        tmp_pool.waitForThreadCompletion();
        BasicInt re = (BasicInt) conn.run("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1))");
        assertEquals(10000,re.getInt());
        BasicIntVector nodes_port_re = (BasicIntVector) conn.run("exec nodePort from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1) group by nodePort order by nodePort");
        BasicInt nodes_port_ex = (BasicInt) conn.run("getNodePort()");
        for(int i = 0;i<nodes_port_re.rows();i++){
            assertEquals(nodes_port_ex.getInt(),nodes_port_re.getInt(i));
        }
        tmp_pool.shutdown();
    }

    @Test(expected = ArithmeticException.class)
    public void test_DBConnectionPool_haSite_zero() throws Exception {
        String[] tmp_null = {};
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, true,tmp_null,null,true,false,false);
    }

    @Test
    public void test_DBConnectionPool_haSite_null() throws Exception {
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, true, null,null,true,false,false);
        conn.run("db_path = \"dfs://test_DBConnectionPool_haSite\";\n" +
                "if(existsDatabase(db_path)){\n" +
                "        dropDatabase(db_path)\n" +
                "}\n" +
                "db = database(db_path, VALUE, 1..100);\n" +
                "t = table(10:0,`id`sym`price`nodePort,[INT,SYMBOL,DOUBLE,INT])\n" +
                "pt1 = db.createPartitionedTable(t,`pt1,`id)");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("t = table(int(take("+i+",100)) as id,rand(`a`b`c`d,100) as sym,int(rand(100,100)) as price,take(getNodePort(),100) as node);"+
                    "pt = loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1);"+
                    "pt.append!(t)");
            tasks.add(task);
        }
        tmp_pool.execute(tasks);
        tmp_pool.waitForThreadCompletion();
        BasicInt re = (BasicInt) conn.run("int(exec count(*) from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1))");
        assertEquals(10000,re.getInt());
        BasicIntVector nodes_port_re = (BasicIntVector) conn.run("exec nodePort from loadTable(\"dfs://test_DBConnectionPool_haSite\",`pt1) group by nodePort order by nodePort");
        BasicIntVector nodes_port_ex = (BasicIntVector) conn.run("exec value from pnodeRun(getNodePort) order by value");
        for(int i = 0;i<nodes_port_re.rows();i++){
            assertEquals(nodes_port_re.getInt(i),nodes_port_ex.getInt(i));
        }
        tmp_pool.shutdown();
    }

    @Test
    public void test_DBConnectionPool_execute_sync() throws Exception {
        DBConnectionPool tmp_pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 5,false,false);
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++){
            BasicDBTask task = new BasicDBTask("sleep(1000)");
            tasks.add(task);
        }
        long startTime = System.currentTimeMillis();
        tmp_pool.execute(tasks);
        long completeTime1 = System.currentTimeMillis();
        long tcompleteTime = completeTime1 - startTime;
        assertEquals(true,tcompleteTime>4000);
        for (int i = 0; i < 10; i++){
            assertEquals(true,tasks.get(i).isSuccessful());
        }
        tmp_pool.shutdown();
    }

    public class pool_test implements Runnable {
        private DBConnectionPool pool;
        private List<DBTask> tasks;
        public pool_test(DBConnectionPool pool,List<DBTask> tasks) {
            this.pool = pool;
            this.tasks = tasks;
        }
        @Override
        public void run() {
            pool.execute(tasks);
        }
    }
    @Test
    public void test_DBConnectionPool_task_async() throws Exception {
        DBConnectionPool pool_task_async = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 4,false,false);
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++){
            BasicDBTask task = new BasicDBTask("sleep(1000)");
            tasks.add(task);
        }
        Thread thread1 = new Thread(new pool_test(pool_task_async,tasks));
        thread1.start();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 20; i++){
            assertEquals(false,tasks.get(i).isSuccessful());
        }
        thread1.join();
        pool_task_async.waitForThreadCompletion();
        pool_task_async.shutdown();
        long completeTime1 = System.currentTimeMillis();
        long tcompleteTime = completeTime1 - startTime;
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        System.out.println(startTime);
        System.out.println(completeTime1);
        System.out.println(tcompleteTime);
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        assertEquals(true,tcompleteTime>5000);
        thread1.interrupt();
        pool_task_async.shutdown();
    }

    @Test
    public void test_pool_execute_timeout_10000() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, false, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        connectionPool.execute(new BasicDBTask("sleep(10000);"), 10000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_execute_timeout_5000() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, false, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        connectionPool.execute(new BasicDBTask("sleep(10000);"), 5000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000<6000);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    //@Test
    public void test_pool_execute_timeout_negative() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, false, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        connectionPool.execute(new BasicDBTask("sleep(10000);"), -1);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    //@Test
    public void test_pool_execute_timeout_listTask() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, false, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("sleep(10000);");
            tasks.add(task);
        }
        //connectionPool.execute(tasks,-1);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_BasicDBTask_timeout_greater_than_script_runTime() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, true, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        BasicDBTask bt = new BasicDBTask("t1 = now();\n do {if(t1 + 10 * 1000 < now())\n break} \n while(true);");
        connectionPool.execute(bt,20000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        assertEquals(true,(end - start) / 1000000<10100);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_BasicDBTask_timeout_less_than_script_runTime() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, true, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        BasicDBTask bt = new BasicDBTask("t111 = now();\n do {if(t111 + 10 * 1000 < now())\n break} \n while(true);");
        connectionPool.execute(bt,5000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>5000);
        assertEquals(true,(end - start) / 1000000<6100);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_BasicDBTask_timeout_less_than_script_runTime_0() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, true, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        BasicDBTask bt = new BasicDBTask("t1111 = now();\n do {if(t1111 + 10 * 1000 < now())\n break} \n while(true);");
        connectionPool.execute(bt,0);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        assertEquals(true,(end - start) / 1000000<10100);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_BasicDBTask_timeout_less_than_script_runTime_1() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, true, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        BasicDBTask bt = new BasicDBTask("t1 = now();\n do {if(t1 + 10 * 1000 < now())\n break} \n while(true);");
        connectionPool.execute(bt,1);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000<100);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
    @Test
    public void test_pool_BasicDBTask_timeout_equal_script_runTime() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, true, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        BasicDBTask bt = new BasicDBTask("t1111 = now();\n do {if(t1111 + 10 * 1000 < now())\n break} \n while(true);");
        connectionPool.execute(bt,10000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>10000);
        assertEquals(true,(end - start) / 1000000<10100);
        connectionPool.waitForThreadCompletion();
        connectionPool.shutdown();
    }
}

