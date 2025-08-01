package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.route.AutoFitTableUpsert;
import com.xxdb.route.PartitionedTableAppender;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static com.xxdb.Prepare.PrepareUser_authMode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
       conn.run("share_table = exec name from objs(true) where shared=1\n" +
                "\tfor(t in share_table){\n" +
                "\t\ttry{clearTablePersistence(objByName(t))}catch(ex){}\n" +
                "\t\tundef(t, SHARED);\t\n" +
                "\t}");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://demohash\")){\n" +"\tdropDatabase(\"dfs://demohash\")\n" +                "}");
        conn.close();
    }

    //ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability)
    //ExclusiveDBConnectionPool(String host, int port, String uid, String pwd, int count, boolean loadBalance, boolean enableHighAvailability, String[] haSites, String initialScript,boolean compress, boolean useSSL, boolean usePython)
   // @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_host_error() throws IOException {
                DBConnectionPool pool1 = new ExclusiveDBConnectionPool("1",PORT,"admin","123456",5,false,false);
    }

    //@Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_host_null() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(null,PORT,"admin","123456",5,false,false);
    }

    //@Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_port_error() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,1,"admin","123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_uid_error() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"error","123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
    public void test_DBConnectionPool_uid_null() throws IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,null,"123456",5,false,false);
    }

    @Test(expected = RuntimeException.class)
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
    public void test_DBConnectionPool_user_authMode_scream() throws Exception {
        PrepareUser_authMode("scramUser","123456","scram");
        conn.run("if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db=database(\"dfs://testArrayVector\",RANGE,int(1..10),,\"TSDB\")\n" +
                "t = table(1000000:0,`sym`tradeDate`volume`valueTrade,[INT,DATETIME,INT[],DOUBLE])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym,,`tradeDate)");
        conn.run("grant(\"scramUser\", DB_OWNER, \"dfs://test*\");\n grant(\"scramUser\", DB_MANAGE);\n grant(\"scramUser\", TABLE_READ,\"*\");\n grant(\"scramUser\", TABLE_WRITE,\"dfs://testArrayVector/pt\");");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"scramUser","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","sym",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("sym");
        colNames.add("tradesDate");
        colNames.add("volume");
        colNames.add("valueTrade");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector biv = new BasicIntVector(new int[]{1,2,3});
        cols.add(biv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{10,20,30});
        cols.add(bdtv);
        List<Vector> value = new ArrayList<>();
        value.add(new BasicIntVector(new int[]{1,2,3}));
        value.add(new BasicIntVector(new int[]{4,5,6,7,8}));
        value.add(new BasicIntVector(new int[]{9,10,11,13,17,21}));
        BasicArrayVector bav = new BasicArrayVector(value);
        cols.add(bav);
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{1.1,3.6,7.9});
        cols.add(bdv);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,res.getColumn(2).getDataType());
        System.out.println(res.getColumn(2).getString());
        pool.shutdown();
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
                "db1=database(\"\",RANGE,datetime(2020.02.02T01:01:01 2022.02.02T01:01:01 2024.02.02T01:01:01))\n" +
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
        for (int i =0 ;i<10;i++) {
            int m = appender.append(new BasicTable(colNames, cols));
            assertEquals(10000,m);
        }
        BasicInt re = (BasicInt) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(100000,re.getInt());

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
    public void test_PartitionedTableAppender_ArrayVector_Int() throws Exception {
        conn.run("if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db=database(\"dfs://testArrayVector\",RANGE,int(1..10),,\"TSDB\")\n" +
                "t = table(1000000:0,`sym`tradeDate`volume`valueTrade,[INT,DATETIME,INT[],DOUBLE])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym,,`tradeDate)");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","sym",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("sym");
        colNames.add("tradesDate");
        colNames.add("volume");
        colNames.add("valueTrade");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector biv = new BasicIntVector(new int[]{1,2,3});
        cols.add(biv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{10,20,30});
        cols.add(bdtv);
        List<Vector> value = new ArrayList<>();
        value.add(new BasicIntVector(new int[]{1,2,3}));
        value.add(new BasicIntVector(new int[]{4,5,6,7,8}));
        value.add(new BasicIntVector(new int[]{9,10,11,13,17,21}));
        BasicArrayVector bav = new BasicArrayVector(value);
        cols.add(bav);
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{1.1,3.6,7.9});
        cols.add(bdv);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,res.getColumn(2).getDataType());
        System.out.println(res.getColumn(2).getString());
        pool.shutdown();
    }

    @Test
    public void test_PartitionedTableAppender_ArrayVector_Double() throws Exception {
        conn.run("if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db=database(\"dfs://testArrayVector\",RANGE,int(1..10),,\"TSDB\")\n" +
                "t = table(1000000:0,`sym`tradeDate`volume`valueTrade,[INT,DATETIME,DOUBLE[],DOUBLE])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym,,`tradeDate)");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","sym",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("sym");
        colNames.add("tradesDate");
        colNames.add("volume");
        colNames.add("valueTrade");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector biv = new BasicIntVector(new int[]{1,2,3});
        cols.add(biv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{10,20,30});
        cols.add(bdtv);
        List<Vector> value = new ArrayList<>();
        value.add(new BasicDoubleVector(new double[]{1.8,2.7,3.9}));
        value.add(new BasicDoubleVector(new double[]{4.8}));
        value.add(new BasicDoubleVector(new double[]{9.05,0.32,4.92,7.32}));
        BasicArrayVector bav = new BasicArrayVector(value);
        cols.add(bav);
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{1.1,3.6,7.9});
        cols.add(bdv);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,res.getColumn(2).getDataType());
        System.out.println(res.getColumn(2).getString());
        pool.shutdown();
    }

    @Test
    public void test_PartitionedTableAppender_ArrayVector_AllDataType() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`char`complex`datehour`datetime`date`double`float`int128`int`ipaddr`long`minute`month`nanotimestamp`nanotime`point`second`short`timestamp`time`uuid`declmal64`decimal32`decimal128" +
                ",[INT,CHAR[],COMPLEX[],DATEHOUR[],DATETIME[],DATE[],DOUBLE[],FLOAT[],INT128[],INT[],IPADDR[],LONG[],MINUTE[],MONTH[],NANOTIMESTAMP[],NANOTIME[],POINT[],SECOND[],SHORT[],TIMESTAMP[],TIME[],UUID[],DECIMAL64(4)[],DECIMAL32(3)[],DECIMAL128(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("char");
        colNames.add("complex");
        colNames.add("datehour");
        colNames.add("datetime");
        colNames.add("date");
        colNames.add("double");
        colNames.add("float");
        colNames.add("int128");
        colNames.add("int");
        colNames.add("ipaddr");
        colNames.add("long");
        colNames.add("minute");
        colNames.add("month");
        colNames.add("nanotimestamp");
        colNames.add("nanotime");
        colNames.add("point");
        colNames.add("second");
        colNames.add("short");
        colNames.add("timestamp");
        colNames.add("time");
        colNames.add("uuid");
        colNames.add("decimal64");
        colNames.add("decimal32");
        colNames.add("decimal128");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bbl = new ArrayList<>();
        bbl.add(new BasicByteVector(new byte[]{'a','b','c'}));
        bbl.add(new BasicByteVector(new byte[]{'d'}));
        bbl.add(new BasicByteVector(new byte[]{'x','y','z','g'}));
        BasicArrayVector bbla = new BasicArrayVector(bbl);
        cols.add(bbla);
        List<Vector> bcvl = new ArrayList<>();
        bcvl.add(new BasicComplexVector(new Double2[]{new Double2(1.1,3.9)}));
        bcvl.add(new BasicComplexVector(new Double2[]{new Double2(0.6,8.3),new Double2(2.05,7.91)}));
        bcvl.add(new BasicComplexVector(new Double2[]{new Double2(5.213,7.398),new Double2(10.02,20.01),new Double2(0.71,17.0)}));
        BasicArrayVector bcvla = new BasicArrayVector(bcvl);
        cols.add(bcvla);
        List<Vector> bdhv = new ArrayList<>();
        bdhv.add(new BasicDateHourVector(new int[]{10,50}));
        bdhv.add(new BasicDateHourVector(new int[]{10422,19422}));
        bdhv.add(new BasicDateHourVector(new int[]{4090}));
        BasicArrayVector bdhva = new BasicArrayVector(bdhv);
        cols.add(bdhva);
        List<Vector> bdtvl = new ArrayList<>();
        bdtvl.add(new BasicDateTimeVector(new int[]{7}));
        bdtvl.add(new BasicDateTimeVector(new int[]{18,890,9000}));
        bdtvl.add(new BasicDateTimeVector(new int[]{55,73}));
        BasicArrayVector bdtvla = new BasicArrayVector(bdtvl);
        cols.add(bdtvla);
        List<Vector> bdvl = new ArrayList<>();
        bdvl.add(new BasicDateVector(new int[]{9000,659}));
        bdvl.add(new BasicDateVector(new int[]{888,865,855}));
        bdvl.add(new BasicDateVector(new int[]{1200,1300,8000,8100,9000}));
        BasicArrayVector bdvla = new BasicArrayVector(bdvl);
        cols.add(bdvla);
        List<Vector> bdbl = new ArrayList<>();
        bdbl.add(new BasicDoubleVector(new double[]{17.01}));
        bdbl.add(new BasicDoubleVector(new double[]{35.72,88.41,91.06}));
        bdbl.add(new BasicDoubleVector(new double[]{40.2,0.98}));
        BasicArrayVector bdbla = new BasicArrayVector(bdbl);
        cols.add(bdbla);
        List<Vector> bfvl = new ArrayList<>();
        bfvl.add(new BasicFloatVector(new float[]{(float) 15.6, (float) 9.92, (float) 4.71}));
        bfvl.add(new BasicFloatVector(new float[]{(float)6.23, (float) 7.71, (float) 1.24}));
        bfvl.add(new BasicFloatVector(new float[]{(float)0.83,(float)5.12,(float)10.1}));
        BasicArrayVector bfvla = new BasicArrayVector(bfvl);
        cols.add(bfvla);
        List<Vector> biv128l = new ArrayList<>();
        biv128l.add(new BasicInt128Vector(new Long2[]{new Long2(10000,1)}));
        biv128l.add(new BasicInt128Vector(new Long2[]{new Long2(323,11),new Long2(571,232)}));
        biv128l.add(new BasicInt128Vector(new Long2[]{new Long2(536,521)}));
        BasicArrayVector biv128la = new BasicArrayVector(biv128l);
        cols.add(biv128la);
        List<Vector> bivl = new ArrayList<>();
        bivl.add(new BasicIntVector(new int[]{1,2,3}));
        bivl.add(new BasicIntVector(new int[]{4,5,6,7}));
        bivl.add(new BasicIntVector(new int[]{8,9,10,11,12}));
        BasicArrayVector bivla = new BasicArrayVector(bivl);
        cols.add(bivla);
        List<Vector> biavl = new ArrayList<>();
        biavl.add(new BasicIPAddrVector(new Long2[]{new Long2(110,104)}));
        biavl.add(new BasicIPAddrVector(new Long2[]{new Long2(1450,251),new Long2(2022,2019)}));
        biavl.add(new BasicIPAddrVector(new Long2[]{new Long2(2022,1949),new Long2(2022,1984),new Long2(1949,1840)}));
        BasicArrayVector bivala = new BasicArrayVector(biavl);
        cols.add(bivala);
        List<Vector> blvl = new ArrayList<>();
        blvl.add(new BasicLongVector(new long[]{717L,851L}));
        blvl.add(new BasicLongVector(new long[]{13L}));
        blvl.add(new BasicLongVector(new long[]{2345L,361L,8080L}));
        BasicArrayVector blvla = new BasicArrayVector(blvl);
        cols.add(blvla);
        List<Vector> bmvl = new ArrayList<>();
        bmvl.add(new BasicMinuteVector(new int[]{60,120,180}));
        bmvl.add(new BasicMinuteVector(new int[]{240,300,360,420}));
        bmvl.add(new BasicMinuteVector(new int[]{480,540}));
        BasicArrayVector bmvla = new BasicArrayVector(bmvl);
        cols.add(bmvla);
        List<Vector> bmol = new ArrayList<>();
        bmol.add(new BasicMonthVector(new int[]{30,52}));
        bmol.add(new BasicMonthVector(new int[]{91,122,43}));
        bmol.add(new BasicMonthVector(new int[]{84,352,271}));
        BasicArrayVector bmola = new BasicArrayVector(bmol);
        cols.add(bmola);
        List<Vector> bntsvl = new ArrayList<>();
        bntsvl.add(new BasicNanoTimestampVector(new long[]{1234L,2345L}));
        bntsvl.add(new BasicNanoTimestampVector(new long[]{3456L,4567L,5678L}));
        bntsvl.add(new BasicNanoTimestampVector(new long[]{6789L,23456L,34567L}));
        BasicArrayVector bntsvla = new BasicArrayVector(bntsvl);
        cols.add(bntsvla);
        List<Vector> bntvl = new ArrayList<>();
        bntvl.add(new BasicNanoTimeVector(new long[]{1919L,1979L}));
        bntvl.add(new BasicNanoTimeVector(new long[]{1908L,1912L,1937L}));
        bntvl.add(new BasicNanoTimeVector(new long[]{1952L,1956L,1961L}));
        BasicArrayVector bntvla = new BasicArrayVector(bntvl);
        cols.add(bntvla);
        List<Vector> bpvl = new ArrayList<>();
        bpvl.add(new BasicPointVector(new Double2[]{new Double2(0.5,5.0)}));
        bpvl.add(new BasicPointVector(new Double2[]{new Double2(1.7,7.1),new Double2(2.3,3.2)}));
        bpvl.add(new BasicPointVector(new Double2[]{new Double2(5.9,9.2),new Double2(1.8,8.1)}));
        BasicArrayVector bpvla = new BasicArrayVector(bpvl);
        cols.add(bpvla);
        List<Vector> bsvl = new ArrayList<>();
        bsvl.add(new BasicSecondVector(new int[]{1,7}));
        bsvl.add(new BasicSecondVector(new int[]{26,99,83}));
        bsvl.add(new BasicSecondVector(new int[]{24,61,59,74}));
        BasicArrayVector bsvla = new BasicArrayVector(bsvl);
        cols.add(bsvla);
        List<Vector> bshv = new ArrayList<>();
        bshv.add(new BasicShortVector(new short[]{2,11,32}));
        bshv.add(new BasicShortVector(new short[]{7,16,27}));
        bshv.add(new BasicShortVector(new short[]{10,19,22}));
        BasicArrayVector bshva = new BasicArrayVector(bshv);
        cols.add(bshva);
        List<Vector> btsvl = new ArrayList<>();
        btsvl.add(new BasicTimestampVector(new long[]{16L}));
        btsvl.add(new BasicTimestampVector(new long[]{32L,57L}));
        btsvl.add(new BasicTimestampVector(new long[]{26L,73L,54L}));
        BasicArrayVector btsvla = new BasicArrayVector(btsvl);
        cols.add(btsvla);
        List<Vector> btvl = new ArrayList<>();
        btvl.add(new BasicTimeVector(new int[]{27,99,175483}));
        btvl.add(new BasicTimeVector(new int[]{98,7896,2557}));
        btvl.add(new BasicTimeVector(new int[]{178,9897,3728}));
        BasicArrayVector btvla = new BasicArrayVector(btvl);
        cols.add(btvla);
        List<Vector> buvl = new ArrayList<>();
        buvl.add(new BasicUuidVector(new Long2[]{new Long2(3,2)}));
        buvl.add(new BasicUuidVector(new Long2[]{new Long2(123,121)}));
        buvl.add(new BasicUuidVector(new Long2[]{new Long2(17,11),new Long2(25,6)}));
        BasicArrayVector buvla = new BasicArrayVector(buvl);
        cols.add(buvla);
        List<Vector> bdv64 = new ArrayList<Vector>();
        Vector v64=new BasicDecimal64Vector(3,4);
        v64.set(0,new BasicDecimal64(15645.00,2));
        v64.set(1,new BasicDecimal64(24635.00001,4));
        v64.set(2,new BasicDecimal64(24635.00001,4));
        bdv64.add(0,v64);
        bdv64.add(1,v64);
        bdv64.add(2,v64);
        BasicArrayVector bdv64a = new BasicArrayVector(bdv64);
        cols.add(bdv64a);
        List<Vector> bdv32 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal32Vector(3,4);
        v32.set(0,new BasicDecimal32(15645.00,4));
        v32.set(1,new BasicDecimal32(24635.00001,4));
        v32.set(2,new BasicDecimal32(24635.00001,4));
        bdv32.add(0,v32);
        bdv32.add(1,v32);
        bdv32.add(2,v32);
        BasicArrayVector bdv32a = new BasicArrayVector(bdv32);
        cols.add(bdv32a);
        List<Vector> bdv128 = new ArrayList<Vector>();
        Vector v128=new BasicDecimal32Vector(3,8);
        v128.set(0,new BasicDecimal32(0.99999999,8));
        v128.set(1,new BasicDecimal32(0.00000001,8));
        v128.set(2,new BasicDecimal32(0.12345678,8));
        bdv128.add(0,v128);
        bdv128.add(1,v128);
        bdv128.add(2,v128);
        BasicArrayVector bdv1281 = new BasicArrayVector(bdv128);
        cols.add(bdv1281);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_COMPLEX_ARRAY,res.getColumn(2).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_INT,res.getColumn(0).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_BYTE_ARRAY,res.getColumn(1).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,res.getColumn(3).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DATETIME_ARRAY,res.getColumn(4).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DATE_ARRAY,res.getColumn(5).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,res.getColumn(6).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_FLOAT_ARRAY,res.getColumn(7).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_INT128_ARRAY,res.getColumn(8).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,res.getColumn(9).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_IPADDR_ARRAY,res.getColumn(10).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_LONG_ARRAY,res.getColumn(11).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_MINUTE_ARRAY,res.getColumn(12).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_MONTH_ARRAY,res.getColumn(13).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIMESTAMP_ARRAY,res.getColumn(14).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_NANOTIME_ARRAY,res.getColumn(15).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_POINT_ARRAY,res.getColumn(16).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_SECOND_ARRAY,res.getColumn(17).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_SHORT_ARRAY,res.getColumn(18).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY,res.getColumn(19).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_TIME_ARRAY,res.getColumn(20).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_UUID_ARRAY,res.getColumn(21).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64_ARRAY,res.getColumn(22).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32_ARRAY,res.getColumn(23).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128_ARRAY,res.getColumn(24).getDataType());
        pool.shutdown();
    }

    @Test
    public void test_PartitionedTableAppender_ArrayVector_decimal() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false,null,null,false,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal32Vector(3,0);
        v32.set(0,new BasicDecimal32(15645.00,0));
        v32.set(1,new BasicDecimal32(24635.00001,0));
        v32.set(2,new BasicDecimal32(24635.00001,0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal32Vector(3,4);
        v321.set(0,new BasicDecimal32(15645.00,2));
        v321.set(1,new BasicDecimal32(24635.00001,3));
        v321.set(2,new BasicDecimal32(24635.00001,4));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal64Vector(3,0);
        v640.set(0,new BasicDecimal64(15645.00,0));
        v640.set(1,new BasicDecimal64(24635.00001,0));
        v640.set(2,new BasicDecimal64(24635.00001,0));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v641=new BasicDecimal64Vector(3,4);
        v641.set(0,new BasicDecimal64(15645.00,2));
        v641.set(1,new BasicDecimal64(24635.00001,8));
        v641.set(2,new BasicDecimal64(24635.00001,0));
        bdvcol3.add(0,v641);
        bdvcol3.add(1,v641);
        bdvcol3.add(2,v641);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v642=new BasicDecimal64Vector(3,8);
        v642.set(0,new BasicDecimal64(15645.00,1));
        v642.set(1,new BasicDecimal64(24635.00001,8));
        v642.set(2,new BasicDecimal64(24635.00001,8));
        bdvcol4.add(0,v642);
        bdvcol4.add(1,v642);
        bdvcol4.add(2,v642);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());

        pool.shutdown();
    }
    @Test
    public void test_PartitionedTableAppender_ArrayVector_decimal_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false,null,null,true,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal32Vector(3,0);
        v32.set(0,new BasicDecimal32(15645.00,0));
        v32.set(1,new BasicDecimal32(24635.00001,0));
        v32.set(2,new BasicDecimal32(24635.00001,0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal32Vector(3,4);
        v321.set(0,new BasicDecimal32(15645.00,4));
        v321.set(1,new BasicDecimal32(24635.00001,4));
        v321.set(2,new BasicDecimal32(24635.00001,4));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal64Vector(3,0);
        v640.set(0,new BasicDecimal64(15645.00,0));
        v640.set(1,new BasicDecimal64(24635.00001,0));
        v640.set(2,new BasicDecimal64(24635.00001,0));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v641=new BasicDecimal64Vector(3,4);
        v641.set(0,new BasicDecimal64(15645.00,4));
        v641.set(1,new BasicDecimal64(24635.00001,4));
        v641.set(2,new BasicDecimal64(24635.00001,4));
        bdvcol3.add(0,v641);
        bdvcol3.add(1,v641);
        bdvcol3.add(2,v641);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v642=new BasicDecimal64Vector(3,8);
        v642.set(0,new BasicDecimal64(15645.00,8));
        v642.set(1,new BasicDecimal64(24635.00001,8));
        v642.set(2,new BasicDecimal64(24635.00001,8));
        bdvcol4.add(0,v642);
        bdvcol4.add(1,v642);
        bdvcol4.add(2,v642);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());

        pool.shutdown();
    }

    @Test
    public void test_PartitionedTableAppender_ArrayVector_decimal128() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v128=new BasicDecimal128Vector(3,0);
        v128.set(0,new BasicDecimal128("15645.00",0));
        v128.set(1,new BasicDecimal128("24635.00001",0));
        v128.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v128);
        bdvcol0.add(1,v128);
        bdvcol0.add(2,v128);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v1281=new BasicDecimal128Vector(3,4);
        v1281.set(0,new BasicDecimal128("15645.00",2));
        v1281.set(1,new BasicDecimal128("24635.00001",3));
        v1281.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol1.add(0,v1281);
        bdvcol1.add(1,v1281);
        bdvcol1.add(2,v1281);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v1282=new BasicDecimal128Vector(3,10);
        v1282.set(0,new BasicDecimal128("15645.00",0));
        v1282.set(1,new BasicDecimal128("24635.00001",0));
        v1282.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol2.add(0,v1282);
        bdvcol2.add(1,v1282);
        bdvcol2.add(2,v1282);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v1283=new BasicDecimal128Vector(3,19);
        v1283.set(0,new BasicDecimal128("15645.00",2));
        v1283.set(1,new BasicDecimal128("24635.00001",8));
        v1283.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol3.add(0,v1283);
        bdvcol3.add(1,v1283);
        bdvcol3.add(2,v1283);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v1284=new BasicDecimal128Vector(3,37);
        v1284.set(0,new BasicDecimal128("1.00",37));
        v1284.set(1,new BasicDecimal128("2.00001",37));
        v1284.set(2,new BasicDecimal128("2.00001",37));
        bdvcol4.add(0,v1284);
        bdvcol4.add(1,v1284);
        bdvcol4.add(2,v1284);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        System.out.println(bt.rows());
        System.out.println(bt.getString());
        int x = appender.append(bt);
        conn.run("sleep(500)");
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v1282.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v1283.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v1284.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
        pool.shutdown();
    }
    @Test
    public void test_PartitionedTableAppender_ArrayVector_decimal128_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false,null,null,true,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v128=new BasicDecimal128Vector(3,0);
        v128.set(0,new BasicDecimal128("15645.00",0));
        v128.set(1,new BasicDecimal128("24635.00001",0));
        v128.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v128);
        bdvcol0.add(1,v128);
        bdvcol0.add(2,v128);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v1281=new BasicDecimal128Vector(3,4);
        v1281.set(0,new BasicDecimal128("15645.00",4));
        v1281.set(1,new BasicDecimal128("24635.00001",4));
        v1281.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol1.add(0,v1281);
        bdvcol1.add(1,v1281);
        bdvcol1.add(2,v1281);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v1282=new BasicDecimal128Vector(3,10);
        v1282.set(0,new BasicDecimal128("15645.00",10));
        v1282.set(1,new BasicDecimal128("24635.00001",10));
        v1282.set(2,new BasicDecimal128("24635.00001",10));
        bdvcol2.add(0,v1282);
        bdvcol2.add(1,v1282);
        bdvcol2.add(2,v1282);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v1283=new BasicDecimal128Vector(3,19);
        v1283.set(0,new BasicDecimal128("15645.00",19));
        v1283.set(1,new BasicDecimal128("24635.00001",19));
        v1283.set(2,new BasicDecimal128("24635.00001",19));
        bdvcol3.add(0,v1283);
        bdvcol3.add(1,v1283);
        bdvcol3.add(2,v1283);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v1284=new BasicDecimal128Vector(3,37);
        v1284.set(0,new BasicDecimal128("1.00",37));
        v1284.set(1,new BasicDecimal128("0.00001",37));
        v1284.set(2,new BasicDecimal128("0.0000000000001",37));
        bdvcol4.add(0,v1284);
        bdvcol4.add(1,v1284);
        bdvcol4.add(2,v1284);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v1282.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v1283.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v1284.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());

        pool.shutdown();
    }

    @Test(timeout = 300000)
    public void test_PartitionedTableAppender_ArrayVector_BigData() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(0..100),,\"TSDB\")\n" +
                "t = table(1048576:0,`cint`char`complex`datehour`datetime`date`double`float`int128`int`ipaddr`long`minute`month`nanotimestamp`nanotime`point`second`short`timestamp`time`uuid," +
                "[INT,CHAR[],COMPLEX[],DATEHOUR[],DATETIME[],DATE[],DOUBLE[],FLOAT[],INT128[],INT[],IPADDR[],LONG[],MINUTE[],MONTH[],NANOTIMESTAMP[],NANOTIME[],POINT[],SECOND[],SHORT[],TIMESTAMP[],TIME[],UUID[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","cint",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("char");
        colNames.add("complex");
        colNames.add("datehour");
        colNames.add("datetime");
        colNames.add("date");
        colNames.add("double");
        colNames.add("float");
        colNames.add("int128");
        colNames.add("int");
        colNames.add("ipaddr");
        colNames.add("long");
        colNames.add("minute");
        colNames.add("month");
        colNames.add("nanotimestamp");
        colNames.add("nanotime");
        colNames.add("point");
        colNames.add("second");
        colNames.add("short");
        colNames.add("timestamp");
        colNames.add("time");
        colNames.add("uuid");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector biv = new BasicIntVector(1048576);
        List<Vector> bcvl = new ArrayList<>();
        List<Vector> bcpvl = new ArrayList<>();
        List<Vector> bdhvl = new ArrayList<>();
        List<Vector> bdtvl = new ArrayList<>();
        List<Vector> bdvl = new ArrayList<>();
        List<Vector> bdbvl = new ArrayList<>();
        List<Vector> bfvl = new ArrayList<>();
        List<Vector> bi128vl = new ArrayList<>();
        List<Vector> bivl = new ArrayList<>();
        List<Vector> bipvl = new ArrayList<>();
        List<Vector> blvl = new ArrayList<>();
        List<Vector> bmivl = new ArrayList<>();
        List<Vector> bmnvl = new ArrayList<>();
        List<Vector> bntsvl = new ArrayList<>();
        List<Vector> bntvl = new ArrayList<>();
        List<Vector> bpvl = new ArrayList<>();
        List<Vector> bsevl = new ArrayList<>();
        List<Vector> bshvl = new ArrayList<>();
        List<Vector> btsvl = new ArrayList<>();
        List<Vector> btvl = new ArrayList<>();
        List<Vector> buvl = new ArrayList<>();
        for(int i=0;i<1048576;i++){
            biv.setInt(i,i%100);
            bcvl.add(new BasicByteVector(new byte[]{'d','o','l','p','h','i','n','d','b'}));
            bcpvl.add(new BasicComplexVector(new Double2[]{new Double2(i+0.1,i+0.2),new Double2(i+2.0,i+3.0)}));
            bdhvl.add(new BasicDateHourVector(new int[]{i+3,i+5}));
            bdtvl.add(new BasicDateTimeVector(new int[]{i+4,i+7}));
            bdvl.add(new BasicDateVector(new int[]{i+5,i+6}));
            bdbvl.add(new BasicDoubleVector(new double[]{i+7.0,i+7.5}));
            bfvl.add(new BasicFloatVector(new float[]{(float) (i+6.3), (float) (i+3.9)}));
            bi128vl.add(new BasicInt128Vector(new Long2[]{new Long2(i+8,i),new Long2(i+18,i+2)}));
            bivl.add(new BasicIntVector(new int[]{i+11,i+8}));
            bipvl.add(new BasicIPAddrVector(new Long2[]{new Long2(i+17,i+2),new Long2(i+15,i+4)}));
            blvl.add(new BasicLongVector(new long[]{i+15L,i+2L}));
            bmivl.add(new BasicMinuteVector(new int[]{i+13,i+10}));
            bmnvl.add(new BasicMonthVector(new int[]{i+12,i+28}));
            bntsvl.add(new BasicNanoTimestampVector(new long[]{i+10000L,i+30000L}));
            bntvl.add(new BasicNanoTimeVector(new long[]{i+1000L,i+2000L}));
            bpvl.add(new BasicPointVector(new Double2[]{new Double2(i+0.17,i+0.86)}));
            bsevl.add(new BasicSecondVector(new int[]{i+30,i+50}));
            bshvl.add(new BasicShortVector(new short[]{(short) (i+9), (short) (i+3)}));
            btsvl.add(new BasicTimestampVector(new long[]{i+6000L,i+12000L}));
            btvl.add(new BasicTimeVector(new int[]{i+20,i+10}));
            buvl.add(new BasicUuidVector(new Long2[]{new Long2(i+1000L,i+19L)}));
        }
        cols.add(biv);
        BasicArrayVector bcvla = new BasicArrayVector(bcvl);
        cols.add(bcvla);
        BasicArrayVector bcpvla = new BasicArrayVector(bcpvl);
        cols.add(bcpvla);
        BasicArrayVector bdhvla = new BasicArrayVector(bdhvl);
        cols.add(bdhvla);
        BasicArrayVector bdtvla = new BasicArrayVector(bdtvl);
        cols.add(bdtvla);
        BasicArrayVector bdvla = new BasicArrayVector(bdvl);
        cols.add(bdvla);
        BasicArrayVector bdbvla = new BasicArrayVector(bdbvl);
        cols.add(bdbvla);
        BasicArrayVector bfvla = new BasicArrayVector(bfvl);
        cols.add(bfvla);
        BasicArrayVector bi128vla = new BasicArrayVector(bi128vl);
        cols.add(bi128vla);
        BasicArrayVector bivla = new BasicArrayVector(bivl);
        cols.add(bivla);
        BasicArrayVector bipvla = new BasicArrayVector(bipvl);
        cols.add(bipvla);
        BasicArrayVector blvla = new BasicArrayVector(blvl);
        cols.add(blvla);
        BasicArrayVector bmivla = new BasicArrayVector(bmivl);
        cols.add(bmivla);
        BasicArrayVector bmnvla = new BasicArrayVector(bmnvl);
        cols.add(bmnvla);
        BasicArrayVector bntsvla = new BasicArrayVector(bntsvl);
        cols.add(bntsvla);
        BasicArrayVector bntvla = new BasicArrayVector(bntvl);
        cols.add(bntvla);
        BasicArrayVector bpvla = new BasicArrayVector(bpvl);
        cols.add(bpvla);
        BasicArrayVector bsevla = new BasicArrayVector(bsevl);
        cols.add(bsevla);
        BasicArrayVector bshvla = new BasicArrayVector(bshvl);
        cols.add(bshvla);
        BasicArrayVector btsvla = new BasicArrayVector(btsvl);
        cols.add(btsvla);
        BasicArrayVector btvla = new BasicArrayVector(btvl);
        cols.add(btvla);
        BasicArrayVector buvla = new BasicArrayVector(buvl);
        cols.add(buvla);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(1048576,res.rows());
        pool.shutdown();
    }

    @Test
    public void test_pool_execute_decimal_arrayvector() throws Exception {
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);

        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        String script=("\n" +
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
        List<DBTask> tasks = new ArrayList<>();
        BasicDBTask task = new BasicDBTask(script);
        tasks.add(task);
        pool.execute(tasks);
        pool.waitForThreadCompletion();
        BasicTable res = (BasicTable) connection.run("ptt");
        System.out.println(res.getString());
        assertEquals(7,res.columns());
        assertEquals(2,res.rows());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,100000.0],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000,3.000,100000.000],[-1.000,0.000,0.123]]",res.getColumn(2).getString());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(3).getString());
        assertEquals("[[1.0,3.0,100000.0],[-1.0,0.0,0.1]]",res.getColumn(4).getString());
        assertEquals("[[1.0000,3.0000,100000.0000],[-1.0000,0.0000,0.1235]]",res.getColumn(5).getString());
        assertEquals("[[1.00000000,3.00001000,100000.00000000],[-1.00000000,0.00000000,0.12345679]]",res.getColumn(6).getString());
        System.out.println(res.getColumn(0).getString());
        pool.shutdown();
    }

    @Test
    public void test_pool_execute_decimal_arrayvector_compress_true() throws Exception {
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false,null,"",true,false,false);

        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        String script=("\n" +
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
        List<DBTask> tasks = new ArrayList<>();
        BasicDBTask task = new BasicDBTask(script);
        tasks.add(task);
        pool.execute(tasks);
        pool.waitForThreadCompletion();
        BasicTable res = (BasicTable) connection.run("ptt");
        System.out.println(res.getString());
        assertEquals(7,res.columns());
        assertEquals(2,res.rows());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0,3.0,100000.0],[-1.0,0.0,0.1]]",res.getColumn(1).getString());
        assertEquals("[[1.000,3.000,100000.000],[-1.000,0.000,0.123]]",res.getColumn(2).getString());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(3).getString());
        assertEquals("[[1.0,3.0,100000.0],[-1.0,0.0,0.1]]",res.getColumn(4).getString());
        assertEquals("[[1.0000,3.0000,100000.0000],[-1.0000,0.0000,0.1235]]",res.getColumn(5).getString());
        assertEquals("[[1.00000000,3.00001000,100000.00000000],[-1.00000000,0.00000000,0.12345679]]",res.getColumn(6).getString());
        System.out.println(res.getColumn(0).getString());
        pool.shutdown();
    }
    @Test
    public void test_pool_execute_decimal128_arrayvector() throws Exception {
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, false, false);
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        String script = ("\n" +
                "t = table(1000:0, `col0`col1`col2`col3`col4, [DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                "share t as ptt;\n" +
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col4=[[1,3.0000000000000000000000000000001,0.9999999999999999999999999999999999999],[-3.0000000000000000000000000000001,0,0.123456789]]\n" +
                "t.tableInsert(col0,col1,col2,col3,col4)\n" +
                "\n");
        List<DBTask> tasks = new ArrayList<>();
        BasicDBTask task = new BasicDBTask(script);
        tasks.add(task);
        pool.execute(tasks);
        pool.waitForThreadCompletion();
        BasicTable res = (BasicTable) connection.run("ptt");
        System.out.println(res.getString());
        assertEquals(5, res.columns());
        assertEquals(2, res.rows());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0000,3.0000,100000.0000],[-1.0000,0.0000,0.1235]]",res.getColumn(1).getString());
        assertEquals("[[1.0000000000,3.0000100000,100000.0000000000],[-1.0000000000,0.0000000000,0.1234567890]]",res.getColumn(2).getString());
        assertEquals("[[1.0000000000000000000,3.0000100000000000000,99999.9999999999849005056],[-1.0000000000000000000,0.0000000000000000000,0.1234567890000000000]]",res.getColumn(3).getString());
        assertEquals("[[1.0000000000000000000000000000000000000,3.0000000000000000000000000000000000000,1.0000000000000000000000000000000000000],[-3.0000000000000000000000000000000000000,0.0000000000000000000000000000000000000,0.1234567889999999866557890289987485696]]",res.getColumn(4).getString());
    }
    @Test
    public void test_pool_execute_decimal128_arrayvector_compress_true() throws Exception {
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, false, false);
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect(HOST, PORT, "admin", "123456");
        String script = ("\n" +
                "t = table(1000:0, `col0`col1`col2`col3`col4, [DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                "share t as ptt;\n" +
                "col0=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col1=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col2=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col3=[[1,3.00001,99999.99999999999],[-1,0,0.123456789]]\n" +
                "col4=[[1,3.0000000000000000000000000000001,0.9999999999999999999999999999999999999],[-3.0000000000000000000000000000001,0,0.123456789]]\n" +
                "t.tableInsert(col0,col1,col2,col3,col4)\n" +
                "\n");
        List<DBTask> tasks = new ArrayList<>();
        BasicDBTask task = new BasicDBTask(script);
        tasks.add(task);
        pool.execute(tasks);
        pool.waitForThreadCompletion();
        BasicTable res = (BasicTable) connection.run("ptt");
        System.out.println(res.getString());
        assertEquals(5, res.columns());
        assertEquals(2, res.rows());
        assertEquals("[[1,3,100000],[-1,0,0]]",res.getColumn(0).getString());
        assertEquals("[[1.0000,3.0000,100000.0000],[-1.0000,0.0000,0.1235]]",res.getColumn(1).getString());
        assertEquals("[[1.0000000000,3.0000100000,100000.0000000000],[-1.0000000000,0.0000000000,0.1234567890]]",res.getColumn(2).getString());
        assertEquals("[[1.0000000000000000000,3.0000100000000000000,99999.9999999999849005056],[-1.0000000000000000000,0.0000000000000000000,0.1234567890000000000]]",res.getColumn(3).getString());
        assertEquals("[[1.0000000000000000000000000000000000000,3.0000000000000000000000000000000000000,1.0000000000000000000000000000000000000],[-3.0000000000000000000000000000000000000,0.0000000000000000000000000000000000000,0.1234567889999999866557890289987485696]]",res.getColumn(4).getString());
    }
    @Test
    public void test_pool_execute_timeout_10000() throws Exception {
        ExclusiveDBConnectionPool connectionPool = new ExclusiveDBConnectionPool(HOST, PORT,
                "admin", "123456", 3, false, true,
                ipports,"", false, false, false);
        long start = System.nanoTime();
        connectionPool.execute(new BasicDBTask("sleep(10000);sleep(10000);"), 10000);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,(end - start) / 1000000>=10000);
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
    @Test
    public void test_PartitionedTableAppender_illegal_string() throws Exception {
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10,,'TSDB');t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]);db.createPartitionedTable(t,'t1', 'id',,'id')");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://db1","t1","id",pool);
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
        int x = appender.append(tmpTable);
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
        pool.shutdown();
    }
    @Test
    public void test_PartitionedTableAppender_dbUrl_null() throws Exception {
        conn.run("t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]); share t as tt");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        String re = null;
        try{
            PartitionedTableAppender appender = new PartitionedTableAppender(null,"tt",null,pool);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("Can't find specified partition column name.", re);
        pool.shutdown();
    }
    @Test
    public void test_PartitionedTableAppender_dbUrl_null_1() throws Exception {
        conn.run("t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]); share t as tt");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        String re = null;
        try{
            PartitionedTableAppender appender = new PartitionedTableAppender("","tt","",pool);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("Can't find specified partition column name.", re);
        pool.shutdown();
    }
    @Test(timeout = 120000)
    public void test_PartitionedTableAppender_allDataType_null() throws Exception {
        List<String> colNames = new ArrayList<String>();
        colNames.add("boolv");
        colNames.add("charv");
        colNames.add("shortv");
        colNames.add("intv");
        colNames.add("longv");
        colNames.add("doublev");
        colNames.add("floatv");
        colNames.add("datev");
        colNames.add("monthv");
        colNames.add("timev");
        colNames.add("minutev");
        colNames.add("secondv");
        colNames.add("datetimev");
        colNames.add("timestampv");
        colNames.add("nanotimev");
        colNames.add("nanotimestampv");
        colNames.add("symbolv");
        colNames.add("stringv");
        colNames.add("uuidv");
        colNames.add("datehourv");
        colNames.add("ippaddrv");
        colNames.add("int128v");
        colNames.add("blobv");
        colNames.add("complexv");
        colNames.add("pointv");
        colNames.add("decimal32v");
        colNames.add("decimal64v");
        colNames.add("decimal128V");

        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicBooleanVector(0));
        cols.add(new BasicByteVector(0));
        cols.add(new BasicShortVector(0));
        cols.add(new BasicIntVector(0));
        cols.add(new BasicLongVector(0));
        cols.add(new BasicDoubleVector(0));
        cols.add(new BasicFloatVector(0));
        cols.add(new BasicDateVector(0));
        cols.add(new BasicMonthVector(0));
        cols.add(new BasicTimeVector(0));
        cols.add(new BasicMinuteVector(0));
        cols.add(new BasicSecondVector(0));
        cols.add(new BasicDateTimeVector(0));
        cols.add(new BasicTimestampVector(0));
        cols.add(new BasicNanoTimeVector(0));
        cols.add(new BasicNanoTimestampVector(0));
        cols.add(new BasicSymbolVector(0));
        cols.add(new BasicStringVector(0));
        cols.add(new BasicUuidVector(0));
        cols.add(new BasicDateHourVector(0));
        cols.add(new BasicIPAddrVector(0));
        cols.add(new BasicInt128Vector(0));
        cols.add(new BasicStringVector(new String[0],true));
        cols.add(new BasicComplexVector(0));
        cols.add(new BasicPointVector(0));
        cols.add(new BasicDecimal32Vector(0,0));
        cols.add(new BasicDecimal64Vector(0,0));
        cols.add(new BasicDecimal128Vector(0,0));
        conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n" +
                " db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n " +
                "t= table(100:0,`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`complexv`pointv`decimal32v`decimal64v`decimal128v, " +
                "[BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, complex, POINT, DECIMAL32(3), DECIMAL64(4),DECIMAL128(10) ]);\n" +
                " pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://empty_table","pt","stringv",pool);
        int res = appender.append(new BasicTable(colNames, cols));
        assertEquals(0, res);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        pool.shutdown();
        conn.close();
    }
    @Test(timeout = 120000)
    public void test_PartitionedTableAppender_allDataType_array_null() throws Exception {
        List<String> colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("boolv");
        colNames.add("charv");
        colNames.add("shortv");
        colNames.add("intv");
        colNames.add("longv");
        colNames.add("doublev");
        colNames.add("floatv");
        colNames.add("datev");
        colNames.add("monthv");
        colNames.add("timev");
        colNames.add("minutev");
        colNames.add("secondv");
        colNames.add("datetimev");
        colNames.add("timestampv");
        colNames.add("nanotimev");
        colNames.add("nanotimestampv");
        //colNames.add("symbolv");
        //colNames.add("stringv");
        colNames.add("uuidv");
        colNames.add("datehourv");
        colNames.add("ippaddrv");
        colNames.add("int128v");
        //colNames.add("blobv");
        colNames.add("complexv");
        colNames.add("pointv");
        colNames.add("decimal32v");
        colNames.add("decimal64v");
        colNames.add("decimal128V");

        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicIntVector(0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BOOL_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BYTE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SHORT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_LONG_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_FLOAT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MONTH_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MINUTE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SECOND_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATETIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIMESTAMP_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_UUID_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_IPADDR_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT128_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_COMPLEX_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_POINT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL32_ARRAY,0,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL64_ARRAY,0,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL128_ARRAY,0,0));
        conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n" +
                " db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n " +
                "t= table(100:0,`id`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`complexv`pointv`decimal32v`decimal64v`decimal128v, " +
                "[INT, BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], COMPLEX[], POINT[], DECIMAL32(3)[], DECIMAL64(4)[],DECIMAL128(10)[] ]);\n" +
                " pt=db.createPartitionedTable(t,`pt,`id,,`id);");
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://empty_table","pt","id", pool);
        int res = appender.append(new BasicTable(colNames, cols));
        assertEquals(0, res);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        pool.shutdown();
        conn.close();
    }

    @Test
    public void Test_PartitionedTableAppender_iotAnyVector() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n" ;
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
        BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
        BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
        BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
        BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
        BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
        BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
        BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
        BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value);\n select * from t");
        System.out.println(bt8.getString());
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType","pt","deviceId", pool);
        appender.append(bt);
        appender.append(bt1);
        appender.append(bt2);
        appender.append(bt3);
        appender.append(bt4);
        appender.append(bt5);
        appender.append(bt6);
        appender.append(bt7);
        appender.append(bt8);
        BasicTable bt10 = (BasicTable) conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
        assertEquals(11, bt10.rows());
        System.out.println(bt10.getString());
        assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", bt10.getColumn(3).getString());
        pool.shutdown();
    }

    @Test
    public void Test_PartitionedTableAppender_iotAnyVector_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n" ;
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
        BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
        BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
        BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
        BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
        BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
        BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
        BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
        BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value);\n select * from t");
        System.out.println(bt8.getString());
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, true, null,null,true,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType","pt","deviceId", pool);
        appender.append(bt);
        appender.append(bt1);
        appender.append(bt2);
        appender.append(bt3);
        appender.append(bt4);
        appender.append(bt5);
        appender.append(bt6);
        appender.append(bt7);
        appender.append(bt8);
        BasicTable bt10 = (BasicTable) conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
        assertEquals(11, bt10.rows());
        System.out.println(bt10.getString());
        assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", bt10.getColumn(3).getString());
        pool.shutdown();
    }
    @Test
    public void Test_PartitionedTableAppender_iotAnyVector_null() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n" ;
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
        BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
        BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
        BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
        BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
        BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
        BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
        BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
        BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
        List<String> colNames = new ArrayList<String>();
        colNames.add("deviceId");
        colNames.add("timestamp");
        colNames.add("location");
        colNames.add("value");
        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicIntVector(0));
        cols.add(new BasicTimestampVector(0));
        cols.add(new BasicStringVector(0));
        cols.add(new BasicIntVector(0));
        BasicTable bt9 = new BasicTable(colNames,cols);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType","pt","deviceId", pool);

        appender.append(bt);
        appender.append(bt1);
        appender.append(bt2);
        appender.append(bt3);
        appender.append(bt4);
        appender.append(bt5);
        appender.append(bt6);
        appender.append(bt7);
        appender.append(bt8);
        appender.append(bt9);
        BasicTable bt10 = (BasicTable) conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
        assertEquals(9, bt10.rows());
        System.out.println(bt10.getColumn(3).getString());
        assertEquals("[,,,,,,,,]", bt10.getColumn(3).getString());
        pool.shutdown();
    }

    @Test
    public void Test_PartitionedTableAppender_iotAnyVector_null_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n" ;
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
        BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
        BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
        BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
        BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
        BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
        BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
        BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
        BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
        List<String> colNames = new ArrayList<String>();
        colNames.add("deviceId");
        colNames.add("timestamp");
        colNames.add("location");
        colNames.add("value");
        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicIntVector(0));
        cols.add(new BasicTimestampVector(0));
        cols.add(new BasicStringVector(0));
        cols.add(new BasicIntVector(0));
        BasicTable bt9 = new BasicTable(colNames,cols);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456", 10, true, true, null,null,true,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType","pt","deviceId", pool);
        appender.append(bt);
        appender.append(bt1);
        appender.append(bt2);
        appender.append(bt3);
        appender.append(bt4);
        appender.append(bt5);
        appender.append(bt6);
        appender.append(bt7);
        appender.append(bt8);
        appender.append(bt9);
        BasicTable bt10 = (BasicTable) conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
        assertEquals(9, bt10.rows());
        System.out.println(bt10.getColumn(3).getString());
        assertEquals("[,,,,,,,,]", bt10.getColumn(3).getString());
        pool.shutdown();
    }

    @Test
    public void Test_PartitionedTableAppender_iotAnyVector_big_data() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType1\")) dropDatabase(\"dfs://testIOT_allDateType1\")\n" +
                "     create database \"dfs://testIOT_allDateType1\" partitioned by   RANGE(100000*(0..10)),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType1\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType1\",\"pt\");\n" ;
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("t=table(take(1..100000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(char(1..100000),1000000) as value);\n select * from t");
        BasicTable bt1 = (BasicTable)conn.run("t=table(take(100001..200000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(short(1..100000),1000000) as value);\n select * from t");
        BasicTable bt2 = (BasicTable)conn.run("t=table(take(200001..300000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(int(1..100000),1000000) as value);\n select * from t");
        BasicTable bt3 = (BasicTable)conn.run("t=table(take(300001..400000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(long(1..100000),1000000) as value);\n select * from t");
        BasicTable bt4 = (BasicTable)conn.run("t=table(take(400001..500000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(true false null,1000000) as value);\n select * from t");
        BasicTable bt5 = (BasicTable)conn.run("t=table(take(500001..600000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33f 0 4.44f,1000000) as value);\n select * from t");
        BasicTable bt6 = (BasicTable)conn.run("t=table(take(600001..700000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33 0 4.44,1000000) as value);\n select * from t");
        BasicTable bt7 = (BasicTable)conn.run("t=table(take(700001..800000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(\"bb\"+string(0..100000), 1000000) as value);\n select * from t");
        BasicTable bt8 = (BasicTable)conn.run("t=table(take(800001..900000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, symbol(take(NULL`bbb`AAA,1000000)) as value);\n select * from t");
        System.out.println(bt8.getString());
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType1","pt","deviceId", pool);
        long start = System.nanoTime();
        appender.append(bt);
        appender.append(bt1);
        appender.append(bt2);
        appender.append(bt3);
        appender.append(bt4);
        appender.append(bt5);
        appender.append(bt6);
        appender.append(bt7);
        appender.append(bt8);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);

        BasicTable bt10 = (BasicTable) conn.run("select count(*) from loadTable(\"dfs://testIOT_allDateType1\",`pt);");
        assertEquals("9000000", bt10.getColumn(0).getString(0));
        pool.shutdown();
    }

    //@Test
    public void Test_PartitionedTableAppender_iotAnyVector_1() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT_allDateType1\")) dropDatabase(\"dfs://testIOT_allDateType1\")\n" +
                "     create database \"dfs://testIOT_allDateType1\" partitioned by   RANGE(100000*(0..10)),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT_allDateType1\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT_allDateType1\",\"pt\");\n" ;
        conn.run(script);
        BasicByte bbyte = new BasicByte((byte) 127);
        BasicShort bshort = new BasicShort((short) 0);
        BasicInt bint = new BasicInt(-4);
        BasicLong blong = new BasicLong(-4);
        BasicBoolean bbool = new BasicBoolean(false);
        BasicFloat bfloat = new BasicFloat((float) 1.99);
        BasicDouble bdouble = new BasicDouble(1.99);
        BasicString bsting = new BasicString("最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa");
        Scalar[] scalar = new Scalar[]{bbyte, bshort, bint, blong, bbool, bfloat, bdouble, bsting, bdouble, bsting};
        BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);
        BasicIntVector deviceId = new BasicIntVector(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        BasicAnyVector BIV1 = new BasicAnyVector(10);
        for (int i = 0; i < 10; i++) {
            BIV1.set(i, new BasicInt(i));
        }

        BasicTimestampVector timestamp = new BasicTimestampVector(new long[]{1577836800001l, 1577836800002l, 1577836800003l, 1577836800004l, 1577836800005l, 1577836800006l, 1577836800007l, 1577836800008l, 1577836800009l, 1577836800010l});
        BasicSymbolVector location = new BasicSymbolVector(Arrays.asList(new String[]{"d1d", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10"}));

        List<Entity> args = Arrays.asList(deviceId, timestamp, location, BIV);
        List<String> colNames = Arrays.asList("deviceId", "timestamp", "location", "BIV");
        List<Vector> cols = Arrays.asList(deviceId, timestamp, location, BIV);
        BasicTable table = new BasicTable(colNames, cols);

        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType1","pt","deviceId", pool);
        appender.append(table);
        BasicTable bt10 = (BasicTable) conn.run("select count(*) from loadTable(\"dfs://testIOT_allDateType1\",`pt);");
        assertEquals("10", bt10.getColumn(0).getString(0));
        pool.shutdown();
    }

    @Test(timeout = 120000)
    public void test_ChunkInTransaction_insert() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_ChunkInTransaction\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE,1..6)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n");
        conn.run(sb.toString());

        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://test_ChunkInTransaction","pt","volume", pool);
        PartitionedTableAppender appender1 = new PartitionedTableAppender("dfs://test_ChunkInTransaction","pt","volume", pool);

        List<String> colNames = Arrays.asList("volume", "valueTrade");
        BasicIntVector volume = new BasicIntVector(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        BasicDoubleVector valueTrade = new BasicDoubleVector(Arrays.asList(1.0, 2.9, 3.9, 4.9, 5.9, 6.7, 7.7, 8.7, 9.7, 10.7));
        List<Vector> cols = Arrays.asList(volume, valueTrade);
        BasicTable table = new BasicTable(colNames, cols);

        class MyThread1 extends Thread {
            @Override
            public void run() {
                try {
                    int rows = appender.append(table);
                    System.out.println("rows：" + rows);
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println( e.getMessage());
                }
            }
        }
        final String[] re = {null};

        class MyThread2 extends Thread {
            @Override
            public void run() {
                try {
                    int rows1 = appender1.append(table);
                    System.out.println("rows1：" + rows1);
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println(e.getMessage());
                    re[0] = e.getMessage();

                }
            }
        }

        MyThread1 thread1 = new MyThread1();
        MyThread2 thread2 = new MyThread2();
        thread1.start();
        Thread.sleep(5);
        thread2.start();
        thread1.join();
        thread2.join();
        System.out.println(re[0].toString());
        assertEquals(true, re[0].toString().contains("is currently locked and in use"));
        pool.shutdown();
    }

}

