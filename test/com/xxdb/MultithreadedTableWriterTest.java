package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public  class MultithreadedTableWriterTest implements Runnable {
    private Logger logger_ = Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    public static String HOST = "192.168.1.23";
    public static Integer PORT = 8848;
    public static Integer insertTime = 5000;
    //private final int id;
    private static MultithreadedTableWriter mutithreadTableWriter_ = null;

    public MultithreadedTableWriterTest() {
    }

    @Before
    public void prepare() throws IOException {
        conn = new DBConnection(false,true,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public void checkData(BasicTable exception, BasicTable resTable) {
        assertEquals(exception.rows(), resTable.rows());
        for (int i = 0; i < exception.columns(); i++) {
            System.out.println("col" + resTable.getColumnName(i));
            assertEquals(exception.getColumn(i).getString(), resTable.getColumn(i).getString());
        }

    }

    @After
    public void close() {
        conn.close();
    }

    @Override
    public void run() {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
    }

    /**
     * Parameter test
     * @throws Exception
     */
    @Test
    public void test_MultithreadedTableWriter_compress_lessthan_cols() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        boolean noErro = true;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10, 2,
                    5, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }
        catch (RuntimeException ex){
           assertEquals(ex.getMessage(),"Compress type size doesn't match column size 3");
        }
    }

    @Test
    public void test_MultithreadedTableWriter_compress_morethan_cols() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10, 2,
                    5, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }
        catch (RuntimeException ex){
            assertEquals(ex.getMessage(),"Compress type size doesn't match column size 3");
        }
    }

    @Test
    public void test_MultithreadedTableWriter_throttle() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10, 2,
                5, "date");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", 1);
        assertTrue(b);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        Thread.sleep(4000);
        bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_host_wrong() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter("192.178.1.321", PORT, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");

    }

    @Test(expected = ConnectException.class)
    public void test_MultithreadedTableWriter_port_wrong() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, 0, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_userid_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "s", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_pwd_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "12356",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_memory_dbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "tt", "", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_memory_tbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "t1", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_dfs_tbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "t1", false, false, null, 10000, 1,
                5, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_dfs_dbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "s", "pt1", false, false, null, 10000, 1,
                5, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_MultithreadedTableWriter_batchsize() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 3, 3000,
                1, "date");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", 0);
        mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", 1);
        assertEquals(true, b);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicLong(1));
            row.add(new BasicString("1"));
            row.add(new BasicLong(i + 2));
            tb.add(row);
        }
        mutithreadTableWriter_.insert(tb, pErrorInfo);
        Thread.sleep(5000);
        bt = (BasicTable) conn.run("select * from t1;");
        assertEquals("[0,1,2,3,4]", bt.getColumn(2).getString());
        assertEquals(5, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_batchsize_Negtive() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, -10000, 1,
                5, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_batchsize_0() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 0, 1,
                5, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_throttle_Negtive() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, -10,
                5, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_threadcount_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                0, "date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_threadcount_null() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                10, "");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_partcolname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                5, "Date");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public void test_MultithreadedTableWriter_partcolname_not_pertitioncol() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                5, "vwap");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_MultithreadedTableWriter_threadcount() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 4, 2,
                2, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", 0);
        mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "B", 0);
        assertEquals(true, b);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicLong(1));
            row.add(new BasicString("A"));
            row.add(new BasicLong(i + 1));
            tb.add(row);
        }
        mutithreadTableWriter_.insert(tb, pErrorInfo);
        Thread.sleep(1000);
        bt = (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.getColumn("id").getString());
        System.out.println(bt.getColumn("values").getString());
        assertEquals("[0,1,2,3]", bt.getColumn(2).getString());
        assertEquals(4, bt.rows());
        for (int i = 0; i < 3; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicLong(1));
            row.add(new BasicString("B"));
            row.add(new BasicLong(i + 1));
            tb.add(row);
        }
        mutithreadTableWriter_.insert(tb, pErrorInfo);
        Thread.sleep(1000);
        bt = (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.getColumn("id").getString());
        System.out.println(bt.getColumn("values").getString());
        assertEquals(8, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_cols_diffwith_table() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "date");
        Boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1);
        assertEquals(false, b);
        assertEquals("Parameter length mismatch 1 expect 2",pErrorInfo.errorInfo);
        b = mutithreadTableWriter_.insert(pErrorInfo, 1, 1, 1);
        assertEquals(false, b);
        assertEquals("Parameter length mismatch 3 expect 2",pErrorInfo.errorInfo);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_allnull_willfail() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");
        Boolean b = mutithreadTableWriter_.insert(pErrorInfo, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals(false, b);
        assertEquals("Can't insert a Null row.",pErrorInfo.errorInfo);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
    }

    @Test
    public void test_insert_partnull() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");
        Boolean b = mutithreadTableWriter_.insert(pErrorInfo, null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            System.out.println(bt.getColumn(i).get(0).toString());
        }
    }

    @Test
    public void test_insert_dfs_part_null() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db  = database(dbName, VALUE,`A`B`C`D);\n" + "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "pt = db.createTable(t,`pt);";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1, 1,
                1, "bool");
        Boolean b = mutithreadTableWriter_.insert(pErrorInfo, true, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt);");
        assertEquals(1, bt.rows());

    }

    @Test
    public void test_insert_type_diffwith_table() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "date");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, "2012.01.02");
        assertEquals(false, b);
        assertEquals("Invalid object error java.lang.RuntimeException: Failed to insert 2012.01.02 to DT_DATE, unsupported data type.",pErrorInfo.errorInfo);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    public void writeData(int num, List<List<Entity>> tb) {
        for (int i = 0; i < 10000; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(num));
            row.add(new BasicDouble(5));
            tb.add(row);
        }
    }

    @Test
    public void test_getUnwrittenData_lessThan_throttle_batchsize() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 100000, 10,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert(pErrorInfo, i, new Date());
        }
        List<List<Entity>> unwrite = new ArrayList<>();
        mutithreadTableWriter_.getUnwrittenData(unwrite);
        assertEquals(15, unwrite.size());
        for (int i = 0; i < 15; i++) {
            assertEquals(String.valueOf(i), unwrite.get(i).get(0).getString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_getUnwrittenData_Parallel_to_the_same_partition() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, RANGE,0 2 4 6)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                2, "volume");
        List<List<Entity>> tb1 = new ArrayList<>();
        List<List<Entity>> tb2 = new ArrayList<>();
        writeData(0,tb1);
        writeData(1,tb2);
        mutithreadTableWriter1.insert(tb1, pErrorInfo);
        mutithreadTableWriter2.insert(tb2, pErrorInfo);
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter1.waitForThreadCompletion();

        List<List<Entity>> unwrite1 = new ArrayList<>();
        List<List<Entity>> unwrite2 = new ArrayList<>();
        mutithreadTableWriter1.getUnwrittenData(unwrite1);
        mutithreadTableWriter2.getUnwrittenData(unwrite2);


        assertTrue(unwrite1.size() == 0);
        assertTrue(unwrite2.size() >0);

    }

    @Test
    public void test_getStatus_write_successful() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert(pErrorInfo, i, new Date());
        }
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadTableWriter_.getStatus(status);
        assertFalse(status.isExiting);
        assertEquals(0, status.sendFailedRows);
        mutithreadTableWriter_.waitForThreadCompletion();
        mutithreadTableWriter_.getStatus(status);
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        ;
        assertTrue(status.isExiting);
        System.out.println(status.threadStatusList.get(0).toString());
    }

    @Test
    public void test_getStatus_insert_successful_normalData() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", true, false, null, 100000, 1000,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert(pErrorInfo, i, new Date());
        }
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadTableWriter_.getStatus(status);
        assertFalse(status.isExiting);
        assertEquals(0, status.sendFailedRows);
        assertEquals(15, status.unsentRows);
        assertEquals(0, status.sentRows);
        mutithreadTableWriter_.waitForThreadCompletion();
        mutithreadTableWriter_.getStatus(status);
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        assertTrue(status.isExiting);
    }

    @Test
    public void test_getStatus_insert_FAILED() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert(pErrorInfo, i);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadTableWriter_.getStatus(status);
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        assertEquals(0, status.sentRows);
    }

    @Test
    public void test_getStatus_write_failed() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = null;
        MultithreadedTableWriter mutithreadTableWriter2 = null;
        try {
            mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                    2, "volume");
            mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                    2, "volume");
            List<List<Entity>> tb = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                tb.add(row);
            }
            for (int i = 0; i < 1; i++) {
                mutithreadTableWriter1.insert(tb, pErrorInfo);
                mutithreadTableWriter2.insert(tb, pErrorInfo);
            }
        } catch (Exception ex) {
        }
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();


        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadTableWriter1.getStatus(status);
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt)");
        assertEquals(bt.rows(), status.sentRows);

        MultithreadedTableWriter.Status status1 = new MultithreadedTableWriter.Status();
        mutithreadTableWriter2.getStatus(status1);
        assertTrue(status1.sendFailedRows>0);
        assertEquals(10000-status1.sendFailedRows, status1.unsentRows);
        assertEquals(0, status1.sentRows);

    }

    @Test
    public void test_insert_digital() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`short`long`float`double`id," +
                "[BOOL,SHORT,LONG,FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");
        for (int i = 0; i < 15; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, true, i, i, 1.1f + i, 45.2 + i, i);
            assertEquals(true, b);
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(15, bt.rows());
        for (int i = 0; i < 15; i++) {
            assertEquals("true", bt.getColumn("bool").get(i).getString());
            assertEquals(String.valueOf(i), bt.getColumn("short").get(i).getString());
            assertEquals(String.valueOf(i), bt.getColumn("long").get(i).getString());
            assertEquals(1.1f + i, bt.getColumn("float").get(i).getNumber());
            assertEquals(45.2 + i, bt.getColumn("double").get(i).getNumber());
            assertEquals(i, bt.getColumn("id").get(i).getNumber());
        }
    }

    @Test
    public void test_insert_digital_compress_lz4() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`short`long`float`double`id," +
                "[BOOL,SHORT,LONG,FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,
                Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});

        for (int i = 0; i < 15; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, true, i, i, 1.1f + i, 45.2 + i, i);
            assertEquals(true, b);
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(15, bt.rows());
        for (int i = 0; i < 15; i++) {
            assertEquals("true", bt.getColumn("bool").get(i).getString());
            assertEquals(String.valueOf(i), bt.getColumn("short").get(i).getString());
            assertEquals(String.valueOf(i), bt.getColumn("long").get(i).getString());
            assertEquals(1.1f + i, bt.getColumn("float").get(i).getNumber());
            assertEquals(45.2 + i, bt.getColumn("double").get(i).getNumber());
            assertEquals(i, bt.getColumn("id").get(i).getNumber());
        }
    }

    /**
     *
     *delta compress test
     */

    @Test(expected = RuntimeException.class)
    public  void test_insert_arrayVector_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`array," +
                "[INT,INT[]]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_DELTA});
    }

    @Test
    public void test_insert_delta_short_int_long() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`short`long," +
                "[INT,SHORT,LONG]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        for (int i = 0; i < 1048576; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, i, i%32768, i);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1048576, bt.rows());
        for (int i = 0; i < 1048576; i++) {
            assertEquals(i, bt.getColumn("int").get(i).getNumber());
            assertEquals((short)(i%32768), bt.getColumn("short").get(i).getNumber());
            assertEquals(Long.valueOf(i), bt.getColumn("long").get(i).getNumber());

        }
    }

    @Test
    public void test_insert_time_delta() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour`nanotime`nanotimestamp," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        LocalDateTime ld = LocalDateTime.of(2022,1,1,1,1,1,10000);

        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,
                Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT, DT, DT, DT , DT, DT,ld,ld);
            assertTrue(b);

        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("date").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+"M", bt.getColumn("month").get(i).getString());
        }
        sdf = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("second").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("datetime").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm:ss.SSS");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("timestamp").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf1.format(DT)+"m", bt.getColumn("minute").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT) + 'T' + sdf1.format(DT), bt.getColumn("datehour").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01.000010000", bt.getColumn("nanotime").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01:01:01.000010000", bt.getColumn("nanotimestamp").get(i).getString());
        }
    }

    @Test(expected = Exception.class)
    public  void test_insert_blob_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_bool_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`bool," +
                "[INT,BOOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
    }

    @Test(expected = Exception.class)
    public  void test_insert_char_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`char," +
                "[INT,CHAR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
    }

    @Test(expected = Exception.class)
    public  void test_insert_float_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,FLOAT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_double_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_symbol_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,SYMBOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_string_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,STRING]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});

        for (int i=0;i<5;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, "fd");
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(5,bt.rows());
        for (int i=0;i<5;i++) {
            assertEquals("fd", bt.getColumn("delta").get(i).getString());
        }
    }


    @Test(expected = Exception.class)
    public  void test_insert_UUID_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,UUID]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_IPADDR_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,IPADDR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = Exception.class)
    public  void test_insert_INT128_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,INT128]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    /**
     * lz4 compress test
     * @throws Exception
     */
    @Test
    public void test_insert_digital_0() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`short`long`float`double`id," +
                "[BOOL,SHORT,LONG,FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");

        for (int i = 0; i < 15; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, false, 0,0,0.0f,0.0,0);
            assertEquals(true, b);
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(15, bt.rows());
        for (int i = 0; i < 15; i++) {
            assertEquals("false", bt.getColumn("bool").get(i).getString());
            assertEquals(String.valueOf(0), bt.getColumn("short").get(i).getString());
            assertEquals(String.valueOf(0), bt.getColumn("long").get(i).getString());
            assertEquals(0.0f, bt.getColumn("float").get(i).getNumber());
            assertEquals(0.0, bt.getColumn("double").get(i).getNumber());
            assertEquals(0, bt.getColumn("id").get(i).getNumber());
        }
    }

    @Test
    public void test_insert_time_date_to_nanotime() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0,`date`nanotime," +
                "[DATE,NANOTIME]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT);
            assertFalse(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_time_date_to_nanotimestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`nanotimestamp," +
                "[DATE,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT);
            assertFalse(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
    }
    @Test
    public void test_insert_time_date() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT, DT, DT, DT , DT, DT);
            assertTrue(b);

        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("date").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+"M", bt.getColumn("month").get(i).getString());
        }
        sdf = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("second").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("datetime").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm:ss.SSS");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("timestamp").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf1.format(DT)+"m", bt.getColumn("minute").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT) + 'T' + sdf1.format(DT), bt.getColumn("datehour").get(i).getString());
        }
    }

    @Test
    public void test_insert_time_LocalTime() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0,`second`minute`time`nanotime," +
                "[SECOND,MINUTE,TIME,NANOTIME]);" +
                "share t as t1;");
        conn.run(sb.toString());

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                10, "second");
        LocalTime DT = LocalTime.of(1,1,1,1);
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT, DT, DT);
            assertTrue(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());

        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01", bt.getColumn("second").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01m", bt.getColumn("minute").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01.000", bt.getColumn("time").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01.000000001", bt.getColumn("nanotime").get(i).getString());
        }
    }

    @Test
    public void test_insert_time_LocalDate() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month," +
                "[DATE,MONTH]);" +
                "share t as t1;");
        conn.run(sb.toString());
        LocalDate DT = LocalDate.of(2022,1,1);

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT);
            assertTrue(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01", bt.getColumn("date").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01M", bt.getColumn("month").get(i).getString());
        }
    }

    @Test
    public void test_insert_time_LocalDateTime() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `datetime`datehour`timstamp`nanotime`nanotimstamp," +
                "[DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        LocalDateTime DT = LocalDateTime.of(2022,1,1,1,1,1,10000);

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "datetime");
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT, DT, DT, DT);
            assertTrue(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01:01:01", bt.getColumn("datetime").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01", bt.getColumn("datehour").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01:01:01.000", bt.getColumn("timstamp").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01.000010000", bt.getColumn("nanotime").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01:01:01.000010000", bt.getColumn("nanotimstamp").get(i).getString());
        }
    }

    @Test
    public void test_insert_time_lz4() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour`nanotime`nanotimestamp," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        LocalDateTime ld = LocalDateTime.of(2022,1,1,1,1,1,10000);

        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "date",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});
        for (int i = 0; i < 8; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, DT, DT, DT, DT, DT , DT, DT,ld,ld);
            assertTrue(b);

        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(8, bt.rows());
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("date").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+"M", bt.getColumn("month").get(i).getString());
        }
        sdf = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT), bt.getColumn("second").get(i).getString());
        }
        sdf = new SimpleDateFormat("yyyy.MM.dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("HH:mm:ss");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("datetime").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm:ss.SSS");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT)+'T'+sdf1.format(DT), bt.getColumn("timestamp").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH:mm");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf1.format(DT)+"m", bt.getColumn("minute").get(i).getString());
        }
        sdf1 = new SimpleDateFormat("HH");
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals(sdf.format(DT) + 'T' + sdf1.format(DT), bt.getColumn("datehour").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("01:01:01.000010000", bt.getColumn("nanotime").get(i).getString());
        }
        for (int i = 0; i < bt.rows(); i++) {
            assertEquals("2022.01.01T01:01:01.000010000", bt.getColumn("nanotimestamp").get(i).getString());
        }
    }

    @Test
    public void test_insert_string() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`symbol`string," +
                "[CHAR,SYMBOL,STRING]);" +
                "share t as t1;");
        sb.append("tt =  streamTable(1000:0, `char`symbol`string,[CHAR,SYMBOL,STRING]);" +
                "share tt as t2;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "char");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicByte((byte) 's'));
            row.add(new BasicString("2" + i));
            row.add(new BasicString("2" + i));
            tb.add(row);
            conn.run("tableInsert{t2}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 's', "2" + i, "2" + i);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        BasicTable ex = (BasicTable) conn.run("select * from t2;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }


    @Test
    public  void test_insert_empty_arrayVector()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<1000;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(1000,bt.rows());
        for (int i=0;i<1000;i++) {
            assertEquals("[]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
    }

    @Test
    public  void test_insert_arrayVector_different_length()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv`arrayv1`arrayv2," +
                "[INT,INT[],BOOL[],BOOL[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");

        boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{1},new Boolean[]{true,null,false},new Boolean[]{true});
        b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{},new Boolean[]{true,null,false},new Boolean[]{true});
        b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{1,2},new Boolean[]{true,null,false},new Boolean[]{true});
        b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{1,null,1},new Boolean[]{true,null,false},new Boolean[]{true});
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(4,bt.rows());
        assertEquals("[1]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(0).getString());
        assertEquals("[]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(1).getString());
        assertEquals("[1,2]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(2).getString());
        assertEquals("[1,,1]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(3).getString());
        for (int i=0;i<4;i++) {
            assertEquals("[true,,false]", ((BasicArrayVector)bt.getColumn("arrayv1")).getVectorValue(i).getString());
            assertEquals("[true]", ((BasicArrayVector)bt.getColumn("arrayv2")).getVectorValue(i).getString());
        }
    }

    /*
     *array vector type
     */

    @Test
    public  void test_insert_arrayVector_int()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Integer[]{1, i});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals(1, ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getNumber());
            assertEquals(i, ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getNumber());
        }
    }

    @Test
    public  void test_insert_arrayVector_char()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,CHAR[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Byte[]{'a','3'});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals("['a','3']", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
    }

    @Test
    public  void test_insert_arrayVector_bool()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,BOOL[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,new Boolean[]{true,false});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals("true",((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getString());
            assertEquals("false", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getString());
        }
    }

    @Test
    public  void test_insert_arrayVector_long()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,LONG[]]);" +
                "share t as t1;");
        int time=1024;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "arrayv");
        for (int i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, new Long[]{1l, Long.valueOf(i)});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(1l, ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getNumber());
            assertEquals(Long.valueOf(i), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getNumber());
        }
    }

    @Test
    public  void test_insert_arrayVector_short()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,SHORT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, new Short[]{1,i});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Short.valueOf("1"), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getNumber());
            assertEquals(Short.valueOf(""+i+""), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getNumber());
        }
    }

    @Test
    public  void test_insert_arrayVector_float()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,FLOAT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, new Float[]{0.0f,Float.valueOf(i)});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(0.0f, ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getNumber());
            assertEquals(Float.valueOf(i), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getNumber());
        }
    }

    @Test
    public  void test_insert_arrayVector_double()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,DOUBLE[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, new Double[]{Double.valueOf(0),Double.valueOf(i-10)});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Double.valueOf(0), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getNumber());
            assertEquals(Double.valueOf(i-10), ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getNumber());
        }
    }

    @Test
    public  void test_insert_arrayVector_date_month()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv1`arrayv2," +
                "[INT,DATE[],MONTH[]]); share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "arrayv1");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, new LocalDate[]{LocalDate.of(1,1,1)},
                    new LocalDate[]{LocalDate.of(2021,1,1)});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(LocalDate.of(1,1,1), ((BasicArrayVector)bt.getColumn("arrayv1")).getVectorValue(i).get(0).getTemporal());
            assertEquals("2021.01M", ((BasicArrayVector)bt.getColumn("arrayv2")).getVectorValue(i).get(0).getString());
        }
    }

    @Test
    public  void test_insert_arrayVector_time_minute_second()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `time`minute`second," +
                "[TIME[],MINUTE[],SECOND[]]);" +
                "share t as t1");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "time");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo,
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,1)});
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals("01:01:01.000", ((BasicArrayVector)bt.getColumn("time")).getVectorValue(i).get(0).getString());
            assertEquals("01:01m", ((BasicArrayVector)bt.getColumn("minute")).getVectorValue(i).get(0).getString());
            assertEquals("01:01:01", ((BasicArrayVector)bt.getColumn("second")).getVectorValue(i).get(0).getString());
        }
    }

    @Test
    public  void test_insert_arrayVector_datetime_timestamp_nanotime_nanotimstamp()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `datetime`timestamp`nanotime`nanotimstamp," +
                "[DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[]]);" +
                "share t as t1");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "datetime");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo,
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalTime[]{LocalTime.of(1,1,1,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i),
                            LocalDateTime.of(2022,2,1,1,1,2,45364654+i)});
            assertEquals(true, b);
        }

       // System.out.println(LocalDateTime.of(2022,2,1,1,1,2,033));
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        System.out.println(((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(0).get(0).getTemporal());

        for (int i=0;i<time;i++) {
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2), ((BasicArrayVector)bt.getColumn("datetime")).getVectorValue(i).get(0).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45000000), ((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(i).get(0).getTemporal());
            assertEquals(LocalTime.of(1,1,1,45364654+i), ((BasicArrayVector)bt.getColumn("nanotime")).getVectorValue(i).get(0).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45364654+i), ((BasicArrayVector)bt.getColumn("nanotimstamp")).getVectorValue(i).get(0).getTemporal());
        }
    }

    @Test
    public  void test_insert_arrayVector_otherType()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `uuid`int128`ipaddr," +
                "[UUID[],INT128[],IPADDR[]]);" +
                "share t as t1");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "uuid");
        BasicUuidVector bv= (BasicUuidVector) conn.run("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87',,'5d212a78-cc48-e3b1-4235-b4d91473ee87'])");
        BasicInt128Vector iv= (BasicInt128Vector) conn.run("int128(['e1671797c52e15f763380b45e841ec32',,'e1671797c52e15f763380b45e841ec32'])");
        BasicIPAddrVector ipv= (BasicIPAddrVector) conn.run("ipaddr(['192.168.1.13',,'192.168.1.13'])");
        for (short i=0;i<time;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo,new String[]{"5d212a78-cc48-e3b1-4235-b4d91473ee87",null,"5d212a78-cc48-e3b1-4235-b4d91473ee87"},new String[]{"e1671797c52e15f763380b45e841ec32",null,"e1671797c52e15f763380b45e841ec32"}
            ,new String[]{"192.168.1.13",null,"192.168.1.13"});
            assertEquals(true, b);
        }

        // System.out.println(LocalDateTime.of(2022,2,1,1,1,2,033));
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(bv.getString(), ((BasicArrayVector)(bt.getColumn("uuid"))).getVectorValue(i).getString());
            assertEquals(iv.getString(), ((BasicArrayVector)(bt.getColumn("int128"))).getVectorValue(i).getString());
            assertEquals(ipv.getString(), ((BasicArrayVector)(bt.getColumn("ipaddr"))).getVectorValue(i).getString());
        }
    }

    @Test
    public  void test_insert_blob()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");

        String blob=conn.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
                "t.toStdJson()").getString();
        for (int i=0;i<1025;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, blob);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(1025,bt.rows());
        for (int i=0;i<1025;i++) {
            assertEquals(blob, bt.getColumn("blob").get(i).getString());
        }
    }

    @Test
    public  void test_insert_wrongtype2()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`double," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i=0;i<20;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicInt(2));
            tb.add(row);
        }
        boolean b=mutithreadTableWriter_.insert(tb,pErrorInfo);
        assertEquals(true, b);
        for (int i=0;i<2;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicString("1"));
            tb.add(row);
        }
        b=mutithreadTableWriter_.insert(tb,pErrorInfo);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        List<List<Entity>> le=new ArrayList<>();
        mutithreadTableWriter_.getUnwrittenData(le);
        MultithreadedTableWriter.Status status=new MultithreadedTableWriter.Status();
        mutithreadTableWriter_.getStatus(status);
        assertEquals(bt.rows(),status.unsentRows+status.sendFailedRows+status.sentRows);
    }

    @Test
    public void test_insert_othertypes() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share t as t1;");
        sb.append("ext = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share ext as ext1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "uuid");
        List<List<Entity>> tb = new ArrayList<>();

        for (int i = 0; i < 15; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicUuid(321324, 32433));
            row.add(new BasicIPAddr(321324, 32433));
            row.add(new BasicInt128(454, 456));
            tb.add(row);
            conn.run("tableInsert{ext1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(15, bt.rows());
        BasicTable ex = (BasicTable) conn.run("select * from ext1;");
        assertEquals(15, ex.rows());
        for (int i = 0; i < bt.columns(); i++) {
            for (int j = 0; j < ex.rows(); j++) {
                assertEquals(ex.getColumn(i).get(j).getString(), bt.getColumn(i).get(j).getString());
            }
        }
    }

    @Test
    public void test_insert_string_otherType_lz4() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`symbol`string`uuid`ipaddr`int128," +
                "[CHAR,SYMBOL,STRING,UUID, IPADDR, INT128]);" +
                "share t as t1;");
        sb.append("tt = streamTable(1000:0, `char`symbol`string`uuid`ipaddr`int128,[CHAR,SYMBOL,STRING,UUID, IPADDR, INT128]);" +
                "share tt as t2;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 5, 1,
                1, "char",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});
        List<List<Entity>> tb = new ArrayList<>();
       // String blob=conn.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
               // "t.toStdJson()").getString();
        for (int i = 0; i < 20; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicByte((byte) 's'));
            row.add(new BasicString("2" + i));
            row.add(new BasicString("2" + i));
            row.add(new BasicUuid(321324, 32433));
            row.add(new BasicIPAddr(321324, 32433));
            row.add(new BasicInt128(454, 456));
            tb.add(row);
            conn.run("tableInsert{t2}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 's', "2" + i, "2" + i, "00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        BasicTable ex = (BasicTable) conn.run("select * from t2;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    /**
     * table types
     * @throws Exception
     */

    @Test
    public void test_insert_BasicType_in_java() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script="t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT]);" +
                "share t as t1;" +
                "tt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT]);\" +\n" +
                "                \"share tt as t2;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10, 1,
                1, "short");
        List<List<Entity>> tb = new ArrayList<>();
        Month mon=LocalDate.of(2022,2,2).getMonth();
        for (int i = 0; i < 10000; i++) {
            boolean b=mutithreadTableWriter_.insert(pErrorInfo,new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
            new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),new BasicFloat(2.312f),new BasicDouble(3.2),
                    new BasicString("sedf"+i),new BasicString("sedf"),new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),new BasicInt(21));
            assertTrue(b);
            List<Entity> args = Arrays.asList(new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),new BasicFloat(2.312f),new BasicDouble(3.2),
                    new BasicString("sedf"+i),new BasicString("sedf"),new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),new BasicInt(21));
            conn.run("tableInsert{t2}", args);

        }
        Thread.sleep(2000);
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by symbol");
        BasicTable  res= (BasicTable) conn.run("select * from t2 order by symbol");
        assertEquals(10000,ex.rows());
        checkData(ex,res);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_keytable() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script =
                "t=keyedStreamTable(`sym,1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10, 1,
                1, "tradeDate");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            mutithreadTableWriter_.insert(pErrorInfo, "2"+i%2, LocalDateTime.of(2012, 1, i % 10 + 1, 1, i%10), i + 0.1, i + 0.1, i % 10, i + 0.1);
        }
        Thread.sleep(2000);
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        assertEquals(2,ex.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void test_insert_dt_multipleThreadCount() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void test_insert_tsdb_dt_multipleThreadCount() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01..2012.01.30,,'TSDB')\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreateTable(dbHandle=db, table=t, tableName=`pt,sortColumns=`sym)\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dt_multipleThread() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                1, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_TSDBdt_multipleThread() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01..2012.01.30,,'TSDB')\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreateTable(dbHandle=db, table=t, tableName=`pt,sortColumns=`sym)\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                1, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dt_oneThread() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                1, "tradeDate");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i));
            row.add(new BasicDateTime(LocalDateTime.of(2012, 1, i % 10 + 1, 1, i)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b=mutithreadTableWriter_.insert(pErrorInfo, "2" + i, LocalDateTime.of(2012, 1, i % 10 + 1, 1, i), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertTrue(b);
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_value() throws Exception {
        //ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, month(2012.01.01)+0..1)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, (i % 10) + 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_hash() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, HASH, [SYMBOL,3])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                20, "sym");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_range_outof_partitions() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, RANGE, 2022.01.01+1..10*2)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 where tradeDate>=2022.01.03,tradeDate< 2022.01.21 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_value_hash() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, 2012.01.01..2012.01.30)\n" +
                "\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i));
            row.add(new BasicDate(LocalDate.of(2022, 1, i + 1)));
            row.add(new BasicTime(LocalTime.of(1, i)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i, LocalDate.of(2022, 1, i + 1), LocalTime.of(1, i), i + 0.1, i + 0.1, i, i + 0.1);
            assertEquals(true, b);
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        assertEquals(10, bt.rows());
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    @Test
    public void test_insert_dfs_value_value() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, 2012.01.01..2012.01.30)\n" +
                "\tdb2=database(\"\", VALUE, 1..2)\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_value_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, 2012.01.01..2012.01.30)\n" +
                "\tdb2=database(\"\", RANGE,0..10*5)\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_range_value() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, 2012.01.01..2012.01.30)\n" +
                "\tdb2=database(\"\", RANGE,0..10*5)\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_range_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", RANGE,0..10*2)\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"],compressMethods={tradeDate:\"delta\", volume:\"delta\"})\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_range_hash() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"volume\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_hash_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }


    @Test
    public void test_insert_dfs_hash_hash() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", HASH, [DATEHOUR,3])\n" +
                "\tdb2=database(\"\", HASH,[SYMBOL,3])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"sym\"],compressMethods={tradeDate:\"delta\", volume:\"delta\"})\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_hash_value() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", HASH, [DATEHOUR,3])\n" +
                "\tdb2=database(\"\", VALUE,0..5*200)\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"volume\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1024, 1,
                2, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_value_hash_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, date(2022.01.01)+0..2)\n" +
                "\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
                "\tdb3=database(\"\", RANGE,0..10*5)\n" +
                "\tdb=database(dbName, COMPO, [db1, db2,db3], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"sym\", \"volume\"],compressMethods={tradeDate:\"delta\", volume:\"delta\"})\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1024, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_streamTable_multipleThread() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script = "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as t2;";

        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t2", "", false, false, null, 1024, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < threadTime; i++) {
            new Thread(new MultithreadedTableWriterTest()).start();
        }
        for (int j = 0; j < threadTime; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{t1}", row);
            }
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from t2 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_PartitionType_datehour() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, datehour(2022.01.01)+0..3)\n" +
                "\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        for (int i = 0; i < 1; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, i, i + 0.1);
            assertFalse(b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_multiple_mutithreadTableWriter_sameTable() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                2, "volume");
        List<List<Entity>> tb1 = new ArrayList<>();
        List<List<Entity>> tb2 = new ArrayList<>();
        writeData(1, tb1);
        writeData(2, tb2);
        for (int i = 0; i < 10; i++) {
            mutithreadTableWriter1.insert(tb1, pErrorInfo);
            mutithreadTableWriter2.insert(tb2, pErrorInfo);
        }
        for (int n = 0; n < 10; n++) {
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(2));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
        }
        //Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
    }

    @Test
    public void test_insert_dfs_multiple_mutithreadTableWriter_differentTable() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"]);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt2, partitionColumns=[\"volume\"]);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt3, partitionColumns=[\"volume\"]);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt4, partitionColumns=[\"volume\"]);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt5, partitionColumns=[\"volume\"]);\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt1", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt2", false, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt3", false, false, null, 1000, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter4 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt4", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter5 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt5", false, false, null, 10, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        writeData(1, tb);
        writeData(2, tb);
        for (int i = 0; i < 10; i++) {
            mutithreadTableWriter1.insert(tb, pErrorInfo);
            mutithreadTableWriter2.insert(tb, pErrorInfo);
            mutithreadTableWriter3.insert(tb, pErrorInfo);
            mutithreadTableWriter4.insert(tb, pErrorInfo);
            mutithreadTableWriter5.insert(tb, pErrorInfo);
        }
        for (int n = 0; n < 10; n++) {
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(2));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
        }
        //Thread.sleep(2000);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter3.waitForThreadCompletion();
        mutithreadTableWriter4.waitForThreadCompletion();
        mutithreadTableWriter5.waitForThreadCompletion();
        BasicTable bt1 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt1)  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt2)  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt3)  order by volume,valueTrade;");
        BasicTable bt4 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt4)  order by volume,valueTrade;");
        BasicTable bt5 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt5)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt1);
        checkData(ex, bt2);
        checkData(ex, bt3);
        checkData(ex, bt4);
        checkData(ex, bt5);
    }

    @Test
    public void test_insert_dfs_multiple_mutithreadTableWriter_differentDataBase() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter1\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"]);");
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter2\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"],compressMethods={volume:\"delta\"});");
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter3\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                ";share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"],compressMethods={volume:\"delta\"});");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter1", "pt1", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter2", "pt1", false, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter3", "pt1", false, false, null, 1000, 1,
                20, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        writeData(1, tb);
        writeData(2, tb);
        for (int i = 0; i < 8; i++) {
            mutithreadTableWriter1.insert(tb, pErrorInfo);
            mutithreadTableWriter2.insert(tb, pErrorInfo);
            mutithreadTableWriter3.insert(tb, pErrorInfo);
        }
        for (int n = 0; n < 8; n++) {
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(2));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
        }
        //Thread.sleep(2000);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter3.waitForThreadCompletion();
        BasicTable bt1 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter1',`pt1)  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter2',`pt1)  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter3',`pt1)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt1);
        checkData(ex, bt2);
        checkData(ex, bt3);
    }

    @Test
    public void test_insert_multiple_mutithreadTableWriter_differentTable_status_isExiting() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t1=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t1 as st1;" +
                "t2=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t2 as st2;" +
                "t3=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t3 as st3;" +
                "t4=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t4 as st4;" +
                "t5=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t5 as st5;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "st1", "", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "st2", "", false, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "st3", "", false, false, null, 1000, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter4 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "st4", "", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter5 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "st5", "", false, false, null, 10, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        writeData(1, tb);
        writeData(2, tb);
        for (int i = 0; i < 10; i++) {
            mutithreadTableWriter1.insert(tb, pErrorInfo);
            mutithreadTableWriter2.insert(tb, pErrorInfo);
            mutithreadTableWriter3.insert(tb, pErrorInfo);
            mutithreadTableWriter4.insert(tb, pErrorInfo);
            mutithreadTableWriter5.insert(tb, pErrorInfo);
        }
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < insertTime; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicString("2" + i % 100));
                row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicDouble(i + 0.1));
                row.add(new BasicInt((i % 10) + 1));
                row.add(new BasicDouble(i + 0.1));
                tb.add(row);
                conn.run("tableInsert{st1}", row);
                conn.run("tableInsert{st2}", row);
                conn.run("tableInsert{st3}", row);
                conn.run("tableInsert{st4}", row);
                conn.run("tableInsert{st5}", row);
            }
        }
        BasicTable bt1 = (BasicTable) conn.run("select * from t1  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from t2  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from t3  order by volume,valueTrade;");
        BasicTable bt4 = (BasicTable) conn.run("select * from t4 order by volume,valueTrade;");
        BasicTable bt5 = (BasicTable) conn.run("select * from t5  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt1);
        checkData(ex, bt2);
        checkData(ex, bt3);
        checkData(ex, bt4);
        checkData(ex, bt5);
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadTableWriter1.getStatus(status);
        assertFalse(status.isExiting);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter1.getStatus(status);
        assertTrue(status.isExiting);
        mutithreadTableWriter2.getStatus(status);
        assertFalse(status.isExiting);
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter2.getStatus(status);
        assertTrue(status.isExiting);
        mutithreadTableWriter3.getStatus(status);
        assertFalse(status.isExiting);
        mutithreadTableWriter3.waitForThreadCompletion();
        mutithreadTableWriter3.getStatus(status);
        assertTrue(status.isExiting);
        mutithreadTableWriter4.getStatus(status);
        assertFalse(status.isExiting);
        mutithreadTableWriter4.waitForThreadCompletion();
        mutithreadTableWriter4.getStatus(status);
        assertTrue(status.isExiting);
        mutithreadTableWriter5.getStatus(status);
        assertFalse(status.isExiting);
        mutithreadTableWriter5.waitForThreadCompletion();
        mutithreadTableWriter5.getStatus(status);
        assertTrue(status.isExiting);
    }

    @Test
    public void test_insert_tsdb_multiple_mutithreadTableWriter_differentTable_useSSL() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5,,'TSDB');\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE]);\n" +
                "share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"],sortColumns=`volume,compressMethods={volume:\"delta\"});" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt2, partitionColumns=[\"volume\"],sortColumns=`volume);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt3, partitionColumns=[\"volume\"],sortColumns=`volume);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt4, partitionColumns=[\"volume\"],sortColumns=`volume);" +
                "createTable(dbHandle=db, table=t, tableName=`pt5,sortColumns=`volume,compressMethods={volume:\"delta\"});\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt1", true, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt2", true, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt3", true, false, null, 1000, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter4 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt4", true, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter5 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt5", true, false, null, 10, 1,
                1, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        writeData(1, tb);
        writeData(2, tb);
        for (int i = 0; i < 10; i++) {
            mutithreadTableWriter1.insert(tb, pErrorInfo);
            mutithreadTableWriter2.insert(tb, pErrorInfo);
            mutithreadTableWriter3.insert(tb, pErrorInfo);
            mutithreadTableWriter4.insert(tb, pErrorInfo);
            mutithreadTableWriter5.insert(tb, pErrorInfo);
        }
        for (int n = 0; n < 10; n++) {
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
            for (int i = 0; i < 10000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(2));
                row.add(new BasicDouble(5));
                conn.run("tableInsert{t1}", row);
            }
        }
        //Thread.sleep(2000);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter3.waitForThreadCompletion();
        mutithreadTableWriter4.waitForThreadCompletion();
        mutithreadTableWriter5.waitForThreadCompletion();
        BasicTable bt1 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt1)  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt2)  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt3)  order by volume,valueTrade;");
        BasicTable bt4 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt4)  order by volume,valueTrade;");
        BasicTable bt5 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt5)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt1);
        checkData(ex, bt2);
        checkData(ex, bt3);
        checkData(ex, bt4);
        checkData(ex, bt5);
    }

    @Test
    public void test_insert_tsdb_keepDuplicates() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 1..5,,'TSDB');\n" +
                "share keyedStreamTable(`volume`date,1:0, `volume`valueTrade`date, [INT, DOUBLE,DATE]) as t1\n" +
                "t=table(1:0, `volume`valueTrade`date, [INT, DOUBLE,DATE]); " +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"],sortColumns=`volume`date,compressMethods={volume:\"delta\"},keepDuplicates=LAST);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt2, partitionColumns=[\"volume\"],sortColumns=`volume`date,keepDuplicates=LAST);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt3, partitionColumns=[\"volume\"],sortColumns=`volume`date,keepDuplicates=LAST);" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt4, partitionColumns=[\"volume\"],sortColumns=`volume`date,keepDuplicates=LAST);" +
                "createTable(dbHandle=db, table=t, tableName=`pt5,sortColumns=`volume`date,compressMethods={volume:\"delta\"},keepDuplicates=LAST);\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt1", true, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt2", true, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt3", true, false, null, 1000, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter4 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt4", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter5 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt5", false, false, null, 10, 1,
                1, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicDouble(5));
            row.add(new BasicDate(LocalDate.now()));
            conn.run("tableInsert{t1}", row);
            tb.add(row);
        }
        mutithreadTableWriter1.insert(tb,pErrorInfo);
        mutithreadTableWriter2.insert(tb,pErrorInfo);
        mutithreadTableWriter3.insert(tb,pErrorInfo);
        mutithreadTableWriter4.insert(tb,pErrorInfo);
        mutithreadTableWriter5.insert(tb,pErrorInfo);
        //Thread.sleep(2000);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter3.waitForThreadCompletion();
        mutithreadTableWriter4.waitForThreadCompletion();
        mutithreadTableWriter5.waitForThreadCompletion();
        BasicTable bt1 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt1)  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt2)  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt3)  order by volume,valueTrade;");
        BasicTable bt4 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt4)  order by volume,valueTrade;");
        BasicTable bt5 = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt5)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt1);
        checkData(ex, bt2);
        checkData(ex, bt3);
        checkData(ex, bt4);
        checkData(ex, bt5);
    }

    @Test
    public void test_insert_dfs_length_eq_1024() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\nshare t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }


    @Test
    public void test_insert_dfs_length_eq_1048576() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }

    @Test
    public void test_insert_dfs_length_morethan_1048576() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1048577; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }


    @Test
    public void test_insert_streamtable_length_eq_1024() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "trades", "", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }

    @Test
    public void test_insert_streamtable_length_eq_1048576() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "trades", "", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
    }

    @Test
    public void test_insert_streamtable_length_morethan_1048576() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "trades", "", false, false, null, 1000, 1,
                2, "volume",new int[]{Vector.COMPRESS_LZ4,2,1,1,2,1});
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 1048577; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
//        assertEquals(ex.rows(), bt.rows());
//        for (int i = 0; i < ex.columns(); i++) {
//            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
//        }
    }


    @Test
    public void test_insert_streamtable_200cols() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script ="t=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;\n" +
                "addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;" +
                "addColumn(tt,\"col\"+string(1..200),take([DOUBLE],200));share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "trades", "", false, false, null, 1000, 1,
                2, "sym");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            for (int j=0;j<200;j++){
                row.add(new BasicDouble(i+0.1));
            }
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0),i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    @Test
    public void test_insert_dfstable_200cols() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script ="t=table(1:0, `sym`tradeDate, [SYMBOL,TIMESTAMP]);\n" +
                "addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, date(1..2),,'TSDB');\n" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"tradeDate\"],sortColumns=`tradeDate,compressMethods={tradeDate:\"delta\"});" ;

        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt1", false, false, null, 1000, 1,
                2, "tradeDate");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            for (int j=0;j<200;j++){
                row.add(new BasicDouble(i+0.1));
            }
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0),i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt1) order by sym,tradeDate;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }
}

