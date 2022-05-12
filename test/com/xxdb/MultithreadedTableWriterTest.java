package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import jdk.net.SocketFlow;
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
    public static ErrorCodeInfo pErrorInfo =new ErrorCodeInfo();;

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
    public void dropAllDB() throws IOException {
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("for(db in getClusterDFSDatabases()){\n" +
                "\tdropDatabase(db)\n" +
                "}");

        conn.run("try{undef(`t1,SHARED)}catch(ex){}");
        conn.run("try{undef(`t2,SHARED)}catch(ex){}");
        conn.run("try{undef(`st1,SHARED)}catch(ex){}");
        conn.run("try{undef(`st2,SHARED)}catch(ex){}");
        conn.run("try{undef(`st3,SHARED)}catch(ex){}");
        conn.run("try{undef(`st4,SHARED)}catch(ex){}");
        conn.run("try{undef(`st5,SHARED)}catch(ex){}");
        conn.close();
    }

    public void checkData(BasicTable exception, BasicTable resTable) {
        assertEquals(exception.rows(), resTable.rows());
        for (int i = 0; i < exception.columns(); i++) {
            System.out.println("col" + resTable.getColumnName(i));
            assertEquals(exception.getColumn(i).getString(), resTable.getColumn(i).getString());
        }

    }

    @After
    public void close() throws IOException {
        conn.close();
        dropAllDB();
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
     * Parameter check
     * @throws Exception
     */

    @Test
    public void test_MultithreadedTableWriter_host_wrong() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
              mutithreadTableWriter_ = new MultithreadedTableWriter("192.178.1.321", PORT, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("192.178.1.321",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test//(expected = ConnectException.class)
    public void test_MultithreadedTableWriter_port_wrong() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
             mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, 0, "admin", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("拒绝连接 (Connection refused)",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_userid_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "s", "123456",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'The user name or password is incorrect.' function: 'login'",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_pwd_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "12356",
                "t1", "", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'The user name or password is incorrect.' function: 'login'",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_memory_dbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "tt", "", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'Syntax Error: [line #1] Cannot recognize the token tt' script: 'schema(tt)'",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_memory_tbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "t1", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'table file does not exist: t1/t1.tbl' script: 'schema(loadTable(\"t1\",\"t1\"))'",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_memory_dnname_null() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10000, 1,
                    5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'table file does not exist: /t1.tbl' script: 'schema(loadTable(\"\",\"t1\"))'",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
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
        try {
                mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "t1", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Server response: 'getFileBlocksMeta on path '/test_MultithreadedTableWriter_pt/t1.tbl' failed, reason: path does not exist' script: 'schema(loadTable(\"dfs://test_MultithreadedTableWriter_pt\",\"t1\"))'",e.getMessage());
        }
    }

    @Test
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
        try {
                mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "s", "pt", false, false, null, 10000, 1,
                5, "tradeDate");
        }catch (Exception e) {
            assertEquals("Server response: 'table file does not exist: s/pt.tbl' script: 'schema(loadTable(\"s\",\"pt\"))'",e.getMessage());
        }
    }

    @Test
    public void test_MultithreadedTableWriter_nopermission() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        conn.run("try{rpc(getControllerAlias(),deleteUser,`EliManning);}catch(ex){} \n" +
                "rpc(getControllerAlias(),createUser,`EliManning, \"123456\");" +
                "rpc(getControllerAlias(),grant,`EliManning, TABLE_READ, \"dfs://test_MultithreadedTableWriter/pt\")");
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "EliManning", "123456",
                    dbName, "pt", false, false, null, 10000, 1,
                    5, "tradeDate");
        }catch (Exception ex){
            conn.run("deleteUser(`EliManning)\n");
            assertEquals("Server response: '<NoPrivilege>Not granted to write table dfs://test_MultithreadedTableWriter_pt/pt' script: 'schema(loadTable(\"dfs://test_MultithreadedTableWriter\",\"pt\"))'",ex.getMessage());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
    }



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
           assertEquals(ex.getMessage(),"The number of elements in parameter compressMethods does not match the column size 3");
        }
        conn.run("undef(`t1,SHARED)");
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
            assertEquals(ex.getMessage(),"The number of elements in parameter compressMethods does not match the column size 3");
        }
        conn.run("undef(`t1,SHARED)");

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
        conn.run("undef(`t1,SHARED)");

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
        mutithreadTableWriter_.insertUnwrittenData(tb, pErrorInfo);
        Thread.sleep(5000);
        bt = (BasicTable) conn.run("select * from t1;");
        assertEquals("[0,1,2,3,4]", bt.getColumn(2).getString());
        assertEquals(5, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");

    }

    @Test
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
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, -10000, 1,
                5, "tradeDate");
        }catch (Exception e){
            assertEquals("The parameter batchSize must be greater than or equal to 1.",e.getMessage());
        }
    }

    @Test//(expected = Exception.class)
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
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 0, 1,
                    5, "tradeDate");
        }catch (Exception e){
            assertEquals("The parameter batchSize must be greater than or equal to 1.",e.getMessage());
        }
    }

    @Test//(expected = Exception.class)
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
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, -10,
                5, "tradeDate");
        }catch (Exception e){
            assertEquals("The parameter throttle must be greater than or equal to 0.",e.getMessage());
        }
    }


    @Test//(expected = Exception.class)
    public void test_MultithreadedTableWriter_threadcount_0() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10000, 1,
                    0, "date");
        }catch (Exception e){
            assertEquals("The parameter threadCount must be greater than or equal to 1.",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test//(expected = Exception.class)
    public void test_MultithreadedTableWriter_threadcount_Negtive() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10000, 1,
                    -10, "date");
        }catch (Exception e){
            assertEquals("The parameter threadCount must be greater than or equal to 1.",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test//(expected = Exception.class)
    public void test_MultithreadedTableWriter_partitioncol_null() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10000, 1,
                    10, "");
        }catch (Exception e){
            assertEquals("The parameter partitionCol must be specified when threadCount is greater than 1.",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test//(expected = Exception.class)
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
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 10, 1,
                    5, "Date");
        }catch (Exception e){
            assertEquals("The parameter partionCol must be the partitioning column tradeDate in the table.",e.getMessage());
        }
    }

    @Test
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
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                5, "vwap");
        }catch (Exception e){
            assertEquals("The parameter partionCol must be the partitioning column tradeDate in the table.",e.getMessage());
        }
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
        mutithreadTableWriter_.insertUnwrittenData(tb, pErrorInfo);
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
        mutithreadTableWriter_.insertUnwrittenData(tb, pErrorInfo);
        Thread.sleep(1000);
        bt = (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.getColumn("id").getString());
        System.out.println(bt.getColumn("values").getString());
        assertEquals(8, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");

    }


    @Test
    public void test_insert_allnull() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`lo`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`int`arrv`blob," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,INT[],BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");

        Boolean b = mutithreadTableWriter_.insert(pErrorInfo, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        mutithreadTableWriter_.insert(pErrorInfo, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        mutithreadTableWriter_.insert(pErrorInfo, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        conn.run("undef(`t1,SHARED)");

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
        conn.run("undef(`t1,SHARED)");
    }

    /**
     * failed operations
     */
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
        assertEquals("Column counts don't match.",pErrorInfo.errorInfo);
       // assertEquals(1,pErrorInfo.errorCode);
        b = mutithreadTableWriter_.insert(pErrorInfo, 1, 1, 1);
        assertEquals(false, b);
        assertEquals("Column counts don't match.",pErrorInfo.errorInfo);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        List<List<Entity>> unwrite = mutithreadTableWriter_.getUnwrittenData();
        assertEquals(0,unwrite.size());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
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
        assertEquals("Invalid object error java.lang.RuntimeException: Failed to insert data. Cannot convert String to DT_DATE.",pErrorInfo.errorInfo);
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();
        
     //   assertEquals(1,status.errorCode);
        assertEquals("",status.errorInfo.toString());
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");

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
        List<List<Entity>> unwrite = mutithreadTableWriter_.getUnwrittenData();
        assertEquals(15, unwrite.size());
        for (int i = 0; i < 15; i++) {
            assertEquals(String.valueOf(i), unwrite.get(i).get(0).getString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
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
        mutithreadTableWriter1.insertUnwrittenData(tb1, pErrorInfo);
        mutithreadTableWriter2.insertUnwrittenData(tb2, pErrorInfo);
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter1.waitForThreadCompletion();

        List<List<Entity>> unwrite1 = mutithreadTableWriter1.getUnwrittenData();
        List<List<Entity>> unwrite2 =  mutithreadTableWriter2.getUnwrittenData();

        assertTrue(unwrite1.size() == 0);
        assertTrue(unwrite2.size() >0);
        conn.run("undef(`t1,SHARED)");

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
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();
        
        assertFalse(status.isExiting);
        assertEquals(0, status.sendFailedRows);
        assertEquals(null, status.errorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        assertTrue(status.isExiting);
        System.out.println(status.threadStatusList.get(0).toString());
        conn.run("undef(`t1,SHARED)");
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
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();
        
        assertFalse(status.isExiting);
        assertEquals(0, status.sendFailedRows);
        assertEquals(15, status.unsentRows);
        assertEquals(0, status.sentRows);
        mutithreadTableWriter_.waitForThreadCompletion();
        
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        assertTrue(status.isExiting);
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_getStatus_insert_invalidParameter() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "date");
        boolean b=mutithreadTableWriter_.insert(pErrorInfo, 1);
        assertFalse(b);
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        assertEquals(0, status.sentRows);
        assertEquals("code=A2 info=Column counts don't match.", status.errorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
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
                mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
                mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
            }
        } catch (Exception ex) {
        }
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();


        MultithreadedTableWriter.Status status =mutithreadTableWriter1.getStatus();
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt)");
        assertEquals(bt.rows(), status.sentRows);

        MultithreadedTableWriter.Status status1 =  mutithreadTableWriter2.getStatus();
        assertTrue(status1.sendFailedRows>0);
        assertEquals(10000-status1.sendFailedRows, status1.unsentRows);
        assertEquals(0, status1.sentRows);
        assertEquals("", status.errorInfo.toString());

        conn.run("undef(`t1,SHARED)");

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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_blob_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 1, 1,
                    1, "int", new int[]{Vector.COMPRESS_DELTA, Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_BLOB",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_bool_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`bool," +
                "[INT,BOOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
                mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_BOOL",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_char_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`char," +
                "[INT,CHAR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_BYTE",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_float_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,FLOAT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_FLOAT",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_double_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_DOUBLE",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_symbol_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,SYMBOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_SYMBOL",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_string_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,STRING]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_STRING",e.getMessage());
        }
//        for (int i=0;i<5;i++) {
//            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1, "fd");
//            assertEquals(true, b);
//        }
//        mutithreadTableWriter_.waitForThreadCompletion();
//        BasicTable bt= (BasicTable) conn.run("select * from t1;");
//        assertEquals(5,bt.rows());
//        for (int i=0;i<5;i++) {
//            assertEquals("fd", bt.getColumn("delta").get(i).getString());
//        }
        conn.run("undef(`t1,SHARED)");
    }


    @Test
    public  void test_insert_UUID_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,UUID]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_UUID",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_IPADDR_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,IPADDR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_IPADDR",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public  void test_insert_INT128_delta()throws Exception {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,INT128]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            assertEquals("Compression Failed: only support integral and temporal data, not support DT_INT128",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    /**
     * lz4 compress test
     * @throws Exception
     */

    @Test
    public void test_insert_bool() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`id," +
                "[BOOL,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "bool");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, true, 1);
        mutithreadTableWriter_.insert(pErrorInfo, false, 1);
        mutithreadTableWriter_.insert(pErrorInfo, null, 1);

        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("true", bt.getColumn("bool").get(0).getString());
        assertEquals("false", bt.getColumn("bool").get(1).getString());
        assertEquals("", bt.getColumn("bool").get(2).getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_insert_byte() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, (byte)1, (byte)1, (byte)1, (byte)1, (byte)1);
        b = mutithreadTableWriter_.insert(pErrorInfo, null, (byte)1, (byte)1, (byte)1, (byte)1);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("1", bt.getColumn("char").get(0).getString());
        assertEquals("", bt.getColumn("char").get(1).getString());
        assertEquals("1", bt.getColumn("int").get(0).getString());
        assertEquals("1", bt.getColumn("long").get(0).getString());
        assertEquals("1", bt.getColumn("short").get(0).getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_insert_short() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, (short)1, (short)1, (short)-1, (short)0, (short)-1);
        b = mutithreadTableWriter_.insert(pErrorInfo, null, (short)1, (short)1, (short)0, (short)1);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_insert_int() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, (int)1, (int)1, (int)-1, (int)0, (int)-1);
        b = mutithreadTableWriter_.insert(pErrorInfo, null, (int)1, (int)1, (int)0, (int)1);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_insert_long() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, (long)1, (long)1, (long)-1, (long)0, (long)-1);
        b = mutithreadTableWriter_.insert(pErrorInfo, null, (long)1, (long)1, (long)0, (long)1);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }


    @Test
    public void test_insert_float() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `float`double`id," +
                "[FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo,1.9f,0.2f,2);
        b = mutithreadTableWriter_.insert(pErrorInfo, -1.90f,-0.2f,2);
        b = mutithreadTableWriter_.insert(pErrorInfo, null,null,2);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("[1.9,-1.9,]", bt.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", bt.getColumn("double").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_insert_double() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `float`double`id," +
                "[FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "id");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo,1.9,0.2,2);
        b = mutithreadTableWriter_.insert(pErrorInfo, -1.90,-0.2,2);
        b = mutithreadTableWriter_.insert(pErrorInfo, null,null,2);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("[1.9,-1.9,]", bt.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", bt.getColumn("double").getString());
        conn.run("undef(`t1,SHARED)");
    }


    @Test
    public void test_insert_string() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `symbol`string`uuid`ipaddr`int128`blob`id," +
                "[SYMBOL,STRING,UUID,IPADDR,INT128,BLOB,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 1, 1,
                1, "symbol");
        boolean b = mutithreadTableWriter_.insert(pErrorInfo,"QQ","qq","5d212a78-cc48-e3b1-4235-b4d91473ee87","192.168.1.13","e1671797c52e15f763380b45e841ec32","dsfgv",1);
        b = mutithreadTableWriter_.insert(pErrorInfo, null, null, null, null, null, null,2);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[QQ,]", bt.getColumn("symbol").getString());
        assertEquals("[qq,]", bt.getColumn("string").getString());
        assertEquals("[5d212a78-cc48-e3b1-4235-b4d91473ee87,00000000-0000-0000-0000-000000000000]", bt.getColumn("uuid").getString());
        assertEquals("[192.168.1.13,0.0.0.0]", bt.getColumn("ipaddr").getString());
        assertEquals("[e1671797c52e15f763380b45e841ec32,00000000000000000000000000000000]", bt.getColumn("int128").getString());
        assertEquals("[dsfgv,]", bt.getColumn("blob").getString());
        conn.run("undef(`t1,SHARED)");
    }

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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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

    /**
     *  array vector test
     * * @throws Exception
     */

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
        for (int i=0;i<1000;i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, 1,null);
            assertEquals(true, b);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(2000,bt.rows());
        for (int i=0;i<2000;i++) {
            assertEquals("[]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
        conn.run("undef(`t1,SHARED)");

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
        conn.run("undef(`t1,SHARED)");
    }

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
        conn.run("undef(`t1,SHARED)");
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
        conn.run("undef(`t1,SHARED)");
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
        boolean b=mutithreadTableWriter_.insertUnwrittenData(tb, pErrorInfo);
        assertEquals(true, b);
        for (int i=0;i<2;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicString("1"));
            tb.add(row);
        }
        b=mutithreadTableWriter_.insertUnwrittenData(tb, pErrorInfo);
        assertEquals(true, b);
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        List<List<Entity>> le=mutithreadTableWriter_.getUnwrittenData();
        MultithreadedTableWriter.Status status= mutithreadTableWriter_.getStatus();
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



    @Test
    public void test_insert_BasicType_in_java() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script="t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share t as t1;" +
                "tt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,\n" +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share tt as t2;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false, null, 10, 1,
                1, "short");

        Month mon=LocalDate.of(2022,2,2).getMonth();
        for (int i = 0; i < 10000; i++) {
            boolean b=mutithreadTableWriter_.insert(pErrorInfo,new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),new BasicFloat(2.312f),new BasicDouble(3.2),
                    new BasicString("sedf"+i),new BasicString("sedf"),new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),
                    new BasicInt(21),new BasicString("d"+i,true));
            assertTrue(b);
            List<Entity> args = Arrays.asList(new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicFloat(2.312f),new BasicDouble(3.2),new BasicString("sedf"+i),new BasicString("sedf"),
                    new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),new BasicInt(21),
                    new BasicString("d"+i,true));
            conn.run("tableInsert{t2}", args);
        }
        Thread.sleep(2000);
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by symbol");
        BasicTable  res= (BasicTable) conn.run("select * from t2 order by symbol");
        assertEquals(10000,ex.rows());
        checkData(ex,res);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    /**
     * table types
     * @throws Exception
     */


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

    @Test//(expected = RuntimeException.class)
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
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 10, 1,
                    10, "tradeDate");
        }catch (Exception ex){
            assertEquals("The parameter threadCount must be 1 for a dimension table.",ex.getMessage());
        }
        //mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test//(expected = RuntimeException.class)
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
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 10, 1,
                    10, "tradeDate");
        }catch (Exception ex){
            assertEquals("The parameter threadCount must be 1 for a dimension table.",ex.getMessage());
        }
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

    /**
     * test dfs partitionType
     * @throws Exception
     */
    @Test
    public void test_insert_dfs_value() throws Exception {
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
                2, "sym");
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
                1, "tradeDate");
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
    public void test_insert_dfs_list() throws Exception {
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, LIST, [`IBM`ORCL`MSFT, `GOOG`FB])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "sym");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "IBM", LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("IBM"));
            row.add(new BasicDateTime(LocalDateTime.of(2022, (i % 10) + 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
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
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_datetime() throws Exception {
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
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicDateTime(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_timestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, datehour(2022.01.01)+0..3)\n" +
                "\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicTimestamp(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_nanotimestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", VALUE, datehour(2022.01.01)+0..3)\n" +
                "\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
                "\tdb=database(dbName, COMPO, [db1, db2], , \"OLAP\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"sym\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                10, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_datetime() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01+0..1)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicDateTime(LocalDateTime.of(2022,2,2,1,1,1)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_timestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01+0..1)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicTimestamp(LocalDateTime.of(2022,2,2,1,1,1,1)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_nanotimestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, 2012.01.01+0..1)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022,2,2,1,1,1,1)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_DATE_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, RANGE,  [2022.01.01, 2022.02.01,  2022.03.01],)\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDate.of(2022,2,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicDate(LocalDate.of(2022,2,2)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_datetime_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, RANGE,  [2022.01.01, 2022.02.01,  2022.03.01])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,3,3,3), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicDateTime( LocalDateTime.of(2022,2,2,3,3,3)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }
    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_timestamp_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName,RANGE,  [2022.01.01, 2022.02.01,  2022.03.01])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicTimestamp(LocalDateTime.of(2022,2,2,1,1,1,1)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_nanotimestamp_range() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName,RANGE,  [2022.01.01, 2022.02.01,  2022.03.01])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022,2,2,1,1,1,1)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_datetime() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, month(1..2))\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicDateTime(LocalDateTime.of(2022,2,2,12,2)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_timestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, month(1..2))\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicTimestamp(LocalDateTime.of(2022,2,2,12,2)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }


    @Test
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_Nanotimestamp() throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();

        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, month(1..2))\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                20, "tradeDate");
        Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            boolean b = mutithreadTableWriter_.insert(pErrorInfo, "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals(true, b);
        }
        for (int i = 0; i < insertTime; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2" + i % 100));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022,2,2,12,2)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt((i % 10) + 1));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
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
            mutithreadTableWriter1.insertUnwrittenData(tb1, pErrorInfo);
            mutithreadTableWriter2.insertUnwrittenData(tb2, pErrorInfo);
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
            mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter3.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter4.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter5.insertUnwrittenData(tb, pErrorInfo);
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
            mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter3.insertUnwrittenData(tb, pErrorInfo);
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
            mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter3.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter4.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter5.insertUnwrittenData(tb, pErrorInfo);
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
        MultithreadedTableWriter.Status status =  mutithreadTableWriter1.getStatus();
        assertFalse(status.isExiting);
        mutithreadTableWriter1.waitForThreadCompletion();
        status =  mutithreadTableWriter1.getStatus();
        assertTrue(status.isExiting);
        status =  mutithreadTableWriter2.getStatus();
        assertFalse(status.isExiting);
        mutithreadTableWriter2.waitForThreadCompletion();
        status =  mutithreadTableWriter2.getStatus();
        assertTrue(status.isExiting);
        status =  mutithreadTableWriter3.getStatus();
        assertFalse(status.isExiting);
        mutithreadTableWriter3.waitForThreadCompletion();
        status =  mutithreadTableWriter3.getStatus();
        assertTrue(status.isExiting);
        status =  mutithreadTableWriter4.getStatus();
        assertFalse(status.isExiting);
        mutithreadTableWriter4.waitForThreadCompletion();
        status =  mutithreadTableWriter4.getStatus();
        assertTrue(status.isExiting);
        status =  mutithreadTableWriter5.getStatus();
        assertFalse(status.isExiting);
        mutithreadTableWriter5.waitForThreadCompletion();
        status =  mutithreadTableWriter5.getStatus();
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
            mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter3.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter4.insertUnwrittenData(tb, pErrorInfo);
            mutithreadTableWriter5.insertUnwrittenData(tb, pErrorInfo);
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
        mutithreadTableWriter1.insertUnwrittenData(tb, pErrorInfo);
        mutithreadTableWriter2.insertUnwrittenData(tb, pErrorInfo);
        mutithreadTableWriter3.insertUnwrittenData(tb, pErrorInfo);
        mutithreadTableWriter4.insertUnwrittenData(tb, pErrorInfo);
        mutithreadTableWriter5.insertUnwrittenData(tb, pErrorInfo);
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

    /**
     * test streamtable
     * @throws Exception
     */

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

