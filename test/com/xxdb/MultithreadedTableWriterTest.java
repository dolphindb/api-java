package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.Callback;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.route.AutoFitTableAppender;
import com.xxdb.route.AutoFitTableUpsert;
import com.xxdb.route.PartitionedTableAppender;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Logger;

import static com.xxdb.Prepare.PrepareUser_authMode;
import static com.xxdb.Prepare.checkData;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


public  class MultithreadedTableWriterTest implements Runnable {
    private Logger logger_ = Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String CONTROLLER_HOST = bundle.getString("CONTROLLER_HOST");
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");

    public static Integer insertTime = 5000;
    public static ErrorCodeInfo pErrorInfo =new ErrorCodeInfo();;

    //private final int id;
    private static MultithreadedTableWriter mutithreadTableWriter_ = null;

    public MultithreadedTableWriterTest() {
    }
    @BeforeClass
    public static void prepare1 () throws IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        controller_conn.run("try{startDataNode('" + HOST + ":" + PORT + "')}catch(ex){}");
        controller_conn.run("sleep(8000)");
    }
    @Before
    public void prepare() throws IOException {
        conn = new DBConnection(false,false,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
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
        conn.run("try{undef(`ext1,SHARED)}catch(ex){}");
        conn.close();
    }

    @After
    public void close() throws IOException {
//        dropAllDB();
        String script = "obj =  exec name from objs(true) where shared=true;\n" +
                "for(s in obj)\n" +
                "{\n" +
                "undef (s, SHARED);\n" +
                "}";
        try{
            conn.run(script);
        }catch(Exception E){
            System.out.println(E.getMessage());
        }
        conn.close();
    }

    @Override
    public void run() {

        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
    }

    /**
     * Parameter check
     * @throws Exception
     */
    Callback callbackHandler = new Callback(){
        public void writeCompletion(Table callbackTable){
            List<String> failedIdList = new ArrayList<>();
            BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
            BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
            for (int i = 0; i < successV.rows(); i++){
                if (!successV.getBoolean(i)){
                    failedIdList.add(idV.getString(i));
                }
            }
        }
    };
    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_host_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        try{
              mutithreadTableWriter_ = new MultithreadedTableWriter("192.178.1.321", PORT, "admin", "123456",
                "", "t1", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Failed to connect to server 192.178.1.321:"+PORT,e.getMessage());
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
                "", "t1", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception e) {
            assertEquals("Failed to connect to server "+HOST+":0",e.getMessage());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(expected = IOException.class)
    public void test_MultithreadedTableWriter_userid_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "s", "123456",
                "", "t1", false, false, null, 10000, 1,
                5, "date");
        conn.run("undef(`t1,SHARED)");
    }

    @Test(expected = IOException.class)
    public void test_MultithreadedTableWriter_pwd_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "12356",
                "", "t1", false, false, null, 10000, 1,
                5, "date");
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_MultithreadedTableWriter_reconnect_false() throws IOException {
        int port=7102;
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, false, null, 10000, 1,
                    5, "date",null,false,false,0);
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("Failed to connect to server "));
    }

    @Test
    public void test_MultithreadedTableWriter_reconnect_false_tryReconnectNums_not_set() throws IOException {
        int port=7102;
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, false, null, 10000, 1,
                    5, "date",null,false,false);
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("Failed to connect to server "));
    }

    @Test
    public void test_MultithreadedTableWriter_reconnect_false_tryReconnectNums_1() throws Exception {
        int port=7102;
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, false, null, 10000, 1,
                    5, "date",null,false,false,1);
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("Failed to connect to server "));
    }

    @Test
    public void test_MultithreadedTableWriter_reconnect_true() throws IOException {
        int port=7102;
        int trynums=3;
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, false, null, 10000, 1,
                    5, "date",null,false,true,trynums);
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals("Connect to "+HOST+":"+port+" failed after "+trynums+" reconnect attempts.",re);
    }

    //@Test 会一直重连 ，故回归注销
    public void test_MultithreadedTableWriter_reconnect_true_tryReconnectNums_not_set() throws IOException {
        int port=7102;
        int trynums=3;
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, false, null, 10000, 1,
                    5, "date",null,false,true);
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals("Connect to "+HOST+":"+port+" failed after "+trynums+" reconnect attempts.",re);
    }

    //@Test  会一直重连 ，故回归注销
    public void test_MultithreadedTableWriter_tryReconnectNums_0_reconnect_true() throws Exception {
        int port=7102;
        int trynums=0;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                "", "tt", false, false, null, 10000, 1,
                5, "date",null,false,true,trynums);
    }

    //@Test tryReconnectNums设置为-1时 会无限重连
    public void test_MultithreadedTableWriter_tryReconnectNums_negetive_reconnect_true() throws Exception {
        int port=7102;
        int trynums=-1;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                "", "tt", false, true, null, 10000, 1,
                5, "date",null,false,false,trynums);
    }

    @Test
    public void test_MultithreadedTableWriter_tryReconnectNums_enableHighAvailability_true_reconnect_false() throws Exception {
        class LogCapture {
            private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            private final PrintStream originalErr = System.err;
            public void start() {
                System.setErr(new PrintStream(baos));
            }
            public void stop() {
                System.setErr(originalErr);
            }
            public String getLogMessages() {
                return baos.toString();
            }
        }
        int port=7102;
        int trynums=3;
        LogCapture logCapture = new LogCapture();
        logCapture.start();
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, true, new String[]{"192.168.0.69:7002"}, 10000, 1,
                    5, "date",null,true,false,trynums);
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        logCapture.stop();
        String s=logCapture.getLogMessages();
        assertTrue(s.contains("Connect failed after "+trynums+" reconnect attempts for every node in high availability sites."));
    }

    @Test
    public void test_MultithreadedTableWriter_tryReconnectNums_enableHighAvailability_true_reconnect_false_1() throws Exception {
        class LogCapture {
            private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            private final PrintStream originalErr = System.err;
            public void start() {
                System.setErr(new PrintStream(baos));
            }
            public void stop() {
                System.setErr(originalErr);
            }
            public String getLogMessages() {
                return baos.toString();
            }
        }
        int port=7102;
        int trynums=3;
        LogCapture logCapture = new LogCapture();
        logCapture.start();
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, true, new String[]{"192.168.0.69:7002","192.168.0.69:7222"}, 10000, 1,
                    5, "date",null,true,false,trynums);
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        logCapture.stop();
        String s=logCapture.getLogMessages();
        assertTrue(s.contains("Connect failed after "+trynums+" reconnect attempts for every node in high availability sites."));
    }

    @Test
    public void test_MultithreadedTableWriter_tryReconnectNums_enableHighAvailability_true_reconnect_true() throws Exception {
        class LogCapture {
            private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            private final PrintStream originalErr = System.err;
            public void start() {
                System.setErr(new PrintStream(baos));
            }
            public void stop() {
                System.setErr(originalErr);
            }
            public String getLogMessages() {
                return baos.toString();
            }
        }
        int port=7102;
        int trynums=3;
        String re = null;
        String[] N={"localhost:7300"};
        DBConnection conn =new DBConnection();
        LogCapture logCapture = new LogCapture();
        logCapture.start();
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, port, "admin", "123456",
                    "", "tt", false, true, new String[]{"192.168.0.69:7002"}, 10000, 1,
                    5, "date",null,true,true,trynums);
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        logCapture.stop();
        String s=logCapture.getLogMessages();
        assertTrue(s.contains("Connect failed after "+trynums+" reconnect attempts for every node in high availability sites."));
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_memory_dbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "tt", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("Syntax Error: [line #1] Cannot recognize the token tt"));

        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_memory_tbname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "t1", false, false, null, 10000, 1,
                5, "date");
        }catch (Exception ex) {
            re = ex.getMessage();
        }
        assertTrue(re.contains("table file does not exist: t1/t1.tbl"));
        conn.run("undef(`t1,SHARED)");
    }

    @Test(expected = IOException.class)
    public void test_MultithreadedTableWriter_memory_dnname_null() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "t1", "", false, false, null, 10000, 1,
                    5, "date");
        conn.run("undef(`t1,SHARED)");
    }

    @Test(expected = IOException.class)
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
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
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
            //assertEquals(HOST+":"+PORT+" Server response: 'table file does not exist: s/pt.tbl' script: 'schema(loadTable(\"s\",\"pt\"))'",e.getMessage());
            assertTrue(e.getMessage().contains("table file does not exist: s/pt.tbl"));
        }
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_nopermission() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter";

        sb.append("if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate, [SYMBOL, DATE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])");
        conn.run(sb.toString());
        conn.run("try{rpc(getControllerAlias(),deleteUser,`EliManning);}catch(ex){} \n" +
                "rpc(getControllerAlias(),createUser,`EliManning, \"123456\");" +
                "rpc(getControllerAlias(),grant,`EliManning, TABLE_READ, \"dfs://test_MultithreadedTableWriter/pt\")");
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "EliManning", "123456",
                dbName, "pt", false, false, null, 10000, 1,
                5, "tradeDate");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicString("12"), new BasicDate(15340));
        mutithreadTableWriter_.waitForThreadCompletion();
        //<NoPrivilege>Not granted to write data to table dfs://test_MultithreadedTableWriter/pt. function: tableInsert{loadTable("dfs://test_MultithreadedTableWriter","pt")}
        BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt)");
        Assert.assertEquals(0,re.rows());
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_compress_lessthan_cols() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10, 2,
                    5, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }
        catch (RuntimeException ex){
           assertEquals(ex.getMessage(),"The number of elements in parameter compressMethods does not match the column size 3");
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_compress_morethan_cols() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10, 2,
                    5, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }
        catch (RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals(re, "The number of elements in parameter compressMethods does not match the column size 3");
        conn.run("undef(`t1,SHARED)");
    }
    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_colType_not_match() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`idDFSDF121212中文`values_中文,[TIMESTAMP,INT,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 10,2,5, "date");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( System.currentTimeMillis(), "A", "A");
        assertEquals("code=A1 info=Invalid object error when create scalar for column 'idDFSDF121212中文': Failed to insert data. Cannot convert String to DT_INT.",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_throttle() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 10,2,5, "date");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( System.currentTimeMillis(), "A", 1);
        assertEquals("code= info=",pErrorInfo.toString());
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        Thread.sleep(4000);
        bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_batchsize() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 3, 3000,
                1, "date");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(System.currentTimeMillis(), "A", 0);
        mutithreadTableWriter_.insert(System.currentTimeMillis(), "A", 1);
        assertEquals("code= info=", pErrorInfo.toString());
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
        mutithreadTableWriter_.insertUnwrittenData(tb);
        Thread.sleep(5000);
        bt = (BasicTable) conn.run("select * from t1;");
        assertEquals("[0,1,2,3,4]", bt.getColumn(2).getString());
        assertEquals(5, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
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
        String re = null;
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, -10000, 1,
                5, "tradeDate");
        }catch (Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The parameter batchSize must be greater than or equal to 1.",re);
    }

    @Test(timeout = 120000)
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
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 0, 1,
                    5, "tradeDate");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter batchSize must be greater than or equal to 1.",re);

    }

    @Test(timeout = 120000)
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
        String re = null;
        try {
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, -10,
                5, "tradeDate");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter throttle must be greater than or equal to 0.",re);
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_threadcount_0() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10000, 1,
                    0, "date");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter threadCount must be greater than or equal to 1.",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_threadcount_Negtive() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10000, 1,
                    -10, "date");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter threadCount must be greater than or equal to 1.",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_partitioncol_null() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 10000, 1,
                    10, "");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter partitionCol must be specified when threadCount is greater than 1.",re);

        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_partcolname_wrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        String dbName = "dfs://test_MultithreadedTableWriter_pt";

        String script = "if(existsDatabase('" + dbName + "')){\n" +
                "\t\tdropDatabase('" + dbName + "')\n" +
                "\t}\n" +
                "\tdb=database('" + dbName + "', VALUE, 2012.01.01..2012.01.30)\n" +
                "t=table(1:0, `sym`tradeDate`tradeTime`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, TIME, DOUBLE, DOUBLE, INT, DOUBLE])\n" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])";
        conn.run(script);
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    dbName, "pt", false, false, null, 10, 1,
                    5, "Date");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter partionCol must be the partitioning column tradeDate in the table.",re);
    }

    @Test(timeout = 120000)
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
        String re = null;
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                5, "vwap");
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("The parameter partionCol must be the partitioning column tradeDate in the table.",re);
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_threadcount() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 4, 2,
                2, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( System.currentTimeMillis(), "A", 0);
        mutithreadTableWriter_.insert( System.currentTimeMillis(), "B", 0);
        assertEquals("code= info=",pErrorInfo.toString());
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
        mutithreadTableWriter_.insertUnwrittenData(tb);
        Thread.sleep(500);
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
        mutithreadTableWriter_.insertUnwrittenData(tb);
        Thread.sleep(500);
        bt = (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.getColumn("id").getString());
        System.out.println(bt.getColumn("values").getString());
        assertEquals(8, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");

    }


    @Test(timeout = 120000)
    public void test_insert_allnull() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`lo`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`int`arrv`blob," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,INT[],BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool");

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        mutithreadTableWriter_.insert( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        mutithreadTableWriter_.insert( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_partnull() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            System.out.println(bt.getColumn(i).get(0).toString());
        }
        conn.run("undef(`t1,SHARED)");
    }

    /**
     * ErrorCodeInfo test
     */
    @Test(timeout = 120000)
    public void test_insert_cols_diffwith_table() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "date");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1);
        assertEquals("Column counts don't match.",pErrorInfo.getErrorInfo());
        assertEquals("A2",pErrorInfo.getErrorCode());
        assertEquals(true,pErrorInfo.hasError());
        assertEquals(false,pErrorInfo.succeed());
        pErrorInfo=mutithreadTableWriter_.insert( 1, 1, 1);
        assertEquals("Column counts don't match.",pErrorInfo.getErrorInfo());
        assertEquals("A2",pErrorInfo.getErrorCode());
        assertEquals(true,pErrorInfo.hasError());
        assertEquals(false,pErrorInfo.succeed());
        pErrorInfo.clearError();
        assertEquals("",pErrorInfo.getErrorInfo());
        assertEquals("",pErrorInfo.getErrorCode());
        assertEquals(false,pErrorInfo.hasError());
        assertEquals(true,pErrorInfo.succeed());
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        List<List<Entity>> unwrite = mutithreadTableWriter_.getUnwrittenData();
        assertEquals(0,unwrite.size());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_type_diffwith_table() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "date");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, "2012.01.02");
        assertEquals("Invalid object error when create scalar for column 'date': Failed to insert data. Cannot convert String to DT_DATE.",pErrorInfo.getErrorInfo());
        assertEquals("A1",pErrorInfo.getErrorCode());
        assertEquals(true,pErrorInfo.hasError());
        assertEquals(false,pErrorInfo.succeed());
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(0, bt.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    public void writeData(int num, List<List<Entity>> tb) {
        List<Entity> row = new ArrayList<>();
        row.add(new BasicInt(num));
        row.add(new BasicDouble(5));
        for (int i = 0; i < 10000; i++) {
            tb.add(row);
        }
    }

    /**
     * Status
     */
    @Test(timeout = 120000)
    public void test_getStatus_write_successful() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert( i, new Date());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        assertEquals(false,status.hasError());
        assertTrue(status.isExiting);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_getStatus_insert_successful_normalData() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", true, false, null, 100000, 1000,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert( i, new Date());
        }
        MultithreadedTableWriter.Status status =  mutithreadTableWriter_.getStatus();

        assertFalse(status.isExiting);
        assertEquals(0, status.sendFailedRows);
        assertEquals(15, status.unsentRows);
        assertEquals(0, status.sentRows);
        assertEquals(false,status.hasError());
        mutithreadTableWriter_.waitForThreadCompletion();

        status =  mutithreadTableWriter_.getStatus();
        assertTrue(status.isExiting);
        assertEquals(0, status.unsentRows);
        assertEquals(15, status.sentRows);
        assertEquals(false,status.hasError());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_getStatus_insert_invalidParameter() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "date");
        ErrorCodeInfo pErrorInfo=mutithreadTableWriter_.insert( 1);
        assertEquals("code=A2 info=Column counts don't match.",pErrorInfo.toString());
        MultithreadedTableWriter.Status status =mutithreadTableWriter_.getStatus();
        assertEquals("",status.getErrorInfo());
        assertEquals("",status.getErrorCode());
        assertEquals(0, status.sendFailedRows);
        assertEquals(0, status.unsentRows);
        assertEquals(0, status.sentRows);
        assertEquals(false, status.hasError());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_getStatus_write_failed() throws Exception {
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
                    1, "volume");
            mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                    1, "volume");
            List<List<Entity>> tb = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                List<Entity> row = new ArrayList<>();
                row.add(new BasicInt(1));
                row.add(new BasicDouble(5));
                tb.add(row);
            }
            for (int i = 0; i < 1; i++) {
                mutithreadTableWriter1.insertUnwrittenData(tb);
                mutithreadTableWriter2.insertUnwrittenData(tb);
            }
        } catch (Exception ex) {
        }
        Thread.sleep(10000);
        MultithreadedTableWriter.Status status =mutithreadTableWriter1.getStatus();
        MultithreadedTableWriter.Status status1 =  mutithreadTableWriter2.getStatus();
        if (status.getErrorInfo().toString().contains(HOST+":"+PORT+" Server response: '<ChunkInTransaction>filepath '/test_MultithreadedTableWriter")){
            assertEquals("",status1.getErrorInfo().toString());
            assertEquals("A5",status.getErrorCode());
            assertTrue(status.sendFailedRows >0);
            assertEquals(true,status.hasError());
            assertEquals(false,status1.hasError());
            assertEquals(false,status.succeed());
            assertEquals(true,status1.succeed());
            assertEquals(true,status.isExiting);
            assertEquals(false,status1.isExiting);
            assertEquals(1000,status.unsentRows+status.sendFailedRows+status.sentRows);
            assertEquals(1000,status1.unsentRows+status.sendFailedRows+status.sentRows);

        }else {
            //assertTrue(status1.errorInfo.toString().contains(HOST+":"+PORT+" Server response: '<ChunkInTransaction>filepath '/test_MultithreadedTableWriter"));
            assertEquals("A5",status1.getErrorCode());
            assertTrue(status1.sendFailedRows >0);
            assertEquals(true,status1.hasError());
            assertEquals(false,status.hasError());
            assertEquals(true,status.succeed());
            assertEquals(false,status1.succeed());
            assertEquals(true,status1.isExiting);
            assertEquals(false,status.isExiting);
            assertEquals(1000,status1.unsentRows+status1.sendFailedRows+status1.sentRows);
            assertEquals(1000,status.unsentRows+status.sendFailedRows+status.sentRows);
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_ChunkInTransaction_insert() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("\n" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE,1..6)\n" +
                "t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
                " ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000, 1,
                1, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000, 1,
                1, "volume");
        for (int i = 0; i < 1000; i++) {
            mutithreadTableWriter1.insert( 1, 1.3);
            mutithreadTableWriter2.insert( 1, 1.3);
        }
        Thread.sleep(10000);
        MultithreadedTableWriter.Status status1=mutithreadTableWriter1.getStatus();
        MultithreadedTableWriter.Status status=mutithreadTableWriter2.getStatus();
        if (status.getErrorInfo().toString().contains(HOST+":"+PORT+" Server response: <ChunkInTransaction>")){
            assertEquals("",status1.getErrorInfo().toString());
            assertEquals("A5",status.getErrorCode());
            assertTrue(status.sendFailedRows >0);
            assertEquals(true,status.hasError());
            assertEquals(false,status1.hasError());
            assertEquals(false,status.succeed());
            assertEquals(true,status1.succeed());
            assertEquals(true,status.isExiting);
            assertEquals(false,status1.isExiting);
            assertEquals(1000,status.unsentRows+status.sendFailedRows+status.sentRows);
            assertEquals(1000,status1.unsentRows+status.sendFailedRows+status.sentRows);

        }else {
            assertTrue(status1.getErrorInfo().toString().contains(HOST+":"+PORT+" Server response: <ChunkInTransaction>The openChunks operation failed because the chunk '/test_MultithreadedTableWriter"));
            assertEquals("A5",status1.getErrorCode());
            assertTrue(status1.sendFailedRows >0);
            assertEquals(true,status1.hasError());
            assertEquals(false,status.hasError());
            assertEquals(true,status.succeed());
            assertEquals(false,status1.succeed());
            assertEquals(true,status1.isExiting);
            assertEquals(false,status.isExiting);
            assertEquals(1000,status1.unsentRows+status.sendFailedRows+status.sentRows);
            assertEquals(1000,status.unsentRows+status.sendFailedRows+status.sentRows);
        }

        BasicTable ex = (BasicTable) conn.run("select * from loadTable(\"dfs://test_MultithreadedTableWriter\",`pt)");
        assertTrue(ex.rows()<2000);
        conn.run("undef(`t1,SHARED)");
    }
    /**
     * getUnwrittenData
     */

    @Test(timeout = 120000)
    public void test_getUnwrittenData_lessThan_throttle_batchsize() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`date,[INT,DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 100000, 10,
                1, "date");
        for (int i = 0; i < 15; i++) {
            mutithreadTableWriter_.insert( i, new Date());
        }
        List<List<Entity>> unwrite = mutithreadTableWriter_.getUnwrittenData();
        assertEquals(15, unwrite.size());
        for (int i = 0; i < 15; i++) {
            assertEquals(String.valueOf(i), unwrite.get(i).get(0).getString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");

    }

    @Test(timeout = 120000)
    public void test_getUnwrittenData_Parallel_to_the_same_partition_insertUnwrittenData() throws Exception {
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
        mutithreadTableWriter1.insertUnwrittenData(tb1);
        mutithreadTableWriter2.insertUnwrittenData(tb2);
        Thread.sleep(10000);
        MultithreadedTableWriter.Status status=mutithreadTableWriter2.getStatus();
        //assertTrue(status.errorInfo.toString().contains(HOST+":"+PORT+" Server response: '<ChunkInTransaction>filepath '/test_MultithreadedTableWriter"));
        assertEquals("A5",status.getErrorCode());
        assertTrue(status.unsentRows >0);
        assertTrue(status.sendFailedRows >0);
        assertEquals(true,status.hasError());
        assertEquals(10000,status.unsentRows+status.sendFailedRows);

        List<List<Entity>> unwrite1 = mutithreadTableWriter1.getUnwrittenData();
        List<List<Entity>> unwrite2 =  mutithreadTableWriter2.getUnwrittenData();

        assertTrue(unwrite1.size() == 0);
        assertTrue(unwrite2.size() >0);
        mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1,
                2, "volume");
        mutithreadTableWriter2.insertUnwrittenData(unwrite2);
        mutithreadTableWriter2.waitForThreadCompletion();
        BasicTable ex = (BasicTable) conn.run("select * from loadTable(\"dfs://test_MultithreadedTableWriter\",`pt)");
        assertEquals(20000,ex.rows());
        conn.run("undef(`t1,SHARED)");

    }


    @Test(timeout = 120000)
    public void test_getUnwrittenData_insertUnwrittenData_sameThread() throws Exception {

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
        mutithreadTableWriter1.insertUnwrittenData(tb1);
        mutithreadTableWriter2.insertUnwrittenData(tb2);
        Thread.sleep(1000);
        List<List<Entity>> unwrite1 = mutithreadTableWriter1.getUnwrittenData();
        assertTrue(unwrite1.size() == 0);
        BasicTable ex1 = (BasicTable) conn.run("select * from loadTable(\"dfs://test_MultithreadedTableWriter\",`pt)");
        List<List<Entity>> unwrite2 =  mutithreadTableWriter2.getUnwrittenData();
        try {
            ErrorCodeInfo errorInfo = mutithreadTableWriter2.insertUnwrittenData(unwrite2);
        }catch (Exception ex){
            assertEquals("Thread is exiting. ",ex.getMessage());
        }
        MultithreadedTableWriter.Status status=mutithreadTableWriter2.getStatus();
        System.out.println(status.getErrorInfo());
        mutithreadTableWriter2.waitForThreadCompletion();
        mutithreadTableWriter1.waitForThreadCompletion();
        BasicTable ex = (BasicTable) conn.run("select * from loadTable(\"dfs://test_MultithreadedTableWriter\",`pt)");
        assertEquals(ex1.rows(),ex.rows());
        conn.run("undef(`t1,SHARED)");
    }


    /**
     *
     *delta compress test
     */

    @Test(expected = RuntimeException.class)
    public  void test_insert_arrayVector_delta()throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`array," +
                "[INT,INT[]]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_DELTA});
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_delta_short_int_long() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`short`long," +
                "[INT,SHORT,LONG]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        for (int i = 0; i < 1048576; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( i, i%32768, i);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1048576, bt.rows());
        for (int i = 0; i < 1048576; i++) {
            assertEquals(i, ((Scalar)bt.getColumn("int").get(i)).getNumber());
            assertEquals((short)(i%32768), ((Scalar)bt.getColumn("short").get(i)).getNumber());
            assertEquals(Long.valueOf(i), ((Scalar)bt.getColumn("long").get(i)).getNumber());

        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_time_delta() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour`nanotime`nanotimestamp," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        LocalDateTime ld = LocalDateTime.of(2022,1,1,1,1,1,10000);

        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,
                Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT, DT, DT, DT , DT, DT,ld,ld);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_blob_delta()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try {
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false, null, 1, 1,
                    1, "int", new int[]{Vector.COMPRESS_DELTA, Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_BLOB",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_bool_delta()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`bool," +
                "[INT,BOOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
                mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_BOOL",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_char_delta()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`char," +
                "[INT,CHAR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_BYTE",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_float_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,FLOAT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_FLOAT",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_double_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_DOUBLE",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_symbol_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,SYMBOL]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_SYMBOL",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_string_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,STRING]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_STRING",re);
        conn.run("undef(`t1,SHARED)");
    }
    @Test(timeout = 120000)
    public  void test_insert_UUID_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,UUID]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_UUID",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_IPADDR_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,IPADDR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_IPADDR",re);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_INT128_delta()throws Exception {
        
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta," +
                "[INT,INT128]);" +
                "share t as t1;");
        conn.run(sb.toString());
        String re = null;
        try{
            mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1,
                    1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        }catch (Exception e){
            re = e.getMessage();
        }
        assertEquals("Compression Failed: only support integral and temporal data, not support DT_INT128",re);
        conn.run("undef(`t1,SHARED)");
    }

    /**
     * lz4 compress test
     * @throws Exception
     */

    @Test(timeout = 120000)
    public void test_insert_bool() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`id," +
                "[BOOL,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( true, 1);
        mutithreadTableWriter_.insert( false, 1);
        mutithreadTableWriter_.insert( null, 1);

        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("true", bt.getColumn("bool").get(0).getString());
        assertEquals("false", bt.getColumn("bool").get(1).getString());
        assertEquals("", bt.getColumn("bool").get(2).getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_byte() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (byte)1, (byte)1, (byte)1, (byte)1, (byte)1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (byte)1, (byte)1, (byte)1, (byte)1);
        assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 60000)
    public void test_insert_short() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (short)1, (short)1, (short)-1, (short)0, (short)-1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (short)1, (short)1, (short)0, (short)1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_int() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (int)1, (int)1, (int)-1, (int)0, (int)-1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (int)1, (int)1, (int)0, (int)1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_long() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (long)1, (long)1, (long)-1, (long)0, (long)-1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (long)1, (long)1, (long)0, (long)1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }
    @Test(timeout = 120000)
    public void test_insert_long_1() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (long)1, (long)1, (long)0, (long)0, (long)-1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (long)1, 9223372036854775807L, (long)0, (long)1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (long)1, -9223372036854775807L, (long)0, (long)1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("[0,9223372036854775807,-9223372036854775807]", bt.getColumn("long").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_float() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `float`double`id," +
                "[FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1.9f,0.2f,2);
        pErrorInfo=mutithreadTableWriter_.insert( -1.90f,-0.2f,2);
        pErrorInfo=mutithreadTableWriter_.insert( null,null,2);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(3, bt.rows());
        assertEquals("[1.89999998,-1.89999998,]", bt.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", bt.getColumn("double").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_double() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `float`double`id," +
                "[FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1.9,0.2,2);
        pErrorInfo=mutithreadTableWriter_.insert( -1.90,-0.2,2);
        pErrorInfo=mutithreadTableWriter_.insert( null,null,2);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.getString());
        assertEquals(3, bt.rows());
        assertEquals("[1.89999998,-1.89999998,]", bt.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", bt.getColumn("double").getString());
        //conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_string() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `symbol`string`uuid`ipaddr`int128`blob`id," +
                "[SYMBOL,STRING,UUID,IPADDR,INT128,BLOB,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "symbol");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert("QQ","qq","5d212a78-cc48-e3b1-4235-b4d91473ee87","192.168.1.13","e1671797c52e15f763380b45e841ec32","dsfgv",1);
        pErrorInfo=mutithreadTableWriter_.insert( null, null, null, null, null, null,2);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[QQ,]", bt.getColumn("symbol").getString());
        assertEquals("[qq,]", bt.getColumn("string").getString());
        assertEquals("[5d212a78-cc48-e3b1-4235-b4d91473ee87,]", bt.getColumn("uuid").getString());
        assertEquals("[192.168.1.13,0.0.0.0]", bt.getColumn("ipaddr").getString());
        assertEquals("[e1671797c52e15f763380b45e841ec32,]", bt.getColumn("int128").getString());
        assertEquals("[dsfgv,]", bt.getColumn("blob").getString());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_blob_to_string()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,STRING]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");

        String blob=conn.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
                "t.toStdJson()").getString();
        BasicString blob1 = new BasicString(blob,true);
        for (int i=0;i<1025;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, blob1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(1025,bt.rows());
        for (int i=0;i<1025;i++) {
            assertEquals(blob, bt.getColumn("blob").get(i).getString());
        }
    }

    @Test(timeout = 120000)
    public void test_insert_digital_0() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`short`long`float`double`id," +
                "[BOOL,SHORT,LONG,FLOAT,DOUBLE,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool");

        for (int i = 0; i < 15; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( false, 0,0,0.0f,0.0,0);
            assertEquals("code= info=",pErrorInfo.toString());
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(15, bt.rows());
        for (int i = 0; i < 15; i++) {
            assertEquals("false", bt.getColumn("bool").get(i).getString());
            assertEquals(String.valueOf(0), bt.getColumn("short").get(i).getString());
            assertEquals(String.valueOf(0), bt.getColumn("long").get(i).getString());
            assertEquals(0.0f, ((Scalar)bt.getColumn("float").get(i)).getNumber());
            assertEquals(0.0, ((Scalar)bt.getColumn("double").get(i)).getNumber());
            assertEquals(0, ((Scalar)bt.getColumn("id").get(i)).getNumber());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_time_date_to_nanotime() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0,`date`nanotime," +
                "[DATE,NANOTIME]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT);
            assertEquals("code=A1 info=Invalid object error when create scalar for column 'nanotime': Failed to insert data. Cannot convert Calendar to DT_NANOTIME.",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_time_date_to_nanotimestamp() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`nanotimestamp," +
                "[DATE,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT);
            assertEquals("code=A1 info=Invalid object error when create scalar for column 'nanotimestamp': Failed to insert data. Cannot convert Calendar to DT_NANOTIMESTAMP.",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_insert_time_date() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT, DT, DT, DT , DT, DT);
            assertEquals("code= info=",pErrorInfo.toString());

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

    @Test(timeout = 120000)
    public void test_insert_time_LocalTime() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0,`second`minute`time`nanotime," +
                "[SECOND,MINUTE,TIME,NANOTIME]);" +
                "share t as t1;");
        conn.run(sb.toString());

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                10, "second");
        LocalTime DT = LocalTime.of(1,1,1,1);
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT, DT, DT);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_time_LocalDate() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month," +
                "[DATE,MONTH]);" +
                "share t as t1;");
        conn.run(sb.toString());
        LocalDate DT = LocalDate.of(2022,1,1);

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date");
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_time_LocalDateTime() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `datetime`datehour`timstamp`nanotime`nanotimstamp," +
                "[DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        LocalDateTime DT = LocalDateTime.of(2022,1,1,1,1,1,10000);

        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "datetime");
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT, DT, DT, DT);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_time_lz4() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `date`month`second`minute`datetime`timestamp`datehour`nanotime`nanotimestamp," +
                "[DATE,MONTH,SECOND,MINUTE,DATETIME,TIMESTAMP,DATEHOUR,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        Date DT = new Date();
        LocalDateTime ld = LocalDateTime.of(2022,1,1,1,1,1,10000);

        SimpleDateFormat sdf ;
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
                1, "date",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});
        for (int i = 0; i < 8; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( DT, DT, DT, DT, DT , DT, DT,ld,ld);
            assertEquals("code= info=",pErrorInfo.toString());

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

    @Test(timeout = 120000)
    public void test_insert_dfs_part_null() throws Exception {
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
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( true, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt);");
        assertEquals(1, bt.rows());

    }

    /**
     *  array vector test
     * * @throws Exception
     */

    @Test(timeout = 120000)
    public  void test_insert_empty_arrayVector()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<1000;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        for (int i=0;i<1000;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,null);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(2000,bt.rows());
        for (int i=0;i<2000;i++) {
            assertEquals("[]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
        conn.run("undef(`t1,SHARED)");

    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_different_length()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv`arrayv1`arrayv2," +
                "[INT,INT[],BOOL[],BOOL[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{1},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{1,2},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{1,null,1},new Boolean[]{true,null,false},new Boolean[]{true});
        assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_int_Integer()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{1, i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals(1, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(i, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_int_int()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new int[]{1, i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals(1, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(i, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 60000)
    public  void test_insert_arrayVector_char_Byte()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,CHAR[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Byte[]{'a','3'});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 60000)
    public  void test_insert_arrayVector_char_byte()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,CHAR[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new byte[]{'a','3'});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_bool_Boolean()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,BOOL[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Boolean[]{true,false});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_bool_bool()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,BOOL[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new boolean[]{true,false});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_long_Long()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,LONG[]]);" +
                "share t as t1;");
        int time=1024;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Long[]{1l, Long.valueOf(i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(1l, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Long.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_long_long()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,LONG[]]);" +
                "share t as t1;");
        int time=1024;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new long[]{1l, (long)i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(1l, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Long.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_short_Short()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,SHORT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Short[]{1,i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Short.valueOf("1"), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Short.valueOf(""+i+""), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_short_short()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,SHORT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new short[]{1,i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Short.valueOf("1"), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Short.valueOf(""+i+""), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_float_Float()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,FLOAT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Float[]{0.0f,Float.valueOf(i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(0.0f, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Float.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_float_float()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,FLOAT[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new float[]{0.0f,(float)i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(0.0f, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Float.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_double_Double()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,DOUBLE[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Double[]{Double.valueOf(0),Double.valueOf(i-10)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Double.valueOf(0), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Double.valueOf(i-10), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_double_double()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv," +
                "[INT,DOUBLE[]]);" +
                "share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new double[]{(double)0,(double)(i-10)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Double.valueOf(0), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Double.valueOf(i-10), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_date_month()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`arrayv1`arrayv2," +
                "[INT,DATE[],MONTH[]]); share t as t1;");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "arrayv1");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new LocalDate[]{LocalDate.of(1,1,1)},
                    new LocalDate[]{LocalDate.of(2021,1,1)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(LocalDate.of(1,1,1), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv1")).getVectorValue(i).get(0)).getTemporal());
            assertEquals("2021.01M", ((BasicArrayVector)bt.getColumn("arrayv2")).getVectorValue(i).get(0).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_time_minute_second()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `time`minute`second," +
                "[TIME[],MINUTE[],SECOND[]]);" +
                "share t as t1");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "time");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,1)});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_datetime_timestamp_nanotime_nanotimstamp()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `datetime`timestamp`nanotime`nanotimstamp," +
                "[DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[]]);" +
                "share t as t1");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "datetime");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalTime[]{LocalTime.of(1,1,1,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i),
                            LocalDateTime.of(2022,2,1,1,1,2,45364654+i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }

       // System.out.println(LocalDateTime.of(2022,2,1,1,1,2,033));
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(time,bt.rows());
        System.out.println(((Scalar)((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(0).get(0)).getTemporal());

        for (int i=0;i<time;i++) {
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2), ((Scalar)((BasicArrayVector)bt.getColumn("datetime")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45000000), ((Scalar)((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalTime.of(1,1,1,45364654+i), ((Scalar)((BasicArrayVector)bt.getColumn("nanotime")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45364654+i), ((Scalar)((BasicArrayVector)bt.getColumn("nanotimstamp")).getVectorValue(i).get(0)).getTemporal());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_otherType()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `uuid`int128`ipaddr," +
                "[UUID[],INT128[],IPADDR[]]);" +
                "share t as t1");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "uuid");
        BasicUuidVector bv= (BasicUuidVector) conn.run("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87',,'5d212a78-cc48-e3b1-4235-b4d91473ee87'])");
        BasicInt128Vector iv= (BasicInt128Vector) conn.run("int128(['e1671797c52e15f763380b45e841ec32',,'e1671797c52e15f763380b45e841ec32'])");
        BasicIPAddrVector ipv= (BasicIPAddrVector) conn.run("ipaddr(['192.168.1.13',,'192.168.1.13'])");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new String[]{"5d212a78-cc48-e3b1-4235-b4d91473ee87",null,"5d212a78-cc48-e3b1-4235-b4d91473ee87"},new String[]{"e1671797c52e15f763380b45e841ec32",null,"e1671797c52e15f763380b45e841ec32"}
            ,new String[]{"192.168.1.13",null,"192.168.1.13"});
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public  void test_insert_blob()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");

        String blob=conn.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
                "t.toStdJson()").getString();
        for (int i=0;i<1025;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, blob);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(1025,bt.rows());
        for (int i=0;i<1025;i++) {
            assertEquals(blob, bt.getColumn("blob").get(i).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_string_to_blob()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`blob," +
                "[INT,BLOB]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");

        String blob=conn.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
                "t.toStdJson()").getString();
        BasicString blob1 = new BasicString(blob,true);
        for (int i=0;i<1025;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, blob1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(1025,bt.rows());
        for (int i=0;i<1025;i++) {
            assertEquals(blob, bt.getColumn("blob").get(i).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_wrongtype2()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`double," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false,null,1, 1,
                1, "int");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i=0;i<20;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicInt(2));
            tb.add(row);
        }
        ErrorCodeInfo pErrorInfo=mutithreadTableWriter_.insertUnwrittenData(tb);
        assertEquals("code= info=",pErrorInfo.toString());
        for (int i=0;i<2;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicString("1"));
            tb.add(row);
        }
        pErrorInfo=mutithreadTableWriter_.insertUnwrittenData(tb);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        List<List<Entity>> le=mutithreadTableWriter_.getUnwrittenData();
        MultithreadedTableWriter.Status status= mutithreadTableWriter_.getStatus();
        assertEquals(bt.rows(),status.unsentRows+status.sendFailedRows+status.sentRows);
    }

    @Test(timeout = 120000)
    public void test_insert_othertypes() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share t as t1;");
        sb.append("ext = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share ext as ext1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "uuid");
        List<List<Entity>> tb = new ArrayList<>();

        for (int i = 0; i < 15; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicUuid(321324, 32433));
            row.add(new BasicIPAddr(321324, 32433));
            row.add(new BasicInt128(454, 456));
            tb.add(row);
            conn.run("tableInsert{ext1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_string_otherType_lz4() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`symbol`string`uuid`ipaddr`int128," +
                "[CHAR,SYMBOL,STRING,UUID, IPADDR, INT128]);" +
                "share t as t1;");
        sb.append("tt = streamTable(1000:0, `char`symbol`string`uuid`ipaddr`int128,[CHAR,SYMBOL,STRING,UUID, IPADDR, INT128]);" +
                "share tt as t2;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 5, 1,
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 's', "2" + i, "2" + i, "00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        BasicTable ex = (BasicTable) conn.run("select * from t2;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    @Test(timeout = 120000)
    public void test_insert_BasicType_in_java() throws Exception {
        String script="t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share t as t1;" +
                "tt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,\n" +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share tt as t2;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 10, 1,
                1, "short");

        Month mon=LocalDate.of(2022,2,2).getMonth();
        for (int i = 0; i < 10000; i++) {
            ErrorCodeInfo pErrorInfo=mutithreadTableWriter_.insert(new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),new BasicFloat(2.312f),new BasicDouble(3.2),
                    new BasicString("sedf"+i),new BasicString("sedf"),new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),
                    new BasicInt(21),new BasicString("d"+i,true));
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_keytable() throws Exception {
        String script =
                "t=keyedStreamTable(`sym,1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 10, 1,
                1, "tradeDate");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            mutithreadTableWriter_.insert( "2"+i%2, LocalDateTime.of(2012, 1, i % 10 + 1, 1, i%10), i + 0.1, i + 0.1, i % 10, i + 0.1);
        }
        Thread.sleep(2000);
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        assertEquals(2,ex.rows());
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)//(expected = RuntimeException.class)
    public void test_insert_dt_multipleThreadCount() throws Exception {
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

    @Test(timeout = 120000)//(expected = RuntimeException.class)
    public void test_insert_tsdb_dt_multipleThreadCount() throws Exception {
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

    @Test(timeout = 120000)
    public void test_insert_dt_multipleThread() throws Exception {
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
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test(timeout = 120000)
    public void test_insert_TSDBdt_multipleThread() throws Exception {
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
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test(timeout = 120000)
    public void test_insert_dt_oneThread() throws Exception {
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
            ErrorCodeInfo pErrorInfo=mutithreadTableWriter_.insert( "2" + i, LocalDateTime.of(2012, 1, i % 10 + 1, 1, i), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    /**
     * test dfs partitionType
     * @throws Exception
     */
    @Test(timeout = 120000)
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

    @Test(timeout = 120000)
    public void test_insert_dfs_hash() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_range_outof_partitions() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 where tradeDate>=2022.01.03,tradeDate< 2022.01.21 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_value_hash() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i, LocalDate.of(2022, 1, i + 1), LocalTime.of(1, i), i + 0.1, i + 0.1, i, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }

        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        assertEquals(10, bt.rows());
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    @Test(timeout = 120000)
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
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "IBM", LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_value_value() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_value_range() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test(timeout = 120000)
    public void test_insert_dfs_range_value() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test(timeout = 120000)
    public void test_insert_dfs_range_range() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_range_hash() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_hash_range() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_hash_hash() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_hash_value() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_value_hash_range() throws Exception {
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_datetime() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }


    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_timestamp() throws Exception {

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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_datehour_partironcol_nanotimestamp() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0,3534435), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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
        Thread.sleep(200);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_datetime() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        //Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_timestamp() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        //Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_nanotimestamp() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_DATE_range() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDate.of(2022,2,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_datetime_range() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,3,3,3), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_timestamp_range() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_date_partironcol_nanotimestamp_range() throws Exception {
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
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,1,1,1,1), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_datetime() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_timestamp() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        //Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_PartitionType_partirontype_month_partironcol_Nanotimestamp() throws Exception {
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
        //Integer threadTime = 10;
        List<List<Entity>> tb = new ArrayList<>();
        //Date dt=new Date();
        for (int i = 0; i < insertTime; i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2" + i % 100, LocalDateTime.of(2022,2,2,12,2), i + 0.1,i + 0.1, (i % 10) + 1, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_dfs_multiple_mutithreadTableWriter_sameTable() throws Exception {
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
            mutithreadTableWriter1.insertUnwrittenData(tb1);
            mutithreadTableWriter2.insertUnwrittenData(tb2);
        }
        for (int n = 0; n < 10; n++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row);
            }
            List<Entity> row1 = new ArrayList<>();
            row1.add(new BasicInt(2));
            row1.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row1);
            }
        }
        //Thread.sleep(2000);
        BasicTable bt = (BasicTable) conn.run("select * from  loadTable('dfs://test_MultithreadedTableWriter',`pt)  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter1.waitForThreadCompletion();
        mutithreadTableWriter2.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_dfs_multiple_mutithreadTableWriter_differentTable() throws Exception {
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
            mutithreadTableWriter1.insertUnwrittenData(tb);
            mutithreadTableWriter2.insertUnwrittenData(tb);
            mutithreadTableWriter3.insertUnwrittenData(tb);
            mutithreadTableWriter4.insertUnwrittenData(tb);
            mutithreadTableWriter5.insertUnwrittenData(tb);
        }
        for (int n = 0; n < 10; n++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row);
            }
            List<Entity> row1 = new ArrayList<>();
            row1.add(new BasicInt(2));
            row1.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
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

    @Test(timeout = 1200000)
    public void test_insert_dfs_multiple_mutithreadTableWriter_differentDataBase() throws Exception {
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
            mutithreadTableWriter1.insertUnwrittenData(tb);
            mutithreadTableWriter2.insertUnwrittenData(tb);
            mutithreadTableWriter3.insertUnwrittenData(tb);
        }
        for (int n = 0; n < 8; n++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row);
            }
            List<Entity> row1 = new ArrayList<>();
            row1.add(new BasicInt(2));
            row1.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
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

    @Test(timeout = 1200000)
    public void test_insert_multiple_mutithreadTableWriter_differentTable_status_isExiting() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("tmp1=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tmp1 as st1;" +
                "tmp2=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tmp2 as st2;" +
                "tmp3=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tmp3 as st3;" +
                "tmp4=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tmp4 as st4;" +
                "tmp5=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tmp5 as st5;");
        conn.run(sb.toString());
        MultithreadedTableWriter mutithreadTableWriter1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "st1", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "st2", false, false, null, 100, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter3 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "st3", false, false, null, 1000, 1,
                20, "volume");
        MultithreadedTableWriter mutithreadTableWriter4 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "st4", false, false, null, 10, 1,
                2, "volume");
        MultithreadedTableWriter mutithreadTableWriter5 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "st5", false, false, null, 10, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        writeData(1, tb);
        writeData(2, tb);
        for (int i = 0; i < 10; i++) {
            mutithreadTableWriter1.insertUnwrittenData(tb);
            mutithreadTableWriter2.insertUnwrittenData(tb);
            mutithreadTableWriter3.insertUnwrittenData(tb);
            mutithreadTableWriter4.insertUnwrittenData(tb);
            mutithreadTableWriter5.insertUnwrittenData(tb);
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
        BasicTable bt1 = (BasicTable) conn.run("select * from tmp1  order by volume,valueTrade;");
        BasicTable bt2 = (BasicTable) conn.run("select * from tmp2  order by volume,valueTrade;");
        BasicTable bt3 = (BasicTable) conn.run("select * from tmp3  order by volume,valueTrade;");
        BasicTable bt4 = (BasicTable) conn.run("select * from tmp4 order by volume,valueTrade;");
        BasicTable bt5 = (BasicTable) conn.run("select * from tmp5  order by volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from tmp1 order by volume,valueTrade;");
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

    @Test(timeout = 220000)
    public void test_insert_tsdb_multiple_mutithreadTableWriter_differentTable_useSSL() throws Exception {
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
            mutithreadTableWriter1.insertUnwrittenData(tb);
            mutithreadTableWriter2.insertUnwrittenData(tb);
            mutithreadTableWriter3.insertUnwrittenData(tb);
            mutithreadTableWriter4.insertUnwrittenData(tb);
            mutithreadTableWriter5.insertUnwrittenData(tb);
        }
        for (int n = 0; n < 10; n++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row);
            }
            List<Entity> row1 = new ArrayList<>();
            row1.add(new BasicInt(2));
            row1.add(new BasicDouble(5));
            for (int i = 0; i < 10000; i++) {
                conn.run("tableInsert{t1}", row1);
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

    @Test(timeout = 120000)
    public void test_insert_tsdb_keepDuplicates() throws Exception {
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
        mutithreadTableWriter1.insertUnwrittenData(tb);
        mutithreadTableWriter2.insertUnwrittenData(tb);
        mutithreadTableWriter3.insertUnwrittenData(tb);
        mutithreadTableWriter4.insertUnwrittenData(tb);
        mutithreadTableWriter5.insertUnwrittenData(tb);
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

    @Test(timeout = 120000)
    public void test_insert_dfs_length_eq_1024() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }

    @Test(timeout = 1200000)
    public void test_insert_dfs_length_eq_1048576() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
    }

    @Test(timeout = 1200000)
    public void test_insert_dfs_length_morethan_1048576() throws Exception {
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
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

    @Test(timeout = 120000)
    public void test_insert_streamTable_multipleThread() throws Exception {
        String script = "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as t2;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t2", false, false, null, 1024, 1,
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
        BasicTable bt = (BasicTable) conn.run("select * from t2 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex, bt);
        mutithreadTableWriter_.waitForThreadCompletion();
    }

    @Test(timeout = 120000)
    public void test_insert_streamtable_length_eq_1024() throws Exception {
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades", false, false, null, 1000, 1,
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);

    }

    @Test(timeout = 600000)
    public void test_insert_streamtable_length_eq_1048576() throws Exception {
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades", false, false, null, 1000, 1,
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
    }

    @Test(timeout = 600000)
    public void test_insert_streamtable_length_morethan_1048576() throws Exception {
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades", false, false, null, 1000, 1,
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
    }


    @Test(timeout = 120000)
    public void test_insert_streamtable_200cols() throws Exception {
        String script ="t=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;\n" +
                "addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;" +
                "addColumn(tt,\"col\"+string(1..200),take([DOUBLE],200));share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades", false, false, null, 1000, 1,
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0),i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }

    @Test(timeout = 120000)
    public void test_insert_dfstable_200cols() throws Exception {
        String script ="t=table(1:0, `sym`tradeDate, [SYMBOL,TIMESTAMP]);\n" +
                "addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
                "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, VALUE, date(1..2),,'TSDB');\n" +
                "createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"tradeDate\"],sortColumns=`sym,compressMethods={tradeDate:\"delta\"});" ;
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
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0),i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1
                    ,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1,i+0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt1) order by sym,tradeDate;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate;");
        assertEquals(ex.rows(), bt.rows());
        for (int i = 0; i < ex.columns(); i++) {
            assertEquals(ex.getColumn(i).getString(), bt.getColumn(i).getString());
        }
    }


    @Test(timeout = 120000)
    public void test_insert_after_insertwrong() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share t as t1;");
        sb.append("ext = streamTable(1000:0, `uuid`ipaddr`int128," +
                "[UUID, IPADDR, INT128]);" +
                "share ext as ext1;go");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "uuid");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicUuid(321324, 32433));
            row.add(new BasicIPAddr(321324, 32433));
            row.add(new BasicInt128(454, 456));
            tb.add(row);
            conn.run("tableInsert{ext1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert("00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals("code= info=", pErrorInfo.toString());
        }
        mutithreadTableWriter_.insert("00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1");
        for (int i = 0; i < 15; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicUuid(321324, 32433));
            row.add(new BasicIPAddr(321324, 32433));
            row.add(new BasicInt128(454, 456));
            tb.add(row);
            conn.run("tableInsert{ext1}", row);
            pErrorInfo = mutithreadTableWriter_.insert("00000000-0004-e72c-0000-000000007eb1", "0:0:4:e72c::7eb1", "00000000000001c600000000000001c8");
            assertEquals("code= info=", pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1 order by uuid;");
        BasicTable ex = (BasicTable) conn.run("select * from ext order by  uuid;");
        checkData(ex,bt);
    }

    public class HashDataWriter implements Runnable {
        public ArrayList<ArrayList<Object>> data;
        private MultithreadedTableWriter mtw;
        public HashDataWriter(ArrayList data, MultithreadedTableWriter mtw) {
            this.data = data;
            this.mtw = mtw;
        }
        @Override
        public void run() {
            List<ArrayList<Object>> subData = data;
            for (int i = 0; i < subData.size(); i++) {
                mtw.insert(subData.get(i).toArray());
            }
        }
    }

    public void write(ArrayList data, MultithreadedTableWriter mtw) throws Exception {
        for (int i = 0; i < 10; i++) {
            new Thread(new HashDataWriter(data, mtw)).start();
        }
    }
    @Test(timeout = 120000)
    public void test_mtw_concurrentWrite_hash_string_partition_table() throws Exception {
        conn.run("login(`admin,`123456)\n" +
                "dbName = \"dfs://test_mtw_concurrentWrite_hash_partition_table\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)\n" +
                "}\n" +
                "db = database(dbName,HASH,[STRING,10])\n" +
                "t = table(10:0,`sym`price`val,[STRING,DOUBLE,INT])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym)");
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_hash_partition_table", "pt",
                false, false, null, 1000, 0.001f, 10, "sym");

        ArrayList<Object> write_data =new ArrayList<Object>();
        int n = 100000;
        for(int i = 0;i<n;i++){
            ArrayList<Object> one_row_list = new ArrayList<Object>();
            int tmp =(int)(Math.random()*100);
            one_row_list.add(tmp+"");
            one_row_list.add((double)tmp);
            one_row_list.add(tmp);
            write_data.add(one_row_list);
        }
        write(write_data,mtw);
        Thread.sleep(8000);
        BasicLong result = (BasicLong) conn.run("exec count(*) from loadTable(\"dfs://test_mtw_concurrentWrite_hash_partition_table\",`pt)");
        assertEquals(1000000,result.getLong());
    }

    @Test(timeout = 120000)
    public void test_mtw_concurrentWrite_hash_Symbol_partition_table() throws Exception {
        conn.run("login(`admin,`123456)\n" +
                "dbName = \"dfs://test_mtw_concurrentWrite_hash_partition_table\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)\n" +
                "}\n" +
                "db = database(dbName,HASH,[SYMBOL,10])\n" +
                "t = table(10:0,`sym`price`val,[SYMBOL,DOUBLE,INT])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym)");
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_hash_partition_table", "pt",
                false, false, null, 1000, 0.001f, 10, "sym");

        ArrayList<Object> write_data =new ArrayList<Object>();
        int n = 100000;
        for(int i = 0;i<n;i++){
            ArrayList<Object> one_row_list = new ArrayList<Object>();
            int tmp =(int)(Math.random()*100);
            one_row_list.add(tmp+"");
            one_row_list.add((double)tmp);
            one_row_list.add(tmp);
            write_data.add(one_row_list);
        }
        write(write_data,mtw);
        Thread.sleep(8000);
        BasicLong result = (BasicLong) conn.run("exec count(*) from loadTable(\"dfs://test_mtw_concurrentWrite_hash_partition_table\",`pt)");
        assertEquals(1000000,result.getLong());
    }
//    @Test(timeout = 120000)
//    public void test_mtw_concurrentWrite_hash_blob_partition_table() throws Exception {
//        conn.run("login(`admin,`123456)\n" +
//                "dbName = \"dfs://test_mtw_concurrentWrite_hash_partition_table\"\n" +
//                "if(existsDatabase(dbName)){\n" +
//                "\tdropDB(dbName)\n" +
//                "}\n" +
//                "db = database(dbName,HASH,[BLOB,10])\n" +
//                "t = table(10:0,`sym`price`val,[SYMBOL,DOUBLE,INT])\n" +
//                "pt = db.createPartitionedTable(t,`pt,`sym)");
//        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_hash_partition_table", "pt",
//                false, false, null, 1000, 0.001f, 10, "sym");
//
//        ArrayList<Object> write_data =new ArrayList<Object>();
//        int n = 100000;
//        for(int i = 0;i<n;i++){
//            ArrayList<Object> one_row_list = new ArrayList<Object>();
//            int tmp =(int)(Math.random()*100);
//            one_row_list.add(tmp+"");
//            one_row_list.add((double)tmp);
//            one_row_list.add(tmp);
//            write_data.add(one_row_list);
//        }
//        write(write_data,mtw);
//        Thread.sleep(20000);
//        BasicLong result = (BasicLong) conn.run("exec count(*) from loadTable(\"dfs://test_mtw_concurrentWrite_hash_partition_table\",`pt)");
//        assertEquals(1000000,result.getLong());
//    }

    @Test(timeout = 120000)
    public void test_mtw_concurrentWrite_getFailedData() throws Exception {
        conn.run("login(`admin,`123456)\n" +
                "dbName = \"dfs://test_mtw_concurrentWrite_getFailedData1\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 10 20 30)\n" +
                "t = table(10:0,`id`price`val,[INT,DOUBLE,INT])\n" +
                "pt = db.createPartitionedTable(t,`pt,`id)");
        MultithreadedTableWriter mtw_getFailedData1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_getFailedData1", "pt",
                false, false, null, 1000, 0.001f, 10, "id");
        MultithreadedTableWriter mtw_getFailedData2 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_getFailedData1", "pt",
                false, false, null, 1000, 0.001f, 10, "id");

        for(int i = 0;i <2000;i++) {
            int tmp =5;
            mtw_getFailedData1.insert(tmp, (double) tmp, 1);
        }
        // insert false
        try {
            for (int i = 0; i < 2000; i++) {
                int tmp = 5;
                mtw_getFailedData2.insert(tmp, (double) tmp, 2);
            }
        }catch (Exception ex){
        }
        Thread.sleep(50000);
        List<List<Entity>> failedData1 = mtw_getFailedData1.getFailedData();
        List<List<Entity>> failedData2 = mtw_getFailedData2.getFailedData();
        List<List<Entity>> unwrittenData1 = mtw_getFailedData1.getUnwrittenData();
        List<List<Entity>> unwrittenData2 = mtw_getFailedData2.getUnwrittenData();
        BasicInt writedData1 = (BasicInt) conn.run("(exec count(*) from pt where val = 1)[0]");
        BasicInt writedData2 = (BasicInt) conn.run("(exec count(*) from pt where val = 2)[0]");
        assertEquals(2000,writedData1.getInt()+failedData1.size()+unwrittenData1.size());
        assertEquals(2000,writedData2.getInt()+failedData2.size()+unwrittenData2.size());
    }

    @Test(timeout = 120000)
    public void test_mtw_concurrentWrite_getFailedData_when_unfinished_write() throws Exception {
        conn.run("login(`admin,`123456)\n" +
                "dbName = \"dfs://test_mtw_concurrentWrite_getFailedData\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 10 20 30)\n" +
                "t = table(10:0,`id`price`val,[INT,DOUBLE,INT])\n" +
                "pt = db.createPartitionedTable(t,`pt,`id)");
        MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_mtw_concurrentWrite_getFailedData", "pt",
                false, false, null, 1000, 0.001f, 10, "id");
        for(int i = 0;i <10000;i++) {
            int tmp =5;
            mtw1.insert(tmp, (double) tmp, 1);
        }
        List<List<Entity>> failedData1 = mtw1.getFailedData();
        List<List<Entity>> unwrittenData1 = mtw1.getUnwrittenData();
        mtw1.waitForThreadCompletion();
        BasicInt writedData1 = (BasicInt) conn.run("(exec count(*) from pt where val = 1)[0]");
        assertEquals(10000,writedData1.getInt()+failedData1.size()+unwrittenData1.size());
    }

    @Test(timeout = 120000)
    public void test_mtw_tableUpsert_DP_updateFirst() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,10,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=false","keyColNames=`sym"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from pt where price = 8.3").rows());
        assertEquals(0,conn.run("select * from pt where price = 7.2").rows());
        assertEquals(8,ua.rows());
        assertEquals(1,conn.run("select * from pt where price = 11.1;").rows());
        assertEquals(1,conn.run("select * from pt where price = 10.5").rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_objNull() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")){\n" +
                "    dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "t = table(10000:0,`sym`date`price`val,[STRING,DATE,FLOAT,INT])\n" +
                "db = database(\"dfs://valuedemo\",VALUE,\"A\" \"B\" \"C\")\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,10,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym","sortColumns=`val"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2001,12,12)),new BasicFloat((float) 4.7),new BasicInt(7));
        mtw.insert(new BasicString("D"),new BasicDate(LocalDate.of(2003,4,24)),new BasicFloat((float) 2.2),new BasicInt(4));
        mtw.insert(new BasicString("C"),new BasicDate(LocalDate.of(2004,2,2)),new BasicFloat((float) 5.3),new BasicInt(11));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2007,10,16)),new BasicFloat((float) 1.6),new BasicInt(1));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2005,7,16)),new BasicFloat((float) 8.4),new BasicInt(9));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        assertEquals(5,ua.rows());
        assertNotEquals("1",ua.getColumn(3).get(0).getString());
        assertNotEquals("11",ua.getColumn(3).get(4).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_ignoreNull() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createPartitionedTable(t, `pt, `id).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,10,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id"});
        mtw.insert(new BasicInt(1),new BasicInt(1),new BasicInt(1));
        mtw.insert(new BasicInt(2),new BasicInt(2),null);
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(0,conn.run("select * from pt where value = NULL").rows());
        MultithreadedTableWriter mtw2 = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,10,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=false","keyColNames=`id"});
        mtw2.insert(new BasicInt(2),new BasicInt(2),null);
        mtw2.waitForThreadCompletion();
        BasicTable ua2 = (BasicTable) conn.run("select * from pt where value=NULL;");
        assertEquals(1,ua2.rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_sortColName() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,10,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym","sortColumns=`date`val"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt where sym = \"A\";");
        assertEquals("2021.12.09",bt.getColumn(1).get(0).getString());
        assertEquals("2021.12.11",bt.getColumn(1).get(2).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_unsortColNames() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,10,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt where sym = \"A\";");
        assertEquals("2021.12.11",bt.getColumn(1).get(0).getString());
        assertEquals("2021.12.10",bt.getColumn(1).get(2).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_bigData() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createPartitionedTable(t, \"pt\", `id).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,10,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id2"});
        for(int i=1;i<=1008000;i++){
            mtw.insert(new BasicInt((i%10)),new BasicInt(i+100),new BasicInt(10080101-i));
        }
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select count(*) from pt");
        assertEquals("1008100",ua.getColumn(0).get(0).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DP_nomatchCols() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createPartitionedTable(t, \"pt\", `id).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,10,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id2"});
        mtw.insert(new BasicString("match"),new BasicInt(101),new BasicInt(101));
        mtw.insert(new BasicString("un"),new BasicString("102"),new BasicString("102"));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(100,ua.rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_updateFirst() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=false","keyColNames=`sym"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,9)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from pt where price = 8.3").rows());
        assertEquals(0,conn.run("select * from pt where price = 7.2").rows());
        assertEquals(8,ua.rows());
        assertEquals(1,conn.run("select * from pt where price = 11.1;").rows());
        assertEquals(1,conn.run("select * from pt where price = 10.5").rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_objNull() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")){\n" +
                "    dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "t = table(10000:0,`sym`date`price`val,[STRING,DATE,FLOAT,INT])\n" +
                "db = database(\"dfs://valuedemo\",VALUE,\"A\" \"B\" \"C\")\n" +
                "pt = db.createTable(t,`pt)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym","sortColumns=`val"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2001,12,12)),new BasicFloat((float) 4.7),new BasicInt(7));
        mtw.insert(new BasicString("D"),new BasicDate(LocalDate.of(2003,4,24)),new BasicFloat((float) 2.2),new BasicInt(4));
        mtw.insert(new BasicString("C"),new BasicDate(LocalDate.of(2004,2,2)),new BasicFloat((float) 5.3),new BasicInt(11));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2007,10,16)),new BasicFloat((float) 1.6),new BasicInt(1));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2005,7,16)),new BasicFloat((float) 8.4),new BasicInt(9));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        assertEquals(5,ua.rows());
        assertNotEquals("1",ua.getColumn(3).get(0).getString());
        assertNotEquals("11",ua.getColumn(3).get(4).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_ignoreNull() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createTable(t,`pt).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,1,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id"});
        mtw.insert(new BasicInt(1),new BasicInt(1),new BasicInt(1));
        mtw.insert(new BasicInt(2),new BasicInt(2),null);
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(0,conn.run("select * from pt where value = NULL").rows());
        MultithreadedTableWriter mtw2 = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,1,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=false","keyColNames=`id"});
        mtw2.insert(new BasicInt(2),new BasicInt(2),null);
        mtw2.waitForThreadCompletion();
        BasicTable ua2 = (BasicTable) conn.run("select * from pt where value=NULL;");
        assertEquals(1,ua2.rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_sortColName() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym","sortColumns=`date`val"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt;");
        assertEquals("2021.12.09",bt.getColumn(1).get(0).getString());
        assertEquals("2021.12.11",bt.getColumn(1).get(7).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_unSortColNames() throws Exception {
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=\"A\" \"B\" \"C\" \"A\" \"D\" \"B\" \"A\"\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,\"A\" \"B\" \"C\")\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://upsert","pt",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym"});
        mtw.insert(new BasicString("A"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(11.1),new BasicInt(12));
        mtw.insert(new BasicString("B"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(10.5),new BasicInt(9));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2021,12,11)),new BasicDouble(6.9),new BasicInt(11));
        mtw.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt;");
        assertEquals("2021.12.11",bt.getColumn(1).get(0).getString());
        assertEquals("2021.12.11",bt.getColumn(1).get(7).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_bigData() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createTable(t,`pt).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,1,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id2"});
        for(int i=1;i<=1008000;i++){
            mtw.insert(new BasicInt((i%10)),new BasicInt(i+100),new BasicInt(10080101-i));
        }
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select count(*) from pt");
        assertEquals("1008100",ua.getColumn(0).get(0).getString());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DD_nomatchCols() throws Exception {
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createTable(t,`pt).append!(t)\n";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://valuedemo","pt",
                false,false,null,1000,1,1,"id",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`id2"});
        mtw.insert(new BasicString("match"),new BasicInt(101),new BasicInt(101));
        mtw.insert(new BasicString("un"),new BasicString("102"),new BasicString("102"));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        //属性不匹配，插入失败，列数不变
        assertEquals(100,ua.rows());
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_KeyedTable_updateSame() throws Exception {
        String script = "sym= \"A\" \"B\" \"C\"\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true"});
        String scripts = "newData = table(`C`D`E as sym1, 2021.01.06 2022.07.11 2022.09.21 as date1, 400 550 720 as x1, 17 88 190 as y1);";
        mtw.insert(new BasicString("C"),new BasicDate(LocalDate.of(2021,1,6)),new BasicInt(400),new BasicInt(17));
        mtw.insert(new BasicString("D"),new BasicDate(LocalDate.of(2022,7,11)),new BasicInt(550),new BasicInt(88));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2022,9,21)),new BasicInt(720),new BasicInt(190));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(5,ua.rows());
        assertEquals(0,conn.run("select * from t where x=3;").rows());
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_KeyedTable_sortColNames() throws Exception {
        String script = "sym= \"A\" \"B\" \"C\"\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","KeyColNames=`sym","sortColumns=`sym`date"});
        mtw.insert(new BasicString("C"),new BasicDate(LocalDate.of(2021,1,6)),new BasicInt(400),new BasicInt(17));
        mtw.insert(new BasicString("D"),new BasicDate(LocalDate.of(2022,7,11)),new BasicInt(550),new BasicInt(88));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2022,9,21)),new BasicInt(720),new BasicInt(190));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(3,ua.rows());
        //sortColumns不支持keyedTable，插入失败
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_KeyedTable_NomatchClos() throws Exception {
        String script = "sym= \"A\" \"B\" \"C\"\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true"});
        mtw.insert(new BasicPoint(3.9,7.6),new BasicDate(LocalDate.of(2021,1,6)),new BasicInt(400),new BasicInt(17));
        mtw.insert(new BasicString("D"),new BasicDate(LocalDate.of(2022,7,11)),new BasicString("550"),new BasicInt(88));
        mtw.insert(new BasicString("E"),new BasicDate(LocalDate.of(2022,9,21)),new BasicInt(720),new BasicString("190"));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(3,ua.rows());
        //类型不匹配插入失败，行数不变
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_indexedTable_updateSame() throws Exception {
        String script = "sym=\"A\" \"B\" \"C\" \"D\" \"E\"\n" +
                "id=5 4 3 2 1\n" +
                "val=52 64 25 48 71\n" +
                "t=indexedTable(`sym`id,sym,id,val);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true"});
        mtw.insert(new BasicString("C"),new BasicInt(3),new BasicInt(36));
        mtw.insert(new BasicString("D"),new BasicInt(2),new BasicInt(77));
        mtw.insert(new BasicString("E"),new BasicInt(1),new BasicInt(66));
        mtw.insert(new BasicString("F"),new BasicInt(7),new BasicInt(94));
        mtw.insert(new BasicString("G"),new BasicInt(8),new BasicInt(82));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(7,ua.rows());
        assertEquals(0,conn.run("select * from t where val = 25;").rows());
        assertEquals(1,conn.run("select * from t where val = 77;").rows());
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_indexedTable_SortColName() throws Exception {
        String script = "sym=\"A\" \"B\" \"C\" \"D\" \"E\"\n" +
                "id=5 4 3 2 1\n" +
                "val=64 52 25 48 71\n" +
                "t=indexedTable(`sym`id,sym,id,val);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"sym",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`sym","sortColumns=`val"});
        mtw.insert(new BasicString("C"),new BasicInt(3),new BasicInt(36));
        mtw.insert(new BasicString("D"),new BasicInt(2),new BasicInt(77));
        mtw.insert(new BasicString("E"),new BasicInt(1),new BasicInt(66));
        mtw.insert(new BasicString("F"),new BasicInt(7),new BasicInt(94));
        mtw.insert(new BasicString("G"),new BasicInt(8),new BasicInt(82));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(5,ua.rows());
        //indexedTable不支持sortColNames，所以插入失败，行数不变
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_indexedTable_AlmostAllDatatype() throws Exception {
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,18)\n" +
                "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"cint",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true"});
        mtw.insert(new BasicBoolean(true),new BasicByte((byte) 'D'),new BasicShort((short) 21),new BasicInt(4),new BasicLong(55),new BasicDate(LocalDate.now()),
                new BasicMonth(2012,Month.AUGUST),new BasicTime(LocalTime.of(13,7,55)),new BasicMinute(LocalTime.of(11,40,53)),
                new BasicSecond(895),new BasicDateTime(LocalDateTime.MIN),new BasicTimestamp(LocalDateTime.MAX),new BasicNanoTime(LocalTime.of(22,14,58,43847)),
                new BasicNanoTimestamp(LocalDateTime.of(2013,11,4,13,34,49,424246)),new BasicFloat((float) 12.71),new BasicDouble(0.783),
                new BasicString("MongoDB"),new BasicDateHour(LocalDateTime.now()),new BasicDecimal32(98,2),new BasicDecimal64(188.68,4),new BasicDecimal128("188.68",18));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(3,ua.rows());
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from st where cminute=08:12m").rows());
        assertEquals(1,conn.run("select * from st where cminute=11:40m").rows());
        assertEquals("188.6800", ((BasicDecimal64Vector)(ua.getColumn("cdecimal64"))).get(1).getString());
        assertEquals("188.680000000000000000", ((BasicDecimal128Vector)(ua.getColumn("cdecimal128"))).get(1).getString());
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_KeyedTable_AlmostAllDatatype() throws Exception {
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,18)\n" +
                "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "share t as st;";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","","st",
                false,false,null,1000,1,1,"cint",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true"});
        mtw.insert(new BasicBoolean(true),new BasicByte((byte) 'D'),new BasicShort((short) 21),new BasicInt(4),new BasicLong(55),new BasicDate(LocalDate.now()),
                new BasicMonth(2012,Month.AUGUST),new BasicTime(LocalTime.of(13,7,55)),new BasicMinute(LocalTime.of(11,40,53)),
                new BasicSecond(895),new BasicDateTime(LocalDateTime.MIN),new BasicTimestamp(LocalDateTime.MAX),new BasicNanoTime(LocalTime.of(22,14,58,43847)),
                new BasicNanoTimestamp(LocalDateTime.of(2013,11,4,13,34,49,424246)),new BasicFloat((float) 12.71),new BasicDouble(0.783),
                new BasicString("MongoDB"),new BasicDateHour(LocalDateTime.now()),new BasicDecimal32(98,2),new BasicDecimal64(188.68,4),new BasicDecimal128("188.68",18));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(3,ua.rows());
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from st where cminute=08:12m").rows());
        assertEquals(1,conn.run("select * from st where cminute=11:40m").rows());
        assertEquals("188.6800", ((BasicDecimal64Vector)(ua.getColumn("cdecimal64"))).get(1).getString());
        assertEquals("188.680000000000000000", ((BasicDecimal128Vector)(ua.getColumn("cdecimal128"))).get(1).getString());
        conn.run("undef(`st,SHARED);");
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_PartitionedTable_AlmostAllDatatype() throws Exception {
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,18)\n" +
                "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,1..10);" +
                "pt = db.createPartitionedTable(t,`pt,`cint);" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://testDecimal","pt",
                false,false,null,1000,1,20,"cint",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`cint"});
        mtw.insert(new BasicBoolean(true),new BasicByte((byte) 'D'),new BasicShort((short) 21),new BasicInt(4),new BasicLong(55),new BasicDate(LocalDate.now()),
                new BasicMonth(2012,Month.AUGUST),new BasicTime(LocalTime.of(13,7,55)),new BasicMinute(LocalTime.of(11,40,53)),
                new BasicSecond(895),new BasicDateTime(LocalDateTime.MIN),new BasicTimestamp(LocalDateTime.MAX),new BasicNanoTime(LocalTime.of(22,14,58,43847)),
                new BasicNanoTimestamp(LocalDateTime.of(2013,11,4,13,34,49,424246)),new BasicFloat((float) 12.71),new BasicDouble(0.783),
                new BasicString("MongoDB"),new BasicDateHour(LocalDateTime.now()),new BasicDecimal32(98,2),new BasicDecimal64(188.68,4),new BasicDecimal128("188.68",18));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(3,ua.rows());
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from pt where cminute=08:12m").rows());
        assertEquals(1,conn.run("select * from pt where cminute=11:40m").rows());
        assertEquals("188.6800", ((BasicDecimal64Vector)(ua.getColumn("cdecimal64"))).get(1).getString());
        assertEquals("188.680000000000000000", ((BasicDecimal128Vector)(ua.getColumn("cdecimal128"))).get(1).getString());
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_mtw_upsert_DimensionTable_AlmostAllDatatype() throws Exception {
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,18)\n" +
                "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,1..10);" +
                "pt = db.createTable(t,`pt);" +
                "pt.append!(t)";
        conn.run(script);
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST,PORT,"admin","123456","dfs://testDecimal","pt",
                false,false,null,1000,1,1,"cint",null,
                MultithreadedTableWriter.Mode.M_Upsert,new String[]{"ignoreNull=true","keyColNames=`cint"});
        mtw.insert(new BasicBoolean(true),new BasicByte((byte) 'D'),new BasicShort((short) 21),new BasicInt(4),new BasicLong(55),new BasicDate(LocalDate.now()),
                new BasicMonth(2012,Month.AUGUST),new BasicTime(LocalTime.of(13,7,55)),new BasicMinute(LocalTime.of(11,40,53)),
                new BasicSecond(895),new BasicDateTime(LocalDateTime.MIN),new BasicTimestamp(LocalDateTime.MAX),new BasicNanoTime(LocalTime.of(22,14,58,43847)),
                new BasicNanoTimestamp(LocalDateTime.of(2013,11,4,13,34,49,424246)),new BasicFloat((float) 12.71),new BasicDouble(0.783),
                new BasicString("MongoDB"),new BasicDateHour(LocalDateTime.now()),new BasicDecimal32(98,2),new BasicDecimal64(188.68,4),new BasicDecimal128("188.68",18));
        mtw.waitForThreadCompletion();
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(3,ua.rows());
        System.out.println(ua.getString());
        assertEquals(0,conn.run("select * from pt where cminute=08:12m").rows());
        assertEquals(1,conn.run("select * from pt where cminute=11:40m").rows());
        assertEquals("188.6800", ((BasicDecimal64Vector)(ua.getColumn("cdecimal64"))).get(1).getString());
        assertEquals("188.680000000000000000", ((BasicDecimal128Vector)(ua.getColumn("cdecimal128"))).get(1).getString());
        conn.run("clear!(t);");
    }

    @Test(timeout = 120000)
    public void test_ThreadStatus_toString()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`delta,[INT,DOUBLE]);\n share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                    "", "t1", false, false,null,1, 1000,
                    1, "int",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});
        mutithreadTableWriter_.insert(new BasicInt(0),new BasicDouble(12.88));
        //conn.run("sleep(500)");
        System.out.println(mutithreadTableWriter_.getStatus().toString());
        //assertEquals(1,mutithreadTableWriter_.getStatus().unsentRows);
        assertEquals(0,mutithreadTableWriter_.getStatus().sendFailedRows);
        assertEquals(0,mutithreadTableWriter_.getStatus().sentRows);
        for (int i=1;i<=100;i++) {
            mutithreadTableWriter_.insert(new BasicInt(1),new BasicDouble(12.88));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        System.out.println(mutithreadTableWriter_.getStatus().toString());
        //assertEquals(101,mutithreadTableWriter_.getStatus().unsentRows);
        assertEquals(0,mutithreadTableWriter_.getStatus().sendFailedRows);
        assertEquals(0,mutithreadTableWriter_.getStatus().sentRows);
        mutithreadTableWriter_.waitForThreadCompletion();
        System.out.println(mutithreadTableWriter_.getStatus().toString());
        assertEquals(0,mutithreadTableWriter_.getStatus().unsentRows);
        assertEquals(0,mutithreadTableWriter_.getStatus().sendFailedRows);
        //assertEquals(101,mutithreadTableWriter_.getStatus().sentRows);
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public  void test_insert_empty_arrayVector_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv,[INT,INT[]]);\n" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<1000;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        for (int i=0;i<1000;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,null);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(2000,bt.rows());
        for (int i=0;i<2000;i++) {
            assertEquals("[]", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
    }

    @Test(timeout = 120000)
    public void test_insert_arrayVector_different_length_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv`arrayv1`arrayv2," +
                "[INT,INT[],BOOL[],BOOL[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{1},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{1,2},new Boolean[]{true,null,false},new Boolean[]{true});
        pErrorInfo=mutithreadTableWriter_.insert( 1,new Integer[]{1,null,1},new Boolean[]{true,null,false},new Boolean[]{true});
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
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

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_int_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,INT[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Integer[]{1, i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals(1, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(i, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_char_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,CHAR[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Byte[]{'a','3'});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals("['a','3']", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_bool_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,BOOL[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<10;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,new Boolean[]{true,false});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(10,bt.rows());
        for (int i=0;i<10;i++) {
            assertEquals("true",((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0).getString());
            assertEquals("false", ((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_long_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,LONG[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1024;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Long[]{1l, Long.valueOf(i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(1l, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Long.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_short_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,SHORT[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=210;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Short[]{1,i});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Short.valueOf("1"), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Short.valueOf(""+i+""), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_float_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,FLOAT[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Float[]{0.0f,Float.valueOf(i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(0.0f, ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Float.valueOf(i), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_double_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv," +
                "[INT,DOUBLE[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new Double[]{Double.valueOf(0),Double.valueOf(i-10)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(Double.valueOf(0), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(0)).getNumber());
            assertEquals(Double.valueOf(i-10), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv")).getVectorValue(i).get(1)).getNumber());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_date_month_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`arrayv1`arrayv2," +
                "[INT,DATE[],MONTH[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=10240;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1, new LocalDate[]{LocalDate.of(1,1,1)},
                    new LocalDate[]{LocalDate.of(2021,1,1)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(LocalDate.of(1,1,1), ((Scalar)((BasicArrayVector)bt.getColumn("arrayv1")).getVectorValue(i).get(0)).getTemporal());
            assertEquals("2021.01M", ((BasicArrayVector)bt.getColumn("arrayv2")).getVectorValue(i).get(0).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_time_minute_second_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`time`minute`second," +
                "[INT,TIME[],MINUTE[],SECOND[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1024;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(
                    1,
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,342)},
                    new LocalTime[]{LocalTime.of(1,1,1,1)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals("01:01:01.000", ((BasicArrayVector)bt.getColumn("time")).getVectorValue(i).get(0).getString());
            assertEquals("01:01m", ((BasicArrayVector)bt.getColumn("minute")).getVectorValue(i).get(0).getString());
            assertEquals("01:01:01", ((BasicArrayVector)bt.getColumn("second")).getVectorValue(i).get(0).getString());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_datetime_timestamp_nanotime_nanotimstamp_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`datetime`timestamp`nanotime`nanotimstamp," +
                "[INT,DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(
                    1,
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i),LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i)},
                    new LocalTime[]{LocalTime.of(1,1,1,45364654+i),LocalTime.of(1,1,1,45364654+i),LocalTime.of(1,1,1,45364654+i)},
                    new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654+i),
                            LocalDateTime.of(2022,2,1,1,1,2,45364654+i)});
            assertEquals("code= info=",pErrorInfo.toString());
        }

        // System.out.println(LocalDateTime.of(2022,2,1,1,1,2,033));
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        System.out.println(((Scalar)((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(0).get(0)).getTemporal());

        for (int i=0;i<time;i++) {
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2), ((Scalar)((BasicArrayVector)bt.getColumn("datetime")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45000000), ((Scalar)((BasicArrayVector)bt.getColumn("timestamp")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalTime.of(1,1,1,45364654+i), ((Scalar)((BasicArrayVector)bt.getColumn("nanotime")).getVectorValue(i).get(0)).getTemporal());
            assertEquals(LocalDateTime.of(2022,2,1,1,1,2,45364654+i), ((Scalar)((BasicArrayVector)bt.getColumn("nanotimstamp")).getVectorValue(i).get(0)).getTemporal());
        }
    }

    @Test(timeout = 120000)
    public  void test_insert_arrayVector_otherType_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`uuid`int128`ipaddr," +
                "[INT,UUID[],INT128[],IPADDR[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");
        BasicUuidVector bv= (BasicUuidVector) conn.run("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87',,'5d212a78-cc48-e3b1-4235-b4d91473ee87'])");
        BasicInt128Vector iv= (BasicInt128Vector) conn.run("int128(['e1671797c52e15f763380b45e841ec32',,'e1671797c52e15f763380b45e841ec32'])");
        BasicIPAddrVector ipv= (BasicIPAddrVector) conn.run("ipaddr(['192.168.1.13',,'192.168.1.13'])");
        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new String[]{"5d212a78-cc48-e3b1-4235-b4d91473ee87",null,"5d212a78-cc48-e3b1-4235-b4d91473ee87"},new String[]{"e1671797c52e15f763380b45e841ec32",null,"e1671797c52e15f763380b45e841ec32"}
                    ,new String[]{"192.168.1.13",null,"192.168.1.13"});
            assertEquals("code= info=",pErrorInfo.toString());
        }

        // System.out.println(LocalDateTime.of(2022,2,1,1,1,2,033));
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from pt;");
        assertEquals(time,bt.rows());
        for (int i=0;i<time;i++) {
            assertEquals(bv.getString(), ((BasicArrayVector)(bt.getColumn("uuid"))).getVectorValue(i).getString());
            assertEquals(iv.getString(), ((BasicArrayVector)(bt.getColumn("int128"))).getVectorValue(i).getString());
            assertEquals(ipv.getString(), ((BasicArrayVector)(bt.getColumn("ipaddr"))).getVectorValue(i).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal32[]{new BasicDecimal32(1,0),new BasicDecimal32(1.00,0),new BasicDecimal32(3.0001,0),new BasicDecimal32("99999.99999999999",0)},new BasicDecimal32[]{new BasicDecimal32(1,3),new BasicDecimal32(1.00,3),new BasicDecimal32(3.0001,3),new BasicDecimal32("99999.99999999999",3)},new BasicDecimal64[]{new BasicDecimal64(1,0),new BasicDecimal64(1.00,0),new BasicDecimal64(3.0001,0),new BasicDecimal64("99999.99999999999",0)},new BasicDecimal64[]{new BasicDecimal64(1,4),new BasicDecimal64(1.00,4),new BasicDecimal64(3.0001,4),new BasicDecimal64("99999.99999999999",4)},new BasicDecimal64[]{new BasicDecimal64(1,8),new BasicDecimal64(1.00,8),new BasicDecimal64(3.0001,8),new BasicDecimal64("99999.99999999999",8)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        BasicDecimal32Vector decv1= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal32Vector decv2= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
        BasicDecimal64Vector decv3= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal64Vector decv4= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
        BasicDecimal64Vector decv5= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal_to_partition_table_compress_lz4()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal32[]{new BasicDecimal32(1,0),new BasicDecimal32(1.00,0),new BasicDecimal32(3.0001,0),new BasicDecimal32("99999.99999999999",0)},new BasicDecimal32[]{new BasicDecimal32(1,3),new BasicDecimal32(1.00,3),new BasicDecimal32(3.0001,3),new BasicDecimal32("99999.99999999999",3)},new BasicDecimal64[]{new BasicDecimal64(1,0),new BasicDecimal64(1.00,0),new BasicDecimal64(3.0001,0),new BasicDecimal64("99999.99999999999",0)},new BasicDecimal64[]{new BasicDecimal64(1,4),new BasicDecimal64(1.00,4),new BasicDecimal64(3.0001,4),new BasicDecimal64("99999.99999999999",4)},new BasicDecimal64[]{new BasicDecimal64(1,8),new BasicDecimal64(1.00,8),new BasicDecimal64(3.0001,8),new BasicDecimal64("99999.99999999999",8)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        BasicDecimal32Vector decv1= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal32Vector decv2= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
        BasicDecimal64Vector decv3= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal64Vector decv4= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
        BasicDecimal64Vector decv5= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
//    @Test(timeout = 120000) not support
    public  void test_insert_arrayVector_decimal_to_partition_table_compress_delta()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(1)[],DECIMAL64(8)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal32[]{new BasicDecimal32(1,0),new BasicDecimal32(1.00,0),new BasicDecimal32(3.0001,0),new BasicDecimal32(99999.99999999999,0)},new BasicDecimal32[]{new BasicDecimal32(1,3),new BasicDecimal32(1.00,3),new BasicDecimal32(3.0001,3),new BasicDecimal32(99999.99999999999,3)},new BasicDecimal64[]{new BasicDecimal64(1,0),new BasicDecimal64(1.00,0),new BasicDecimal64(3.0001,0),new BasicDecimal64(99999.99999999999,0)},new BasicDecimal64[]{new BasicDecimal64(1,4),new BasicDecimal64(1.00,4),new BasicDecimal64(3.0001,4),new BasicDecimal64(99999.99999999999,4)},new BasicDecimal64[]{new BasicDecimal64(1,8),new BasicDecimal64(1.00,8),new BasicDecimal64(3.0001,8),new BasicDecimal64(99999.99999999999,8)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        BasicDecimal32Vector decv1= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal32Vector decv2= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
        BasicDecimal64Vector decv3= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal64Vector decv4= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
        BasicDecimal64Vector decv5= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal_to_memory_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "share table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]) as tt1 ;" );
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "tt1", false, false,null,1, 1,
                1, "int");

        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal32[]{new BasicDecimal32(1,0),new BasicDecimal32(1.00,0),new BasicDecimal32(3.0001,0),new BasicDecimal32("99999.99999999999",0)},new BasicDecimal32[]{new BasicDecimal32(1,3),new BasicDecimal32(1.00,3),new BasicDecimal32(3.0001,3),new BasicDecimal32("99999.99999999999",3)},new BasicDecimal64[]{new BasicDecimal64(1,0),new BasicDecimal64(1.00,0),new BasicDecimal64(3.0001,0),new BasicDecimal64("99999.99999999999",0)},new BasicDecimal64[]{new BasicDecimal64(1,4),new BasicDecimal64(1.00,4),new BasicDecimal64(3.0001,4),new BasicDecimal64("99999.99999999999",4)},new BasicDecimal64[]{new BasicDecimal64(1,8),new BasicDecimal64(1.00,8),new BasicDecimal64(3.0001,8),new BasicDecimal64("99999.99999999999",8)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from tt1;");
        assertEquals(time,bt1.rows());
        BasicDecimal32Vector decv1= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal32Vector decv2= (BasicDecimal32Vector) conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
        BasicDecimal64Vector decv3= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal64Vector decv4= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
        BasicDecimal64Vector decv5= (BasicDecimal64Vector) conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
        conn.run("undef(`tt1,SHARED)");
        conn.close();
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal128_to_partition_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int");

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal128[]{new BasicDecimal128("1",0),new BasicDecimal128("1.00",0),new BasicDecimal128("3.0001",0),new BasicDecimal128("99999.99999999999",0)},new BasicDecimal128[]{new BasicDecimal128("1",1),new BasicDecimal128("1.00",1),new BasicDecimal128("3.0001",1),new BasicDecimal128("99999.99999999999",1)},new BasicDecimal128[]{new BasicDecimal128("1",10),new BasicDecimal128("1.00",10),new BasicDecimal128("3.0001",10),new BasicDecimal128("99999.99999999999",10)},new BasicDecimal128[]{new BasicDecimal128("1",18),new BasicDecimal128("1.00",18),new BasicDecimal128("3.0001",18),new BasicDecimal128("99999.99999999999",18)},new BasicDecimal128[]{new BasicDecimal128("1",30),new BasicDecimal128("1.00",30),new BasicDecimal128("3.0001",30),new BasicDecimal128("99999.99999999999",30)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        System.out.println(bt1.getColumn("col3").getString());
        BasicDecimal128Vector decv1= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal128Vector decv2= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
        BasicDecimal128Vector decv3= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
        String decv4= "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
        String decv5= "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal128_to_partition_table_compress_lz4()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),0),new BasicDecimal128(String.valueOf(1.00),0),new BasicDecimal128(String.valueOf(3.0001),0),new BasicDecimal128(String.valueOf(99999.99999999999),0)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),1),new BasicDecimal128(String.valueOf(1.00),1),new BasicDecimal128(String.valueOf(3.0001),1),new BasicDecimal128(String.valueOf(99999.99999999999),1)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),10),new BasicDecimal128(String.valueOf(1.00),10),new BasicDecimal128(String.valueOf(3.0001),10),new BasicDecimal128(String.valueOf(99999.99999999999),10)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),18),new BasicDecimal128(String.valueOf(1.00),18),new BasicDecimal128(String.valueOf(3.0001),18),new BasicDecimal128(String.valueOf(99999.99999999999),18)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),30),new BasicDecimal128(String.valueOf(1.00),30),new BasicDecimal128(String.valueOf(3.0001),30),new BasicDecimal128(String.valueOf(99999.99999999999),30)});

            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        BasicDecimal128Vector decv1= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal128Vector decv2= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
        BasicDecimal128Vector decv3= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
        String decv4= "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
        String decv5= "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
    //    @Test(timeout = 120000) not support
    public  void test_insert_arrayVector_decimal128_to_partition_table_compress_delta()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n"+
                "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_arrayVector_in_partition_table", "pt", false, false,null,1, 1,
                1, "int",new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});

        for (int i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),0),new BasicDecimal128(String.valueOf(1.00),0),new BasicDecimal128(String.valueOf(3.0001),0),new BasicDecimal128(String.valueOf(99999.99999999999),0)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),1),new BasicDecimal128(String.valueOf(1.00),1),new BasicDecimal128(String.valueOf(3.0001),1),new BasicDecimal128(String.valueOf(99999.99999999999),1)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),10),new BasicDecimal128(String.valueOf(1.00),10),new BasicDecimal128(String.valueOf(3.0001),10),new BasicDecimal128(String.valueOf(99999.99999999999),10)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),18),new BasicDecimal128(String.valueOf(1.00),18),new BasicDecimal128(String.valueOf(3.0001),18),new BasicDecimal128(String.valueOf(99999.99999999999),18)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),30),new BasicDecimal128(String.valueOf(1.00),30),new BasicDecimal128(String.valueOf(3.0001),30),new BasicDecimal128(String.valueOf(99999.99999999999),30)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
        assertEquals(time,bt1.rows());
        BasicDecimal128Vector decv1= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal128Vector decv2= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
        BasicDecimal128Vector decv3= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
        BasicDecimal128Vector decv4= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],18)");
        BasicDecimal128Vector decv5= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],30)");

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
    }
    @Test(timeout = 120000)
    public  void test_insert_arrayVector_decimal128_to_memory_table()throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "share table(1000:0, `int`col0`col1`col2`col3`col4," +
                        "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]) as tt1 ;" );
        int time=1048;
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "tt1", false, false,null,1, 1,
                1, "int");

        for (short i=0;i<time;i++) {
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1,new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),0),new BasicDecimal128(String.valueOf(1.00),0),new BasicDecimal128(String.valueOf(3.0001),0),new BasicDecimal128(String.valueOf(99999.99999999999),0)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),1),new BasicDecimal128(String.valueOf(1.00),1),new BasicDecimal128(String.valueOf(3.0001),1),new BasicDecimal128(String.valueOf(99999.99999999999),1)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),10),new BasicDecimal128(String.valueOf(1.00),10),new BasicDecimal128(String.valueOf(3.0001),10),new BasicDecimal128(String.valueOf(99999.99999999999),10)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),18),new BasicDecimal128(String.valueOf(1.00),18),new BasicDecimal128(String.valueOf(3.0001),18),new BasicDecimal128(String.valueOf(99999.99999999999),18)},new BasicDecimal128[]{new BasicDecimal128(String.valueOf(1),30),new BasicDecimal128(String.valueOf(1.00),30),new BasicDecimal128(String.valueOf(3.0001),30),new BasicDecimal128(String.valueOf(99999.99999999999),30)});
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt1= (BasicTable)conn.run("select * from tt1;");
        assertEquals(time,bt1.rows());
        BasicDecimal128Vector decv1= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
        BasicDecimal128Vector decv2= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
        BasicDecimal128Vector decv3= (BasicDecimal128Vector) conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
        String decv4= "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
        String decv5= "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

        for (int i=0;i<time;i++) {
            assertEquals(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getVectorValue(i).getString());
            assertEquals(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getVectorValue(i).getString());
            assertEquals(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getVectorValue(i).getString());
            assertEquals(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getVectorValue(i).getString());
            assertEquals(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getVectorValue(i).getString());
        }
        conn.run("undef(`tt1,SHARED)");
        conn.close();
    }

    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_batchSize_greater_than_Number_of_inserts_no_waitForThreadCompletion()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());

        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 200000, 0, 5, "id", null, callbackHandler);

        for (int i = 0; i < 1000; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        //mtw.waitForThreadCompletion();
        conn.run("sleep(5000)");
        System.out.println("callback rows");


        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        System.out.println("ex.rows()" + ex.rows());
        assertEquals(ex.rows(), 1000);
        conn.close();
    }
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_batchSize_greater_than_Number_of_inserts_waitForThreadCompletion()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());

        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 200000, 0, 5, "id", null, callbackHandler);

        for (int i = 0; i < 1000; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();
        //conn.run("sleep(2000)");
        System.out.println("callback rows");


        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        System.out.println("ex.rows()" + ex.rows());
        assertEquals(ex.rows(), 1000);
        conn.close();
    }
    @Test(timeout = 120000)
    public void test_insert_integer_to_floating() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("t = table(100:0, `float`double," +
                "[FLOAT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "float");
        mtw.insert( (int)1, (int)11);
        mtw.insert( (long)2, (long)22);
        mtw.insert( (byte)3, (byte)33);
        mtw.insert( (short)4, (short)44);
        mtw.insert( (float)5.55, (float)55);
        mtw.insert( (double)6.66, (double)66.66);
        mtw.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(6, bt.rows());
        System.out.println(bt.getString());
        assertEquals("[1,2,3,4,5.55000019,6.65999985]", bt.getColumn("float").getString());
        assertEquals("[11,22,33,44,55,66.66]", bt.getColumn("double").getString());
        conn.run("undef(`t1,SHARED)");
    }
    @Test
    public void test_MultithreadedTableWriter_illegal_string() throws Exception {
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10,,'TSDB');t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]);db.createPartitionedTable(t,'t1', 'id',,'id')");
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://db1", "t1", false, false, null, 1, 1,
                1, "id");
        mtw.insert( new BasicInt(1), new BasicString("string1AM\000ZN/n0"),new BasicString("symbol1AM\0\0ZN"),new BasicString("blob1AM\0ZN",true));
        mtw.insert( new BasicInt(2), new BasicString("\000\00PL\0"),new BasicString("\0symbol1AP\0PL\0"),new BasicString("\0blob1AM\0ZN",true));
        mtw.insert( new BasicInt(3), new BasicString("string1AM\0"),new BasicString("symbol1AM\0"),new BasicString("blob1AMZN\0",true));
        mtw.waitForThreadCompletion();
        BasicTable table2 = (BasicTable) conn.run("select * from loadTable(\"dfs://db1\", `t1) ;\n");
        System.out.println(table2.getString());
        assertEquals("string1AM", table2.getColumn(1).get(0).getString());
        assertEquals("", table2.getColumn(1).get(1).getString());
        assertEquals("string1AM", table2.getColumn(1).get(2).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(0).getString());
        assertEquals("", table2.getColumn(2).get(1).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(2).getString());
        assertEquals("blob1AM\0ZN", table2.getColumn(3).get(0).getString());
        assertEquals("\0blob1AM\0ZN", table2.getColumn(3).get(1).getString());
        assertEquals("blob1AMZN\0", table2.getColumn(3).get(2).getString());
    }
    @Test
    public void test_MultithreadedTableWriter_illegal_string_1() throws Exception {
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10,,'TSDB');t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]);db.createPartitionedTable(t,'t1', 'id',,'id')");
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://db1", "t1", false, false, null, 1, 1,
                1, "id");
        mtw.insert( 1, "string1AM\000ZN/n0","symbol1AM\0\0ZN","blob1AM\0ZN");
        mtw.insert( 2, "\000\00PL\0","\0symbol1AP\0PL\0","\0blob1AM\0ZN");
        mtw.insert( 3, "string1AM\0","symbol1AM\0","blob1AMZN\0");
        mtw.waitForThreadCompletion();
        BasicTable table2 = (BasicTable) conn.run("select * from loadTable(\"dfs://db1\", `t1) ;\n");
        System.out.println(table2.getString());
        assertEquals("string1AM", table2.getColumn(1).get(0).getString());
        assertEquals("", table2.getColumn(1).get(1).getString());
        assertEquals("string1AM", table2.getColumn(1).get(2).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(0).getString());
        assertEquals("", table2.getColumn(2).get(1).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(2).getString());
        assertEquals("blob1AM\0ZN", table2.getColumn(3).get(0).getString());
        assertEquals("\0blob1AM\0ZN", table2.getColumn(3).get(1).getString());
        assertEquals("blob1AMZN\0", table2.getColumn(3).get(2).getString());
    }
    @Test(timeout = 200000)
    public  void test_MultithreadedTableWriter_write_block()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                synchronized (callback) {
                    try {
                        callback.getColumn(0).Append(idV);
                        callback.getColumn(1).Append(successV);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 50000, 1, 1, "id", null, callbackHandler);

        for (int i = 0; i < 65537; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();

        System.out.println("callback rows"+callback.rows());
        //assertEquals(1000000000, callback.rows()-1);

        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn.upload(map);
        BasicTable act = (BasicTable) conn.run("select * from testUpload where issuccess = true order by id");
        BasicTable act1 = (BasicTable) conn.run("select * from testUpload order by id");

        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
        }
        conn.close();
    }

    //@Test
    public  void test_MultithreadedTableWriter_write_block_1()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);
        int count = 0;
        Callback callbackHandler = new Callback(){
            int count = 0;
            public void writeCompletion(Table callbackTable) {

                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                synchronized (callback) {
                    try {
                        callback.getColumn(0).Append(idV);
                        callback.getColumn(1).Append(successV);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                count++;
                System.out.println("COUNT:"+ count);
//                for (int i = 0; i < successV.rows(); i++){
//                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
//                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                true, null, 1, 100, 1, "id", null, callbackHandler);

        conn1.run("sleep(2000)");
//        BasicTable re = (BasicTable) conn1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
//        for (int i = 0; i < re.rows(); i++) {
//            conn1.run("try{stopDataNode('"+HOST+":"+re.getColumn(0).get(i)+"')}catch(ex){}");
//            conn1.run("sleep(2000)");
//        }
        for (int i = 0; i < 262139; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        System.out.println(mtw.getStatus());
        System.out.println("----------------------");

        conn1.run("sleep(2000)");
//        for (int i = 0; i < re.rows(); i++) {
//            conn1.run("try{startDataNode('"+HOST+":"+re.getColumn(0).get(i)+"')}catch(ex){}");
//            //conn1.run("sleep(2000)");
//        }
        conn1.run("sleep(2000)");
        mtw.waitForThreadCompletion();
        System.out.println(mtw.getStatus());

        System.out.println("callback rows"+callback.rows());
        //assertEquals(1000000000, callback.rows()-1);

        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.upload(map);
        BasicTable act = (BasicTable) conn.run("select * from testUpload where issuccess = true order by id");
        BasicTable act1 = (BasicTable) conn.run("select * from testUpload order by id");

        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
        }
        conn.close();
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_enableActualSendTime_true_column_not_NANOTIMESTAMP() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`nanotimestamp1," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIME]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",true);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code=A1 info=Data type error: DT_NANOTIME,should be NANOTIMESTAMP.",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_enableActualSendTime_true_column_not_exist() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",true);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code=A2 info=Column counts don't match.",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_enableActualSendTime_true_column_NANOTIMESTAMP() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`enableActualSendTime," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",true);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp = formatter.format(now);
        System.out.println("当前时间戳：" + formattedTimestamp);
        mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        mutithreadTableWriter_.waitForThreadCompletion();
        LocalDateTime now1 = LocalDateTime.now();
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp1 = formatter1.format(now1);
        System.out.println("当前时间戳：" + formattedTimestamp1);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        BasicNanoTimestamp sendTime = (BasicNanoTimestamp)bt.getColumn("enableActualSendTime").get(0);
        assertEquals(true, sendTime.getNanoTimestamp().getNano() >= now.getNano());
        assertEquals(true, sendTime.getNanoTimestamp().getNano() <= now1.getNano());
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_enableActualSendTime_false_column_NANOTIMESTAMP() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`nanotimestamp1," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIMESTAMP]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",false);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code=A2 info=Column counts don't match.",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        conn.run("undef(`t1,SHARED)");
    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_enableActualSendTime_false() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",false);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        for (int i = 0; i < bt.columns(); i++) {
            System.out.println(bt.getColumn(i).get(0).toString());
        }
        conn.run("undef(`t1,SHARED)");
    }
    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_streamTable_enableActualSendTime_true() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`enableActualSendTime," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIMESTAMP]);\n" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "bool",true);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp = formatter.format(now);
        System.out.println("当前时间戳：" + formattedTimestamp);
        mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1);
        mutithreadTableWriter_.waitForThreadCompletion();
        LocalDateTime now1 = LocalDateTime.now();
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp1 = formatter1.format(now1);
        System.out.println("当前时间戳：" + formattedTimestamp1);
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        BasicNanoTimestamp sendTime = (BasicNanoTimestamp)bt.getColumn("enableActualSendTime").get(0);
        assertEquals(true, sendTime.getNanoTimestamp().getNano() >= now.getNano());
        assertEquals(true, sendTime.getNanoTimestamp().getNano() <= now1.getNano());
        conn.run("undef(`t1,SHARED)");
    }
    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_dfs_enableActualSendTime_true() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("pt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`enableActualSendTime," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIMESTAMP]);\n" +
                "if(existsDatabase(\"dfs://enableActualSendTime\")) dropDatabase(\"dfs://enableActualSendTime\");\n" +
                "db = database(\"dfs://enableActualSendTime\", VALUE, 1..1000, , 'TSDB');\n" +
                "pt = db.createPartitionedTable(pt,`pt, `id , , `id, );");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://enableActualSendTime", "pt", false, false, null, 1, 1,
                1, "id",true);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp = formatter.format(now);
        System.out.println("now当前时间戳：" + formattedTimestamp);
        mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1);
        mutithreadTableWriter_.waitForThreadCompletion();
        LocalDateTime now1 = LocalDateTime.now();
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp1 = formatter1.format(now1);
        System.out.println("now1当前时间戳：" + formattedTimestamp1);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://enableActualSendTime\",`pt);");
        assertEquals(1, bt.rows());
        BasicNanoTimestamp sendTime = (BasicNanoTimestamp)bt.getColumn("enableActualSendTime").get(0);
        assertEquals(true, sendTime.getNanoTimestamp().getNano() > now.getNano());
        assertEquals(true, sendTime.getNanoTimestamp().getNano() < now1.getNano());
    }

    @Test(timeout = 20000)
    public void test_MultithreadedTableWriter_Dimension_enableActualSendTime_true() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("pt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`enableActualSendTime," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,NANOTIMESTAMP]);\n" +
                "if(existsDatabase(\"dfs://enableActualSendTime\")) dropDatabase(\"dfs://enableActualSendTime\");\n" +
                "db = database(\"dfs://enableActualSendTime\", VALUE, 1..1000, , 'TSDB');\n" +
                "pt = db.createTable(pt,`pt, ,`id );");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://enableActualSendTime", "pt", false, false, null, 1, 1,
                1, "id",true);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp = formatter.format(now);
        System.out.println("当前时间戳：" + formattedTimestamp);
        mutithreadTableWriter_.insert( null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1);
        mutithreadTableWriter_.waitForThreadCompletion();
        LocalDateTime now1 = LocalDateTime.now();
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSSSSSSS");
        String formattedTimestamp1 = formatter1.format(now1);
        System.out.println("当前时间戳：" + formattedTimestamp1);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://enableActualSendTime\",`pt);");
        assertEquals(1, bt.rows());
        BasicNanoTimestamp sendTime = (BasicNanoTimestamp)bt.getColumn("enableActualSendTime").get(0);
        assertEquals(true, sendTime.getNanoTimestamp().getNano() > now.getNano());
        assertEquals(true, sendTime.getNanoTimestamp().getNano() < now1.getNano());
    }

    @Test(timeout = 120000)//server support setStreamTableTimestamp
    public void test_MultithreadedTableWriter_enableActualSendTime_setStreamTableTimestamp() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `id`enableActualSendTime`streamTableTimestamp," +
                "[INT,NANOTIMESTAMP,NANOTIMESTAMP]);" +
                "setStreamTableTimestamp(t, `streamTableTimestamp);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id",true);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(  1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(1, bt.rows());
        System.out.println(bt.getString());
        Entity re = conn.run("t[`enableActualSendTime][0]<t[`streamTableTimestamp][0]");
        assertEquals("true", re.getString());
        conn.run("undef(`t1,SHARED)");
    }

    //@Test(timeout = 120000)//server not support setStreamTableTimestamp
    public void test_MultithreadedTableWriter_enableActualSendTime_not_support_setStreamTableTimestamp() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, 8878, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `id`enableActualSendTime`streamTableTimestamp," +
                "[INT,NANOTIMESTAMP,NANOTIMESTAMP]);" +
                "setStreamTableTimestamp(t, `streamTableTimestamp);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, 8878, "admin", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id",true);

        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(  1);
        assertEquals("code=A2 info=Column counts don't match.",pErrorInfo.toString());
//        mutithreadTableWriter_.waitForThreadCompletion();
//        BasicTable bt = (BasicTable) conn.run("select * from t1;");
//        assertEquals(1, bt.rows());
        conn.run("undef(`t1,SHARED)");
    }

    //@Test(timeout = 120000)
    public void test_MultithreadedTableWriter_allDataType_null() throws Exception {
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
                " pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://empty_table", "pt", false, false, null, 1, 1, 1, "stringv");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( cols);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        conn.close();
    }
    //@Test(timeout = 120000)
    public void test_MultithreadedTableWriter_allDataType_array_null() throws Exception {
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
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://empty_table", "pt", false, false, null, 1, 1, 1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( cols);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        conn.close();
    }


//    @Test(timeout = 120000)
//    public void test_insert_haStreamTable() throws Exception {
//        conn.run("haTableName='ha_stream'; " +
//                "try{ dropStreamTable(haTableName); }catch(ex){}\n " +
//                "t = table(1:0, `tradeDate`tradePrice`vwap`volume`valueTrade, [DATETIME, DOUBLE, DOUBLE, INT, DOUBLE]);" +
//                "haStreamTable(3,t,haTableName,1000000);");
//        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
//                "", "ha_stream", false, true, ipports, 10, 1,
//                1, "tradeDate");
//        List<List<Entity>> tb = new ArrayList<>();
//        for (int i = 0; i < 10000; i++) {
//            mutithreadTableWriter_.insert( LocalDateTime.of(2012, 1, i % 10 + 1, 1, i%10), i + 0.1, i + 0.1, i % 10, i + 0.1);
//        }
//        Thread.sleep(2000);
//        BasicTable ex = (BasicTable) conn.run("select * from ha_stream order by tradeDate,tradePrice,vwap,volume,valueTrade;");
//        assertEquals(10000,ex.rows());
//        mutithreadTableWriter_.waitForThreadCompletion();
//    }

    //@Test(timeout = 120000)
    public void test_mtw_enableHighAvailability_true() throws Exception {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,8802,"admin","123456");
        conn1.run("share table(10:0,`id`price`val,[INT,DOUBLE,INT]) as table1;\n");

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,8803,"admin","123456");
        conn2.run("share table(10:0,`id`price`val,[INT,DOUBLE,INT]) as table1;\n");

        System.out.println("节点断掉");
        Thread.sleep(1000);
        MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "", "table1",
                false, true, ipports, 1000, 0.001f, 1, "id");
        //检查线程连接情况
        for(int i = 0;i <1000;i++) {
            int tmp =5;
            mtw1.insert(tmp, (double) tmp, 1);
            Thread.sleep(100);
            System.out.println("循环次数："+i);
        }
        mtw1.waitForThreadCompletion();
        //BasicInt writedData1 = (BasicInt) conn1.run("(exec count(*) from table1 where val = 1)[0]");
        BasicInt writedData2 = (BasicInt) conn2.run("(exec count(*) from table1 where val = 1)[0]");
        //System.out.println(writedData1);
        System.out.println(writedData2);
    }

    //@Test//AJ-856
    public void test_mtw_enableHighAvailability_true_append_dfs() throws Exception {
        DBConnection conn1 = new DBConnection();
        conn1.connect("192.168.0.69",8802,"admin","123456");
        conn1.run("t1= table(10:0,`id`price`val,[INT,DOUBLE,INT])\n" +
                "dbPath = \"dfs://TSDB_mtw\"\n" +
                "if(existsDatabase(dbPath)){dropDatabase(dbPath)}\n" +
                "db = database(dbPath, VALUE, 0..1000, engine='TSDB')\n" +
                "pt=db.createPartitionedTable(t1, \"pt\",`id, , [`id])");

        DBConnection conn2 = new DBConnection();
        conn2.connect("192.168.0.69",8803,"admin","123456");
        DBConnection conn3 = new DBConnection();
        conn3.connect("192.168.0.69",8800,"admin","123456");

        System.out.println("节点断掉");
        Thread.sleep(1000);
        MultithreadedTableWriter mtw1 = new MultithreadedTableWriter("192.168.0.69", 8802, "admin", "123456", "dfs://TSDB_mtw", "pt",
                false, true, new String[]{"192.168.0.69:8802","192.168.0.69:8803"}, 1, 0.001f, 1, "id");
        //检查线程连接情况
        for(int i = 0;i <1000;i++) {
            int tmp =5;
            mtw1.insert(tmp, (double) tmp, 1);
            Thread.sleep(100);
            System.out.println("循环次数："+i);
        }
        mtw1.waitForThreadCompletion();
        BasicInt writedData2 = (BasicInt) conn3.run("(exec count(*) from table1 where val = 1)[0]");
        System.out.println(writedData2);
    }

//    @Test not support
//    public void Test_MultithreadedTableWriter_iotAnyVector() throws Exception {
//        String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
//                "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2025.01.01), engine='TSDB'\n" +
//                "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
//                "     deviceId INT,\n" +
//                "     timestamp TIMESTAMP,\n" +
//                "     location SYMBOL,\n" +
//                "     value IOTANY,\n" +
//                " )\n" +
//                "partitioned by deviceId, timestamp,\n" +
//                "sortColumns=[`deviceId, `location, `timestamp],\n" +
//                "latestKeyCache=true;\n" +
//                "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n" ;
//        conn.run(script);
//        BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
//        BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
//        BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
//        BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
//        BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
//        BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
//        BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
//        BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
//        BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value);\n select * from t limit 1");
//        System.out.println(bt8.getString());
//
//        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://testIOT_allDateType", "pt", false, false, null, 1, 1, 1, "deviceId");
//        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( 1,System.currentTimeMillis(),"loc1",(byte)1);
//        ErrorCodeInfo pErrorInfo1 = mutithreadTableWriter_.insert( 2,System.currentTimeMillis(),"loc1",(short) 0);
//        ErrorCodeInfo pErrorInfo2 = mutithreadTableWriter_.insert( 3,System.currentTimeMillis(),"loc1",(int)-233);
//        ErrorCodeInfo pErrorInfo3 = mutithreadTableWriter_.insert( 4,System.currentTimeMillis(),"loc1",(long)-233);
//        ErrorCodeInfo pErrorInfo4 = mutithreadTableWriter_.insert( 5,System.currentTimeMillis(),"loc1",true);
//        ErrorCodeInfo pErrorInfo5 = mutithreadTableWriter_.insert( 6,System.currentTimeMillis(),"loc1",233.34f);
//        ErrorCodeInfo pErrorInfo6 = mutithreadTableWriter_.insert( 7,System.currentTimeMillis(),"loc1",-233.34);
//        ErrorCodeInfo pErrorInfo7 = mutithreadTableWriter_.insert( 8,System.currentTimeMillis(),"loc1","loc1");
//        List<String> list = new ArrayList<>();
//        list.add(null);
//        list.add("OceanBase");
//        BasicSymbolVector bsymbolv = new BasicSymbolVector(list);
//        ErrorCodeInfo pErrorInfo8 = mutithreadTableWriter_.insert(new BasicIntVector( new int[]{9,10}),new BasicTimestampVector(Arrays.asList(new Long[]{Long.valueOf(9), Long.valueOf(10)})),new BasicStringVector(new String[]{"GOOG","MS"}),bsymbolv);
//        BasicTable bt10 = (BasicTable) conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
//        //assertEquals(11, bt10.rows());
//        System.out.println(bt10.getString());
//    }

    @Test(timeout = 120000)
    public void test_MultithreadedTableWriter_user_authMode_scream() throws Exception {
        PrepareUser_authMode("scramUser","123456","scram");
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `char`int`long`short`id," +
                "[CHAR,INT,LONG,SHORT,INT]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "scramUser", "123456",
                "", "t1", false, false, null, 1, 1,
                1, "id");
        ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( (int)1, (int)1, (int)-1, (int)0, (int)-1);
        pErrorInfo=mutithreadTableWriter_.insert( null, (int)1, (int)1, (int)0, (int)1);
        assertEquals("code= info=",pErrorInfo.toString());
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(2, bt.rows());
        assertEquals("[1,]", bt.getColumn("char").getString());
        assertEquals("[1,1]", bt.getColumn("int").getString());
        assertEquals("[-1,1]", bt.getColumn("long").getString());
        assertEquals("[0,0]", bt.getColumn("short").getString());
        conn.run("undef(`t1,SHARED)");
    }
}

