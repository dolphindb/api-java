package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import static com.xxdb.Prepare.*;

public class StreamingSQLClientTest {
    public static DBConnection conn;
    public static DBConnection conn1;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @BeforeClass
    public static void setUp() throws IOException {
        try {clear_env_1();}catch (Exception e){}
    }

    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{conn.run("login(`admin, `123456)\n" +
                "    res = getStreamingSQLStatus()\n" +
                "    for(sqlStream in res){\n" +
                "        try{unsubscribeStreamingSQL(, sqlStream.queryId)}catch(ex){print ex}\n" +
                "        try{revokeStreamingSQL(sqlStream.queryId)}catch(ex){print ex}\n" +
                "    }\n" +
                "    go;\n" +
                "    try{revokeStreamingSQLTable(`t1)}catch(ex){print ex}\n" +
                "    try{revokeStreamingSQLTable(`t2)}catch(ex){print ex}\n" +
                "    try{undef(`t1,SHARED)}catch(ex){print ex}\n" +
                "    try{undef(`t2,SHARED)}catch(ex){print ex}");}catch (Exception ex){}
    }

    @After
    public void after() throws IOException, InterruptedException {
        try {clear_env();}catch (Exception e){}
        conn.close();
    }
    public static void Preparedata(String dataType) throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, "+dataType +"]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, "+dataType +"]) as t2;";
        conn.run(script);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
    }
    public static void writer_data(int count, String tableName, String dataType) throws IOException {
        String script = "login(`admin, `123456); \n" +
                "n="+count+";\n" +
                "bool = bool(rand([true, false, NULL], n));\n" +
                "char = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                "short = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                "int = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                "long = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                "double = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                "float = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                "date = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                "month = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                "time = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                "minute = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                "second = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                "datetime = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                "timestamp1 = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                "nanotime = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                "nanotimestamp = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                "symbol = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "string = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "uuid = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                "datehour = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                "ippaddr = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                "int128 = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                "blob = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                "complex = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "point = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "decimal32 = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                "decimal64 = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                "decimal128 = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                "data = table(take(\"A\"+string(1..300000), n) as id, timestamp(2025.08.26T12:36:23.438+1..n) as timestamp, "+ dataType +");\n" +
                tableName+".append!(data)\n";
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
        }

    public static void writerdata_array(long count1,long count2, String tableName, String dataType) throws IOException {
        String script1 = "login(`admin, `123456); \n"+
                "n="+count1+";\n" +
                "m="+count2+";\n" +
                "rows=ceil(double(n)/m)\n" +
                "bool = array(BOOL[]).append!(cut(take([true, false, NULL], n), m))\n" +
                "char = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), n), m))\n" +
                "short = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), n), m))\n" +
                "int = array(INT[]).append!(cut(take(-100..100 join NULL, n), m))\n" +
                "long = array(LONG[]).append!(cut(take(long(-100..100 join NULL), n), m))\n" +
                "double = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, n) + 0.254, m))\n" +
                "float = array(FLOAT[]).append!(cut(take(-100..100 join NULL, n) + 0.254f, m))\n" +
                "date = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, n), m))\n" +
                "month = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, n), m))\n" +
                "time = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, n), m))\n" +
                "minute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, n), m))\n" +
                "second = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, n), m))\n" +
                "datetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, n), m))\n" +
                "timestamp1 = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, n), m))\n" +
                "nanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "nanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "uuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), n), m))\n" +
                "datehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), n), m))\n" +
                "ipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), n), m))\n" +
                "int128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), n), m))\n" +
                "complex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "point = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "decimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, n) + 0.254, 3), m))\n" +
                "decimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25, 4), m))\n" +
                "decimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25, 5), m))\n" +
                "data = table(take(\"A\"+string(1..300000), rows) as id, timestamp(2025.08.26T12:36:23.438+1..rows) as timestamp, "+ dataType +");\n" +
                tableName+".append!(data)\n";
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script1);
    }

    public static void writer_data_decimal(String tableName) throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("n=100;\n" +
                "data = table(take(\"A\"+string(1..30), n) as id, timestamp(2025.08.26T12:36:23.438+1..n) as timestamp, take(decimal32(1..10,2), n) as value)\n" +
                tableName+".append!(data)");
    }

    public static void writer_data_array(String tableName) throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("n=100;\n" +
                "data = table(take(\"A\"+string(1..30), n) as id, timestamp(2025.08.26T12:36:23.438+1..n) as timestamp, take([take(1..10,10),take(1..10,10),take(1..10,10)], n) as value)\n" +
                tableName+".append!(data)");
    }

    @Test
    public void test_StreamingSQLClient_HOST_null() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        String re = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient(null, PORT, "admin","123456");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals("The param 'host' cannot be null or empty.", re);

        String re1 = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient("", PORT, "admin","123456");
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals("The param 'host' cannot be null or empty.", re1);
    }

    @Test
    public void test_StreamingSQLClient_HOST_not_true() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        String re = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient("erer", PORT, "admin","123456");
            streamingSQLClient.declareStreamingSQLTable("t1");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("Couldn't send script/function to the remote host because the connection has been closed"));
    }

    @Test
    public void test_StreamingSQLClient_PORT_error() throws IOException {
        Preparedata("DOUBLE");
        String re = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, -1, "admin","123456");
            streamingSQLClient.declareStreamingSQLTable("t1");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("Couldn't send script/function to the remote host because the connection has been closed"));

        String re1 = null;
        try{
            StreamingSQLClient streamingSQLClient= new StreamingSQLClient(HOST, 8888, "admin","123456");
            streamingSQLClient.declareStreamingSQLTable("t1");
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals(true, re1.contains("Couldn't send script/function to the remote host because the connection has been closed"));
    }

    @Test
    public void test_StreamingSQLClient_user_error() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        String re = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin123","123456");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true,re.contains("The user name or password is incorrect"));
    }

    @Test
    public void test_StreamingSQLClient_password_error() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        String re = null;
        try{
            StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456WWW");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true,re.contains("The user name or password is incorrect"));
    }

    @Test
    public void test_StreamingSQLClient_declareStreamingSQLTable_tableName_null() throws IOException, InterruptedException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            streamingSQLClient.declareStreamingSQLTable(null);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("declare streaming sql table error"));

        String re1 = null;
        try{
            streamingSQLClient.declareStreamingSQLTable("");
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals(true, re1.contains("declare streaming sql table error"));
    }

    @Test
    public void test_StreamingSQLClient_declareStreamingSQLTable_tableName_not_exist() throws IOException, InterruptedException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            streamingSQLClient.declareStreamingSQLTable("tttt12");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("Cannot recognize the token tttt12 script: declareStreamingSQLTable(tttt12)"));
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_sqlQuery_null() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String re = null;
        try{
            String id1 = streamingSQLClient.registerStreamingSQL(null);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals("The param 'sqlQuery' cannot be null or empty.", re);
        String re1 = null;
        try{
            String id2 = streamingSQLClient.registerStreamingSQL("");
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals("The param 'sqlQuery' cannot be null or empty.", re1);
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_sqlQuery_error() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String re = null;
        try{
            String id1 = streamingSQLClient.registerStreamingSQL("select * from t1 where aa = 1");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("register streaming SQL error"));
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_queryId_null() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1,"");
        String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1,null);
        Assert.assertNotNull(id1);
        Assert.assertNotNull(id2);
        Assert.assertNotEquals(id1, id2);
        BasicTable re1 = streamingSQLClient.getStreamingSQLStatus(id1);
        Assert.assertEquals(id1, re1.getColumn("queryId").get(0).getString());
        Assert.assertEquals("admin", re1.getColumn("user").get(0).getString());
        Assert.assertNotNull(re1.getColumn("registerTime").get(0).getString());
        Assert.assertEquals("SQL_REGISTERED", re1.getColumn("status").get(0).getString());
        Assert.assertEquals(sqlStr1, re1.getColumn("sqlQuery").get(0).getString());
        Assert.assertEquals("t1,t2", re1.getColumn("involvedTables").get(0).getString());
        Assert.assertEquals("", re1.getColumn("lastErrorMessage").get(0).getString());

        BasicTable re2 = streamingSQLClient.getStreamingSQLStatus(id2);
        Assert.assertEquals(id2, re2.getColumn("queryId").get(0).getString());
        Assert.assertEquals("admin", re2.getColumn("user").get(0).getString());
        Assert.assertNotNull(re2.getColumn("registerTime").get(0).getString());
        Assert.assertEquals("SQL_REGISTERED", re2.getColumn("status").get(0).getString());
        Assert.assertEquals(sqlStr1, re2.getColumn("sqlQuery").get(0).getString());
        Assert.assertEquals("t1,t2", re2.getColumn("involvedTables").get(0).getString());
        Assert.assertEquals("", re2.getColumn("lastErrorMessage").get(0).getString());

        streamingSQLClient.revokeStreamingSQL(id1);
        streamingSQLClient.revokeStreamingSQL(id2);
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_queryId_exist() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1,"ddb");
        String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1,"ddb");
        Assert.assertNotNull(id1);
        Assert.assertNotNull(id2);
        Assert.assertNotEquals(id1, id2);
        Assert.assertEquals("ddb", id1);
//        System.out.println(id2);
        Assert.assertEquals(true, id2.contains("ddb_"));
        streamingSQLClient.revokeStreamingSQL(id1);
        streamingSQLClient.revokeStreamingSQL(id2);
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_logTableCacheSize_negative() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String re = null;
        try{
            String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1,"ddb",-100);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals("logTableCacheSize must be a positive integer.", re);

        String re1 = null;
        try{
            String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1,"ddb",0);
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals("logTableCacheSize must be a positive integer.", re1);
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_logTableCacheSize_MIN_VALUE() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1,"ddb",Integer.MIN_VALUE);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");

        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_registerStreamingSQL_logTableCacheSize_10000() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1, 10000);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");

        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_revokeStreamingSQL_queryId_not_exist() throws IOException, InterruptedException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            streamingSQLClient.revokeStreamingSQL("123");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("revoke streaming SQL error:"));
    }

    @Test
    public void test_StreamingSQLClient_revokeStreamingSQL_repeatedly() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        streamingSQLClient.revokeStreamingSQL(id1);
        String re = null;
        try{
            streamingSQLClient.revokeStreamingSQL(id1);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("revoke streaming SQL error:"));
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_queryId_null() throws IOException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            BasicTable bt = streamingSQLClient.subscribeStreamingSQL("");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals("The param 'queryId' cannot be null or empty.", re);

        String re1 = null;
        try{
            BasicTable bt1 = streamingSQLClient.subscribeStreamingSQL(null);
        }catch(Exception e){
            re1 = e.getMessage();
        }
        Assert.assertEquals("The param 'queryId' cannot be null or empty.", re1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_queryId_not_exist() throws IOException, InterruptedException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            BasicTable bt = streamingSQLClient.subscribeStreamingSQL("id111");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals("queryId id111 does not exist.", re);
    }

    @Test
    public void test_StreamingSQLClient_getStreamingSQLStatus_queryId_not_exist() throws IOException, InterruptedException {
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        String re = null;
        try{
            streamingSQLClient.getStreamingSQLStatus("www");
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains("Server response: getStreamingSQLStatus(\"www\") => queryId www does not exist. script: getStreamingSQLStatus(\"www\")"));
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_memory_table() throws IOException, InterruptedException {
        String script = "share table( 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share table( 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" ;
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");

        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_batchSize_lt0() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,-100,1);
        writer_data(8192,"t1","double");
        writer_data(8192,"t2","double");

        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_throttle_lt0() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,-1);
        writer_data(8192,"t1","double");
        writer_data(8192,"t2","double");

        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_batchSize_throttle() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,10000,1);
        writer_data(8192,"t1","double");
        writer_data(8192,"t2","double");

        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_batchSize_1000000_throttle() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1000000,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");

        Thread.sleep(1200);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_batchSize_100_throttle_100() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,100,100);
        writer_data(99,"t1","double");
        writer_data(99,"t2","double");
        System.out.println(bt.rows());
        Thread.sleep(2000);
        Assert.assertEquals(0, bt.rows());
        conn.run("n=1\n" +
                "data = table(take(\"A\"+string(100..100000), n) as id, timestamp(2025.09.26T12:36:23.438+1..n) as timestamp, double(rand(rand(-100..100, 1000)*0.23, n) as value));\n" +
                "t1.append!(data)\n" +
                "t2.append!(data)");
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_BatchSize_lt0_throttle_lt0() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,-1,-1);
        Thread.sleep(100);
        Assert.assertEquals(0, bt.rows());
        writer_data(1,"t1","double");
        writer_data(1,"t2","double");
        Thread.sleep(200);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        Assert.assertEquals(1, bt.rows());
        writer_data(2,"t1","double");
        writer_data(2,"t2","double");
        Thread.sleep(200);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        checkData(ex1, bt);
        Assert.assertEquals(2, bt.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_msg_null() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,10,1);
        Thread.sleep(1100);
        Assert.assertEquals(0, bt.rows());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }


    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_basic() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        streamingSQLClient.isClose();
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(1000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        Assert.assertEquals(100, bt.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_handler_null() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(1000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        Assert.assertEquals(100, bt.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_table_have_data() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DECIMAL32(5)]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DECIMAL32(5)]) as t2;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        writer_data_decimal("t1");
        writer_data_decimal("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_int() throws IOException, InterruptedException {
        Preparedata("INT");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","int");
        writer_data(100,"t2","int");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_bool() throws IOException, InterruptedException {
        Preparedata("BOOL");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","bool");
        writer_data(100,"t2","bool");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_char() throws IOException, InterruptedException {
        Preparedata("CHAR");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","char");
        writer_data(100,"t2","char");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_short() throws IOException, InterruptedException {
        Preparedata("SHORT");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","short");
        writer_data(100,"t2","short");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_long() throws IOException, InterruptedException {
        Preparedata("LONG");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","long");
        writer_data(100,"t2","long");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_double() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_float() throws IOException, InterruptedException {
        Preparedata("FLOAT");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","float");
        writer_data(100,"t2","float");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_date() throws IOException, InterruptedException {
        Preparedata("DATE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","date");
        writer_data(100,"t2","date");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_month() throws IOException, InterruptedException {
        Preparedata("MONTH");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","month");
        writer_data(100,"t2","month");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_time() throws IOException, InterruptedException {
        Preparedata("TIME");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","time");
        writer_data(100,"t2","time");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_minute() throws IOException, InterruptedException {
        Preparedata("MINUTE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","minute");
        writer_data(100,"t2","minute");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_second() throws IOException, InterruptedException {
        Preparedata("SECOND");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","second");
        writer_data(100,"t2","second");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_datetime() throws IOException, InterruptedException {
        Preparedata("DATETIME");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","datetime");
        writer_data(100,"t2","datetime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_timestamp() throws IOException, InterruptedException {
        Preparedata("TIMESTAMP");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","timestamp1");
        writer_data(100,"t2","timestamp1");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_nanotime() throws IOException, InterruptedException {
        Preparedata("NANOTIME");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","nanotime");
        writer_data(100,"t2","nanotime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_nanotimestamp() throws IOException, InterruptedException {
        Preparedata("NANOTIMESTAMP");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","nanotimestamp");
        writer_data(100,"t2","nanotimestamp");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_symbol() throws IOException, InterruptedException {
        Preparedata("SYMBOL");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","symbol");
        writer_data(100,"t2","symbol");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_string() throws IOException, InterruptedException {
        Preparedata("STRING");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","string");
        writer_data(100,"t2","string");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_uuid() throws IOException, InterruptedException {
        Preparedata("UUID");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","uuid");
        writer_data(100,"t2","uuid");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_datehour() throws IOException, InterruptedException {
        Preparedata("DATEHOUR");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","datehour");
        writer_data(100,"t2","datehour");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_ipaddr() throws IOException, InterruptedException {
        Preparedata("IPADDR");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","ippaddr");
        writer_data(100,"t2","ippaddr");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_int128() throws IOException, InterruptedException {
        Preparedata("INT128");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","int128");
        writer_data(100,"t2","int128");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_BLOB() throws IOException, InterruptedException {
        Preparedata("BLOB");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","blob");
        writer_data(100,"t2","blob");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_complex() throws IOException, InterruptedException {
        Preparedata("COMPLEX");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value  FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","complex");
        writer_data(100,"t2","complex");
        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_dataType_point() throws IOException, InterruptedException {
        Preparedata("POINT");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id,t1.value as t1_value, t2.value as t2_value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data(100,"t1","point");
        writer_data(100,"t2","point");
        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_DECIMAL() throws IOException, InterruptedException {
        String script = "colName=[\"id\",\"col0\",\"col1\",\"col2\",\"col3\",\"col4\",\"col5\"]\n" +
                "colType=[\"INT\",\"DECIMAL32(4)\",\"DECIMAL64(4)\",\"DECIMAL128(4)\",\"DECIMAL32(0)\",\"DECIMAL64(0)\",\"DECIMAL128(0)\"]\n" +
                "tmp = table(100:0, colName, colType);\n" +
                "share keyedTable(`id,100:0, colName, colType) as t1;\n" +
                "share keyedTable(`id,100:0, colName, colType) as t2;";
        conn.run(script);
        String script1 ="insert into tmp values(1, NULL, NULL, NULL, NULL, NULL, NULL);\n" +
                "insert into tmp values(2,2.2222,3.3333,4.4444,5.5555,6.6666,7.6666);\n" +
                "insert into tmp values(3,-2,-3,-4,-5,-6,-7);\n" +
                "insert into tmp values(4,-2.999999,-3.999999,-4.999999,-5.999999,-6.999999,-7.999999);\n" +
                "insert into tmp values(5,99999.9999,9999999999999.9999,999999999999999999999999999999999.9999,999999999,999999999999999999,9999999999999999999999999999999999999);\n" +
                "insert into tmp values(6,0,0,0,0,0,0);\n" +
                "insert into tmp values(7,-99999.9999,-9999999999999.9999,-999999999999999999999999999999999.9999,-999999999,999999999999999999,-9999999999999999999999999999999999999);\n" +
                "insert into tmp values(9,-0.999,-0.999,-1.0001000000000000000000000002,-999999999,999999999999999999,-0.1001000000000000000000000002);\n" +
                "t1.append!(tmp)\n" +
                "t2.append!(tmp)";
                StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.col0, t1.col1, t1.col2, t1.col3, t1.col4, t1.col5 FROM t1 INNER JOIN t2 ON t1.id = t2.id order by id";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        conn.run(script1);
        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_bool() throws IOException, InterruptedException {
        Preparedata("BOOL[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","bool");
        writerdata_array(100,5,"t2","bool");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_char() throws IOException, InterruptedException {
        Preparedata("CHAR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","char");
        writerdata_array(100,5,"t2","char");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_short() throws IOException, InterruptedException {
        Preparedata("SHORT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","short");
        writerdata_array(100,5,"t2","short");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_int() throws IOException, InterruptedException {
        Preparedata("INT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","int");
        writerdata_array(100,5,"t2","int");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_long() throws IOException, InterruptedException {
        Preparedata("LONG[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","long");
        writerdata_array(100,5,"t2","long");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_double() throws IOException, InterruptedException {
        Preparedata("DOUBLE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","double");
        writerdata_array(100,5,"t2","double");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_float() throws IOException, InterruptedException {
        Preparedata("FLOAT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","float");
        writerdata_array(100,5,"t2","float");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_date() throws IOException, InterruptedException {
        Preparedata("DATE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","date");
        writerdata_array(100,5,"t2","date");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_month() throws IOException, InterruptedException {
        Preparedata("MONTH[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","month");
        writerdata_array(100,5,"t2","month");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_time() throws IOException, InterruptedException {
        Preparedata("TIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","time");
        writerdata_array(100,5,"t2","time");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_minute() throws IOException, InterruptedException {
        Preparedata("MINUTE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","minute");
        writerdata_array(100,5,"t2","minute");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_second() throws IOException, InterruptedException {
        Preparedata("SECOND[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","second");
        writerdata_array(100,5,"t2","second");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_datetime() throws IOException, InterruptedException {
        Preparedata("DATETIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","datetime");
        writerdata_array(100,5,"t2","datetime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_timestamp() throws IOException, InterruptedException {
        Preparedata("TIMESTAMP[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","timestamp1");
        writerdata_array(100,5,"t2","timestamp1");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_nanotime() throws IOException, InterruptedException {
        Preparedata("NANOTIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","nanotime");
        writerdata_array(100,5,"t2","nanotime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_nanotimestamp() throws IOException, InterruptedException {
        Preparedata("NANOTIMESTAMP[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","nanotimestamp");
        writerdata_array(100,5,"t2","nanotimestamp");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_uuid() throws IOException, InterruptedException {
        Preparedata("UUID[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","uuid");
        writerdata_array(100,5,"t2","uuid");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_datehour() throws IOException, InterruptedException {
        Preparedata("DATEHOUR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","datehour");
        writerdata_array(100,5,"t2","datehour");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_ipaddr() throws IOException, InterruptedException {
        Preparedata("IPADDR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","ipaddr");
        writerdata_array(100,5,"t2","ipaddr");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_int128() throws IOException, InterruptedException {
        Preparedata("INT128[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","int128");
        writerdata_array(100,5,"t2","int128");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_complex() throws IOException, InterruptedException {
        Preparedata("COMPLEX[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","complex");
        writerdata_array(100,5,"t2","complex");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_point() throws IOException, InterruptedException {
        Preparedata("POINT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","point");
        writerdata_array(100,5,"t2","point");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_decimal32() throws IOException, InterruptedException {
        Preparedata("DECIMAL32(2)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        Assert.assertEquals(0, bt.rows());
        writerdata_array(100,5,"t1","decimal32");
        writerdata_array(100,5,"t2","decimal32");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_decimal64() throws IOException, InterruptedException {
        Preparedata("DECIMAL64(7)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","decimal64");
        writerdata_array(100,5,"t2","decimal64");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_array_decimal128() throws IOException, InterruptedException {
        Preparedata("DECIMAL128(19)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","decimal128");
        writerdata_array(100,5,"t2","decimal128");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_any() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, ANY]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, ANY]) as t2;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time,t1.value FROM t1 FULL JOIN t2 ON t1.id=t2.id";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writer_data_array("t1");
        writer_data_array("t2");
        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    public static MessageHandler MessageHandler_handler = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
                            String script = String.format("insert into Receive values('%s',%f)", msg.getEntity(2).getString(), Double.valueOf(msg.getEntity(3).toString()));
            try {
                conn.run(script);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_handler() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" +
                "share table(1:0, `id`value, [SYMBOL, DOUBLE]) as Receive;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1, MessageHandler_handler);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(5000);
        BasicTable re = (BasicTable)conn.run("select * from Receive order by id");
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.getString());
        checkData(ex, re);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_getStreamingSQLStatus() throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" +
                "share table(1:0, `id`value, [SYMBOL, DOUBLE]) as Receive;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt1 = streamingSQLClient.subscribeStreamingSQL(id1);
        BasicTable bt2 = streamingSQLClient.subscribeStreamingSQL(id2);
        BasicTable re = streamingSQLClient.getStreamingSQLStatus();
        BasicTable ex = (BasicTable)conn.run("getStreamingSQLStatus()");
        checkData(ex, re);
        Assert.assertEquals(2, re.rows());
        System.out.println(re.getString());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
        streamingSQLClient.unsubscribeStreamingSQL(id2);
    }

    @Test
    public void test_StreamingSQLClient_getStreamingSQLStatus_queryId_null() throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" +
                "share table(1:0, `id`value, [SYMBOL, DOUBLE]) as Receive;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt1 = streamingSQLClient.subscribeStreamingSQL(id1);
        BasicTable bt2 = streamingSQLClient.subscribeStreamingSQL(id2);
        BasicTable re = streamingSQLClient.getStreamingSQLStatus(null);
        BasicTable ex = (BasicTable)conn.run("getStreamingSQLStatus()");
        checkData(ex, re);
        Assert.assertEquals(2, re.rows());
        System.out.println(re.getString());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
        streamingSQLClient.unsubscribeStreamingSQL(id2);
    }

    @Test
    public void test_StreamingSQLClient_getStreamingSQLStatus_queryId() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" +
                "share table(1:0, `id`value, [SYMBOL, DOUBLE]) as Receive;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        String id2 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);

        BasicTable re1 = streamingSQLClient.getStreamingSQLStatus(id1);
        BasicTable re2 = streamingSQLClient.getStreamingSQLStatus(id2);
        BasicTable ex1 = (BasicTable)conn.run("getStreamingSQLStatus('" + id1 + "')");
        BasicTable ex2 = (BasicTable)conn.run("getStreamingSQLStatus('" + id2 + "')");
        checkData(ex1, re1);
        checkData(ex2, re2);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_revokeStreamingSQLTable() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        BasicTable re1 = (BasicTable)conn.run("listStreamingSQLTables()");
        Assert.assertEquals(true, re1.getString().contains("\n" +
                "t1 "));
        streamingSQLClient.revokeStreamingSQLTable("t1");
        BasicTable re2 = (BasicTable)conn.run("listStreamingSQLTables()");
        Assert.assertEquals(false, re2.getString().contains("\n" +
                "t1 "));
    }

    @Test
    public void test_StreamingSQLClient_listStreamingSQLTables() throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;\n" +
                "share table(1:0, `id`value, [SYMBOL, DOUBLE]) as Receive;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        BasicTable re = streamingSQLClient.listStreamingSQLTables();
        BasicTable ex = (BasicTable)conn.run("listStreamingSQLTables()");
        checkData(ex, re);
        streamingSQLClient.revokeStreamingSQLTable("t1");
        streamingSQLClient.revokeStreamingSQLTable("t2");
    }

    @Test
    public void test_StreamingSQLClient_unsubscribeStreamingSQL() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_unsubscribeStreamingSQL_repeatedly() throws IOException, InterruptedException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
        String re = null;
        try{
            streamingSQLClient.unsubscribeStreamingSQL(id1);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(true, re.contains(" Query is not running:"));
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_unsubscribeStreamingSQL_namy_times() throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, DOUBLE]) as t2;";
        conn.run(script);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        for(int i=0;i<10;i++){
            System.out.println("i:"+i);
            BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
            streamingSQLClient.unsubscribeStreamingSQL(id1);
        }
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_data_1() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(1,"t1","double");
        writer_data(1,"t2","double");

        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_data_10000() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,10000,1);
        writer_data(10000,"t1","double");
        writer_data(10000,"t2","double");

        Thread.sleep(5000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        BasicTable exee = (BasicTable)conn.run("select * from t1;");
        System.out.println(exee.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_sql_order_by() throws IOException, InterruptedException {
        Preparedata("BOOL[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id order by id\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","bool");
        writerdata_array(100,5,"t2","bool");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);

        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("bt_tmp", bt);
        conn.upload(map);
        Entity ex1 = conn.run("res1 = select * from bt_tmp order by id;\n" +
                "ex1 = SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id order by id \n" +
                "assert 1, each(eqObj, res1.values(), ex1.values())");
        System.out.println(ex1.getString());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
}
