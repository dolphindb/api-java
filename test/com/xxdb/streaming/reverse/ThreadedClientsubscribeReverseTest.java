package com.xxdb.streaming.reverse;

import com.xxdb.DBConnection;
import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.streaming.client.*;
import org.javatuples.Pair;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static com.xxdb.Prepare.PrepareUser;
import static com.xxdb.Prepare.clear_env;
import static com.xxdb.data.Entity.DATA_TYPE.*;
import static org.junit.Assert.*;

public class ThreadedClientsubscribeReverseTest {
    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT = 9002;
    private static ThreadedClient client;

    @BeforeClass
    public static void setUp() throws IOException {

    }
    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
            client = new ThreadedClient(HOST, 0);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{client.unsubscribe(HOST, PORT, "Trades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades2");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        clear_env();
    }

    @After
    public void after() throws IOException, InterruptedException {
        try{client.unsubscribe(HOST, PORT, "Trades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades2");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        clear_env();
        Thread.sleep(2000);
        client.close();
        conn.close();
        Thread.sleep(2000);
    }

    @AfterClass
    public static void clear_conn() {
    }
    public static MessageHandler MessageHandler_handler = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                conn.run(script);
                //  System.out.println(msg.getEntity(0).getString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };

    public static List<Integer> save_batch_size = new ArrayList<Integer>();
    BatchMessageHandler BatchMessageHandler_handler = new BatchMessageHandler() {
        @Override
        public void batchHandler(List<IMessage> msgs) {
            try {
                save_batch_size.add(msgs.size());
                for(int x = 0; x<msgs.size(); x++){
                    doEvent(msgs.get(x));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        @Override
        public void doEvent(IMessage msg) {
            try {
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                conn.run(script);
                //  System.out.println(msg.getEntity(0).getString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public void wait_data(String table_name,int data_row) throws IOException, InterruptedException {
        BasicInt row_num;
        while(true){
            row_num = (BasicInt)conn.run("(exec count(*) from "+table_name+")[0]");
//            System.out.println(row_num.getInt());
            if(row_num.getInt() == data_row){
                break;
            }
            Thread.sleep(100);
        }
    }
    public static void PrepareStreamTable(String dataType) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n"+
                "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n"+
                "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n"+
                "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n"+
                "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n"+"" +
                "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n"+
                "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n"+
                "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n"+
                "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n"+
                "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n"+
                "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n"+
                "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n"+
                "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n"+
                "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n"+
                "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n"+
                "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n"+
                "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n"+
                "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType +"[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareStreamTableDecimal(String dataType, int scale) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"("+scale+")[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_DECIMAL32 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL64 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL128 =   array(DECIMAL128(8)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType +"("+scale+")[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareStreamTable_StreamDeserializer(String dataType) throws IOException {
        String script = "share streamTable(1000000:0, `permno`sym`blob`dateType, [TIMESTAMP,SYMBOL,BLOB,"+dataType+"[]]) as outTables;\n"+
                "timestampv = 2018.12.01T01:21:23.000 + 1..1000; \n"+
                "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n"+
                "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n"+
                "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n"+
                "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n"+
                "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n"+"" +
                "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n"+
                "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n"+
                "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n"+
                "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n"+
                "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n"+
                "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n"+
                "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n"+
                "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n"+
                "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n"+
                "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n"+
                "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n"+
                "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n"+
                "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n"+
                "share table(timestampv as timestampv,dateType_"+dataType +" as dateType) as pub_t1;\n"+
                "share table(timestampv as timestampv,dateType_"+dataType +" as dateType) as pub_t2;\n"+
                "d = dict(['msg1','msg2'], [pub_t1, pub_t2]);\n"+
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv);\n"+
                "share streamTable(1000000:0, `timestampv`dateType, [TIMESTAMP,"+dataType +"[]]) as sub1;\n"+
                "share streamTable(1000000:0, `timestampv`dateType, [TIMESTAMP,"+dataType +"[]]) as sub2;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareStreamTableDecimal_StreamDeserializer(String dataType, int scale) throws IOException {
        String script = "share streamTable(1000000:0, `permno`sym`blob`dateType, [TIMESTAMP,SYMBOL,BLOB,"+dataType+"("+scale+")[]]) as outTables;\n"+
                "timestampv = 2018.12.01T01:21:23.000 + 1..1000;  \n"+
                "dateType_DECIMAL32 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL64 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL128 =   array(DECIMAL128(8)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "share table(timestampv as timestampv,dateType_"+dataType +" as dateType) as pub_t1\n"+
                "share table(timestampv as timestampv,dateType_"+dataType +" as dateType) as pub_t2\n"+
                "d = dict(['msg1','msg2'], [pub_t1, pub_t2]);\n"+
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv);\n"+
                "share streamTable(1000000:0, `timestampv`dateType, [TIMESTAMP,"+dataType +"("+scale+")[]]) as sub1;\n"+
                "share streamTable(1000000:0, `timestampv`dateType, [TIMESTAMP,"+dataType +"("+scale+")[]]) as sub2;\n";

        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void checkResult() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            if (tmpNum.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(2000);
        }
        BasicTable except = (BasicTable)conn.run("select * from  Trades");
        BasicTable res = (BasicTable)conn.run("select * from  sub1");
        assertEquals(except.rows(), res.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
        }
    }
    public static void checkResult1() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2");
            if (tmpNum.getInt()==(1000)&& tmpNum1.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(2000);
        }
        BasicTable except = (BasicTable)conn.run("select * from  pub_t1");
        BasicTable res = (BasicTable)conn.run("select * from  sub1");
        BasicTable except1 = (BasicTable)conn.run("select * from  pub_t2");
        BasicTable res1 = (BasicTable)conn.run("select * from  sub2");
        assertEquals(except.rows(), res.rows());
        assertEquals(except1.rows(), res1.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
            assertEquals(except1.getColumn(i).getString(), res1.getColumn(i).getString());
        }
    }
    @Test
    public void test_ThreadedClient_HOST_error() throws IOException {
        ThreadedClient client2 = new ThreadedClient("host_error",10022);
        client2.close();
    }

    @Test
    public void test_ThreadedClient_PORT_error() throws IOException {
        ThreadedClient client2 = new ThreadedClient(HOST,0);
        client2.close();
    }

    @Test
    public void test_ThreadedClient_HOST_subport() throws IOException {
        ThreadedClient client2 = new ThreadedClient(HOST,10022);
        client2.close();
    }
    @Test
    public void test_ThreadedClient_null() throws IOException {
        ThreadedClient client2 = new ThreadedClient();
        client2.close();
    }

    @Test
    public void test_ThreadedClient_only_subscribePort() throws IOException {
        ThreadedClient client2 = new ThreadedClient(0);
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades1)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades1),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        client2.subscribe(HOST,PORT,"Trades1","subTrades1",MessageHandler_handler);
        client2.unsubscribe(HOST,PORT,"Trades1","subTrades1");
        client2.close();
    }



    @Test(timeout = 120000)
    public void test_subscribe_ex1() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades1)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades1),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        client.subscribe(HOST, PORT, "Trades1", MessageHandler_handler);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        wait_data("Receive",1000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades1");
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
        try {
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades");
        }catch (Exception ex){

        }
    }
    @Test(timeout = 180000)
    public void test_subscribe_ex2() throws Exception{
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, true);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",5000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(5000, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_batchSize_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                MessageHandler_handler, -1, true, filter1, true, -10000, 5);

    }
    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_BatchMessageHandler_batchSize_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                BatchMessageHandler_handler, -1, true, filter1, true, -10000, 5);

    }
    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_batchSize_floatThrottle_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                MessageHandler_handler, -1L, true, filter1, true, -10000, (float)5.5);

    }

    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_BatchMessageHandler_batchSize_floatThrottle_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                BatchMessageHandler_handler, -1L, true, filter1, true, -10000, (float)5.5);

    }

    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_throttle_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                MessageHandler_handler, -1, true, filter1, true, 10000, -5);

    }
    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_BatchMessageHandler_throttle_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                BatchMessageHandler_handler, -1, true, filter1, true, 10000, -5);

    }

    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_throttle_float_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                MessageHandler_handler, (long) -1, true, filter1, true, 10000, (float) -5.5);

    }
    @Test(expected = IllegalArgumentException.class)
    public void test_subscribe_BatchMessageHandler_throttle_float_lt0() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",
                BatchMessageHandler_handler, (long) -1, true, filter1, true, 10000, (float) -5.5);

    }

    @Test(timeout = 180000)
    public void test_subscribe_tableName_offset_usr_pass() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 0;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst,false,"admin","123456");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
        client.unsubscribe(HOST, PORT, "Trades");
    }

    @Test(timeout = 180000)
    public void test_subscribe_tableName_actionName() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        client.subscribe(HOST, PORT, "Trades","subTrades1",MessageHandler_handler);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",3000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(3000, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i + 100));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i + 100));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 100)).getNumber().doubleValue(),4);
        }
        client.unsubscribe(HOST, PORT, "Trades","subTrades1");
    }

    @Test(timeout = 180000)
    public void test_subscribe_tableName_handler_offset_reconnect_success() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades",MessageHandler_handler,-1,true);
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',9055,'Trades')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',9055,'Trades')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades");
    }

    @Test(timeout = 180000)
    public void test_subscribe_TableName_ActionName_Handler_reconnect() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTrades1",MessageHandler_handler,true);
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',9055,'Trades',\"subTrades1\")");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',9055,'Trades',\"subTrades1\")");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTrades1");
    }

    @Test(timeout = 180000)
    public void test_subscribe_tn_an_hd_ofst_reconnect_filter_ae_bs_thFloat_usr_pass() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,(float)4.5,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test(timeout = 180000)
    public void test_subscribe_tn_an_bmhd_ofst_reconnect_filter_ae_bs_th_usr_pass() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100,5,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_subscribe_tn_an_bmhd_ofst_reconnect_filter_ae_bs_thFloat_usr_pass() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100,(float)4.5,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_subscribe_tn_an_hd_ofst_reconnect_filter_deserialize() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,null);
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_tn_an_handler_ofst_reconnect_filter_ae_usr_pass() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_ofst0() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 0;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
        client.unsubscribe(HOST, PORT, "Trades");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_ofst_negative2() throws IOException, InterruptedException {
        int ofst = -2;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        Thread.sleep(2000);
        client.unsubscribe(HOST, PORT, "Trades");
    }

    @Test(timeout=180000)
    public void test_subscribe_ofst_negative1() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        int ofst = -1;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",3000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(3000, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i + 100));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i + 100));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 100)).getNumber().doubleValue(),4);
        }
        client.unsubscribe(HOST, PORT, "Trades");
    }

    @Test(timeout = 60000)
    public void test_subscribe_ofst_10() throws Exception{
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        int ofst = 10;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",3090);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        assertEquals(3090, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i + 10));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i + 10));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 10)).getNumber().doubleValue(), 4);
        }
        client.unsubscribe(HOST, PORT, "Trades");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_ofst_morethan_tablecount() throws IOException {
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
    }

    @Test(timeout = 60000)
    public void test_subscribe_filter() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        String script3 = "st3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
        conn.run(script3);
        MessageHandler handler1 = new MessageHandler() {
            @Override
            public void doEvent(IMessage msg) {
                try {
                    String script = String.format("insert into filter values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                    conn.run(script);
                    //  System.out.println(msg.getEntity(0).getString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        int ofst = -1;
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, filter1);
        Vector filter2 = (Vector) conn.run("2001..3000");
        client.subscribe(HOST, PORT, "Trades", "subTrades2", handler1, -1, filter2);
        conn.run("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        wait_data("filter",1000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        BasicTable fil = (BasicTable) conn.run("filter");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades2");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades1");
        conn.run("dropStreamTable(`filter)");
        assertEquals(1000, re.rows());
        assertEquals(1000, fil.rows());

        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }

        for (int i = 0; i < fil.rows(); i++) {
            assertEquals(fil.getColumn(0).get(i), tra.getColumn(0).get(i + 2000));
            assertEquals(fil.getColumn(1).get(i), tra.getColumn(1).get(i + 2000));
            assertEquals(((Scalar)fil.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 2000)).getNumber().doubleValue(), 4);
        }
    }

    @Test(timeout = 120000)
    public void test_subscribe_batchSize_throttle() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(2000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 10000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 10000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 10000)).getNumber().doubleValue(), 4);
        }
    }

    @Test(timeout = 60000)
    public void test_subscribe_batchSize_throttle2() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",200);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(200, re.rows());
        for (int i = 0; i <re.rows();i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
    }

    @Test(timeout = 60000)
    public void test_subscribe_unsubscribe_resubscribe() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        for (int i=0;i<10;i++){
        client.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades2", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades3", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades1");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades2");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades3");
        }
    }

    @Test(expected = IOException.class)
    public void test_subscribe_user_error() throws IOException {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "admin_error", "123456");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_password_error() throws IOException {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin","error_password");

    }

    @Test(timeout = 60000)
    public void test_subscribe_admin() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","admin",MessageHandler_handler,-1,false,filter1,true,100,5,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","admin");
    }
    @Test(timeout = 60000)
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        PrepareUser("test1","123456");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,false,filter1,true,100,5,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_other_user_allow_unsubscribe_login() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        PrepareUser("test1","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),grant{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,false,filter1,true,100,5,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_other_user_unallow() throws IOException{
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        PrepareUser("test1","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});");

        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(true,e.getMessage().contains("No access to shared table [Trades]."));
        }
    }

    @Test(timeout = 60000)
    public void test_subscribe_other_some_user() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        PrepareUser("test1","123456");
        PrepareUser("test2","123456");
        PrepareUser("test3","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades\"});");
        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(true,e.getMessage().contains("No access to shared table [Trades]."));
            System.out.println(e.getMessage());
        }

        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]. Contact an administrator. RefId:S03009' function: 'publishTable'",e.getMessage());
        }
        client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, false, filter1, true, 100, 5, "test2", "123456");
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        PrepareUser("test1","123456");
        conn.run("share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        client.subscribe(HOST,PORT,"tmp_st1","subTread1",MessageHandler_handler,-1,true,null,true,100,5,"test1","123456");
        client.subscribe(HOST,PORT,"tmp_st2","subTread1",MessageHandler_handler,-1,true,null,true,100,5,"test1","123456");
        try {
            client.subscribe(HOST, PORT, "tmp_st3", "subTread1", MessageHandler_handler, -1, true, null, true, 100, 5, "test1", "123456_error");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        BasicInt row_num;
        wait_data("Receive",20000);
        row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"tmp_st1","subTread1");
        client.unsubscribe(HOST,PORT,"tmp_st2","subTread1");
    }

    @Test(timeout = 60000)
    public void test_func_BatchMessageHandler() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","BatchMessageHandler",BatchMessageHandler_handler,-1,false,filter1,true,1024,5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","BatchMessageHandler");
    }

    @Test(timeout = 120000)
    public void test_func_BatchMessageHandler_not_set_batchSize() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTrades",BatchMessageHandler_handler,-1,true,filter1,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(true,save_batch_size.isEmpty());
        client.unsubscribe(HOST,PORT,"Trades","subTrades");
    }

    @Test(timeout = 60000)
    public void test_func_BatchMessageHandler_single_msg() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","single_msg",BatchMessageHandler_handler,-1,false,filter1,true,100,1);
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(1,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","single_msg");
    }

    @Test(timeout = 120000)
    public void test_func_BatchMessageHandler_mul_single_msg() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,false,filter1,true,100,100000);
        for(int n = 0;n<10000;n++) {
            conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        }
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Ignore
    public void test_func_BatchMessageHandler_batchSize_over_meg_size() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100000,2);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(10000,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 200000)
    public void test_func_BatchMessageHandler_batchSize_over_meg_size_small_throttle() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,false,filter1,true,100000,1);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 200000)
    public void test_func_BatchMessageHandler_batchSize_over_meg_size_big_throttle() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,false,filter1,true,100000,10);
        conn.run("n=100000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",100000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(100000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test(timeout = 200000)
        public void test_subscribe_throttle_0f() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,false,filter1,null,true,100000,0.0f);
        conn.run("n=100000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",100000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(100000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test(timeout = 200000)
    public void test_subscribe_throttle_0() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,false,filter1,null,true,1000,0);
        conn.run("n=100000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(1000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test(timeout = 120000)
    public void test_func_BatchMessageHandler_mul_subscribe() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST, PORT, "Trades", "subTread1", BatchMessageHandler_handler, -1, false, filter1, true, 1000, 2);
        client.subscribe(HOST, PORT, "Trades", "subTread2", BatchMessageHandler_handler, -1, false, filter1, true, 1000, 2);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",20000);
        BasicInt row_num = (BasicInt) conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000, row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
        client.unsubscribe(HOST,PORT,"Trades","subTread2");
    }

    public class MyThread extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    @Test(timeout=120000)
    public void test_subscribe_reconnect_successful() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share st1 as Trades1\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades1),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades1","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin","123456");
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        Thread.sleep(100);
        MyThread write_data  = new MyThread ();
//        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',0,'Trades1','subTread1')");
        Thread.sleep(10000);
        conn.run("stopPublishTable('"+HOST+"',0,'Trades1','subTread1')");
        Thread.sleep(3000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num);
        Thread.sleep(2000);
        BasicInt row_num2 = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(row_num2);
        assertEquals(true,row_num.getInt()<=row_num2.getInt());
//        write_data.interrupt();
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_reconnect_error() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true, 100, 5, "admin", "123456");
        System.out.println("Successful subscribe");
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTrades');" +
                "try{dropStreamTable('Trades')}catch(ex){};");
        Thread.sleep(5000);
        try {
            client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        }catch (Exception ex){}
    }

    @Test(timeout = 120000)
    public void test_subscribe_reconnect_false() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread2",MessageHandler_handler,-1,false,filter1,true,100,5,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('"+HOST+"',8676,'Trades','subTread2')");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertNotEquals(2000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread2");
    }

    class Handler6 implements MessageHandler {
        private StreamDeserializer deserializer_;
        private List<BasicMessage> msg1 = new ArrayList<>();
        private List<BasicMessage> msg2 = new ArrayList<>();

        public Handler6(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void batchHandler(List<IMessage> msgs) {
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public List<BasicMessage> getMsg1() {
            return msg1;
        }
        public List<BasicMessage> getMsg2() {
            return msg2;
        }
    }

    @Test(timeout = 120000)
    public void test_BasicMessage_parse_outputTable_col_size_2() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`sym,[TIMESTAMP,BLOB])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        conn.run("t = table(2016.10.12T00:00:00.000 2016.10.12T00:00:00.000 as a,blob(`a`b) as b)\n" +
                    "outTables.append!(t)");
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_BasicMessage_parse_outputTable_second_col_not_string() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`id`blob`price1,[TIMESTAMP,INT,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");

        Map<String, Pair<String, String>> tables = new HashMap<>();
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        conn.run("t = table(timestamp(1 2) as a,1 2 as b,blob(`a`b) as c,1.1 2.1 as d)\n" +
                "outTables.append!(t)");
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_BasicMessage_parse_outputTable_third_col_not_blob() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`sym`string`price1,[TIMESTAMP,STRING,STRING,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");

        Map<String, Pair<String, String>> tables = new HashMap<>();
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        conn.run("t = table(timestamp(1 2) as a,`x`y as b,`a`b as c,1.1 2.1 as d)\n" +
                "outTables.append!(t)");
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_BasicMessage_parse_outputTable_filter_col_not_exist() throws IOException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,STRING,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");

        Map<String, Pair<String, String>> tables = new HashMap<>();
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        conn.run("t = table(timestamp(1 2) as a,`x`y as b,blob(`a`b) as c,1.1 2.1 as d)\n" +
                "outTables.append!(t)");
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_filters_subscribe_isomate_table_StreamDeserializer_tableName_NULL() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,STRING,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", ""));
        tables.put("msg2", new Pair<>("", ""));
        try {
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            fail("no exception thrown");
        }catch(Exception ex){
            assertEquals(true,ex.getMessage().contains("schema() => The function [schema] expects 1 argument(s), but the actual number of arguments is: 0"));
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_filters_subscribe_isomate_table_StreamDeserializer_tableName_unexists() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        conn.run("st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,STRING,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "test1"));
        tables.put("msg2", new Pair<>("", "test2"));
        try {
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            fail("no exception thrown");
        }catch(Exception ex){
            assertEquals(true,ex.getMessage().contains("Cannot recognize the token test2"));
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_filters_subscribe_isomate_table_StreamDeserializer_tableNames_NULL() throws IOException, InterruptedException {
        try {
            StreamDeserializer streamFilter = new StreamDeserializer(null, conn);
            fail("no exception thrown");
        }catch(Exception ex){
            assertEquals("The tableNames_ is null. ",ex.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_filters_subscribe_isomate_table_StreamDeserializer_connect_NULL_error() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 1;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "table1"));
        tables.put("msg2", new Pair<>("", "table2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables,null );

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(5000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    class Handler_conn_null implements MessageHandler {
        private StreamDeserializer deserializer_;
        private List<BasicMessage> msg1 = new ArrayList<>();
        private List<BasicMessage> msg2 = new ArrayList<>();

        public Handler_conn_null(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void batchHandler(List<IMessage> msgs) {
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = (BasicMessage) msg;
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public List<BasicMessage> getMsg1() {
            return msg1;
        }

        public List<BasicMessage> getMsg2() {
            return msg2;
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_filters_subscribe_partition_table_StreamDeserializer_connect_NULL() throws Exception {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;" +
                "dbName = 'dfs://test_StreamDeserializer_pair';"+
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)}"+
                "db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01);"+
                "table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1);"+
                "pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2);"+
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("dfs://test_StreamDeserializer_pair", "pt1"));
        tables.put("msg2", new Pair<>("dfs://test_StreamDeserializer_pair", "pt2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, null);

        Handler_conn_null handler = new Handler_conn_null(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true,null,streamFilter,false,"admin","123456");
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_memory_table_filters_subscribe_isomate_table_write()throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "table1"));
        tables.put("msg2", new Pair<>("", "table2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }


    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_partition_table_filters_subscribe_isomate_table()throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;" +
                "dbName = 'dfs://test_StreamDeserializer_pair';"+
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)}"+
                "db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01);"+
                "table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1);"+
                "pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2);"+
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("dfs://test_StreamDeserializer_pair", "pt1"));
        tables.put("msg2", new Pair<>("dfs://test_StreamDeserializer_pair", "pt2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(5000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_stream_table_filters_subscribe_isomate_table_write()throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;" +
                "table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "stable1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "stable2 = streamTable(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "stable1"));
        tables.put("msg2", new Pair<>("", "stable2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(20000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    class Handler7 implements MessageHandler {
        private StreamDeserializer deserializer_;
        private List<BasicMessage> msg1 = new ArrayList<>();
        private List<BasicMessage> msg2 = new ArrayList<>();

        public Handler7(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void batchHandler(List<IMessage> msgs) {
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public List<BasicMessage> getMsg1() {
            return msg1;
        }

        public List<BasicMessage> getMsg2() {
            return msg2;
        }
    }
    @Test(timeout = 120000)
    public void test_StreamDeserializer_dataType_filters_subscribe_haStreamTable() throws IOException, InterruptedException {
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "t = table(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "haStreamTable(11,t,`outTables,100000)\t\n";
//                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "leader_node = getStreamingLeader(11)\n" +
                "rpc(leader_node,replay,d,`outTables,`timestampv,`timestampv)";
//        "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)"+
        conn.run(replayScript);
        Thread.sleep(5000);
        System.out.println(conn.run("select count(*) from outTables ").getString());
        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");
        Entity.DATA_TYPE[] array1 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE,DT_DOUBLE};
        Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE};
        List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
        List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
        HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
        filter.put("msg1",filter1);
        filter.put("msg2",filter2);

        StreamDeserializer streamFilter = new StreamDeserializer(filter);

        Handler7 handler = new Handler7(streamFilter);
        BasicString StreamLeaderTmp = (BasicString)conn.run("getStreamingLeader(11)");
        String StreamLeader = StreamLeaderTmp.getString();
        BasicString StreamLeaderHostTmp = (BasicString)conn.run("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\""+StreamLeader+"\")[0]");
        String StreamLeaderHost = StreamLeaderHostTmp.getString();
        BasicInt StreamLeaderPortTmp = (BasicInt)conn.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where name=\""+StreamLeader+"\")[0]");
        int StreamLeaderPort = StreamLeaderPortTmp.getInt();
        System.out.println(StreamLeaderHost);
        System.out.println(String.valueOf(StreamLeaderPort));
        client.subscribe(StreamLeaderHost, StreamLeaderPort, "outTables", "mutiSchema", handler, 0, true);
//        DBConnection conn1 = new DBConnection();
//        conn1.connect(HOST, 18922, "admin", "123456");
//        conn1.run("streamCampaignForLeader(3)");
        Thread.sleep(5000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Thread.sleep(5000);
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_StreamDeserializer_dataType_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");
        Entity.DATA_TYPE[] array1 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE,DT_DOUBLE};
        Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE};
        List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
        List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
        HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
        filter.put("msg1",filter1);
        filter.put("msg2",filter2);

        StreamDeserializer streamFilter = new StreamDeserializer(filter);

        Handler7 handler = new Handler7(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_ERROR_dataType_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");
        Entity.DATA_TYPE[] array1 = {DT_DOUBLE,DT_DOUBLE};
        Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_DOUBLE};
        List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
        List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
        HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
        filter.put("msg1",filter1);
        filter.put("msg2",filter2);

        StreamDeserializer streamFilter = new StreamDeserializer(filter);

        Handler7 handler = new Handler7(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_null_dataType_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");
        HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
        filter.put("msg1",null);
        filter.put("msg2",null);
        try {
            StreamDeserializer streamFilter = new StreamDeserializer(filter);
            fail("no exception thrown");
        }catch (Exception ex){
            assertEquals("The colTypes can not be null",ex.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_error_key_dataType_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        Entity.DATA_TYPE[] array1 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE,DT_DOUBLE};
        Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE};
        List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
        List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
        HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
        filter.put("msg3",filter1);
        filter.put(null,filter2);

        StreamDeserializer streamFilter = new StreamDeserializer(filter);

        Handler7 handler = new Handler7(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(0, msg1.size());
        Assert.assertEquals(0, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_NULL_dir_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        Map<String,BasicDictionary > tables = new HashMap<>();
        tables.put("msg1", null);
        tables.put("msg2", null);
        try {
            StreamDeserializer streamFilter = new StreamDeserializer(tables);
            fail("no exception thrown");
        }catch (Exception ex){
            assertEquals("The schema can not be null",ex.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_dir_filters_subscribe_isomate_table() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");
        BasicDictionary table1_schema = (BasicDictionary)conn.run("table1.schema()");
        BasicDictionary table2_schema = (BasicDictionary)conn.run("table2.schema()");
        Map<String,BasicDictionary > tables = new HashMap<>();
        tables.put("msg1", table1_schema);
        tables.put("msg2", table2_schema);
        StreamDeserializer streamFilter = new StreamDeserializer(tables);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        Thread.sleep(30000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_pair_stream_table_filters_subscribe_isomate_table_frequently_write()throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        conn.run("try{dropStreamTable(`outTables)}catch(ex){};" +
                "st11 = NULL");
        String script = "st11 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st11, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;" +
                "table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "stable1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "stable2 = streamTable(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "stable1"));
        tables.put("msg2", new Pair<>("", "stable2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);

        Handler6 handler = new Handler6(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0, true);
        for(int x = 0;x<1000;x++) {
            conn.run("table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                    "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                    "n= 10;tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                    "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                    "d = dict(['msg1','msg2'], [table1, table2]);" +
                    "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)");
        }
        Thread.sleep(40000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        Assert.assertEquals(20000, msg1.size());
        Assert.assertEquals(20000, msg2.size());
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test
    public void test_ThreadedClient_only_subscribePort1() throws IOException, InterruptedException {
        ThreadedClient client2 = new ThreadedClient(0);
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades1)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades1),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        client2.subscribe(HOST,PORT,"Trades1","subTrades1",MessageHandler_handler, -1, true);
       // Thread.sleep(100000000);
        client2.unsubscribe(HOST,PORT,"Trades1","subTrades1");
        client2.close();
    }
    public static MessageHandler Handler_array = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                msg.getEntity(0);
                String script = String.format("insert into sub1 values( %s,%s)", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' '));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_INT() throws IOException, InterruptedException {
        PrepareStreamTable("INT");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_BOOL() throws IOException, InterruptedException {
        PrepareStreamTable("BOOL");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_CHAR() throws IOException, InterruptedException {
        PrepareStreamTable("CHAR");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_SHORT() throws IOException, InterruptedException {
        PrepareStreamTable("SHORT");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_LONG() throws IOException, InterruptedException {
        PrepareStreamTable("LONG");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DOUBLE() throws IOException, InterruptedException {
        PrepareStreamTable("DOUBLE");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_FLOAT() throws IOException, InterruptedException {
        PrepareStreamTable("FLOAT");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DATE() throws IOException, InterruptedException {
        PrepareStreamTable("DATE");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_MONTH() throws IOException, InterruptedException {
        PrepareStreamTable("MONTH");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_TIME() throws IOException, InterruptedException {
        PrepareStreamTable("TIME");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_MINUTE() throws IOException, InterruptedException {
        PrepareStreamTable("MINUTE");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_SECOND() throws IOException, InterruptedException {
        PrepareStreamTable("SECOND");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DATETIME() throws IOException, InterruptedException {
        PrepareStreamTable("DATETIME");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_NANOTIME() throws IOException, InterruptedException {
        PrepareStreamTable("NANOTIME");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_NANOTIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable("NANOTIMESTAMP");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_UUID = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                msg.getEntity(0);
                String script = String.format("insert into sub1 values( %s,[uuid(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_UUID() throws IOException, InterruptedException {
        PrepareStreamTable("UUID");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_UUID, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_DATEHOUR = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                msg.getEntity(0);
                String script = String.format("insert into sub1 values( %s,[datehour(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DATEHOUR() throws IOException, InterruptedException {
        PrepareStreamTable("DATEHOUR");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_DATEHOUR, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_IPADDR = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                msg.getEntity(0);
                String script = String.format("insert into sub1 values( %s,[ipaddr(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_IPADDR() throws IOException, InterruptedException {
        PrepareStreamTable("IPADDR");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_IPADDR, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_INT128 = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                msg.getEntity(0);
                String script = String.format("insert into sub1 values( %s,[int128(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_INT128() throws IOException, InterruptedException {
        PrepareStreamTable("INT128");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_INT128, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_COMPLEX = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String complex1 = msg.getEntity(1).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
                System.out.println(complex1);
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split(",");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split("\\+");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                complex1 = complex1.replaceAll("i","");
                String script = String.format("insert into sub1 values( %s,[complex(%s)])", msg.getEntity(0).getString(), complex1);
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_COMPLEX() throws IOException, InterruptedException {
        PrepareStreamTable("COMPLEX");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_COMPLEX, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    public static MessageHandler Handler_array_POINT = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String dataType = msg.getEntity(1).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
                dataType = dataType.substring(1, dataType.length() - 1);
                String[] dataType1 = dataType.split("\\),\\(");
                String dataType2 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<dataType1.length;i++){
                    dataType2 = dataType1[i];
                    String[] dataType3 = dataType2.split(",");
                    re1.append(dataType3[0]);
                    re1.append(' ');
                    re2.append(dataType3[1]);
                    re2.append(' ');
                }
                dataType = re1+","+re2;
                dataType = dataType.replaceAll("\\(","").replaceAll("\\)","");
                String script = String.format("insert into sub1 values( %s,[point(%s)])", msg.getEntity(0).getString(), dataType);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_POINT() throws IOException, InterruptedException {
        PrepareStreamTable("POINT");
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_POINT, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DECIMAL32() throws IOException, InterruptedException {
        PrepareStreamTableDecimal("DECIMAL32",3);
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_TDECIMAL64() throws IOException, InterruptedException {
        PrepareStreamTableDecimal("DECIMAL64",4);
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadClient_subscribe_arrayVector_DECIMAL128() throws IOException, InterruptedException {
        PrepareStreamTableDecimal("DECIMAL128",7);
        ThreadedClient client = new ThreadedClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    static class Handler_StreamDeserializer_array implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
                String script = null;
                if (message.getSym().equals("msg1")) {
                     script = String.format("insert into sub1 values( %s,%s)", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                     script = String.format("insert into sub2 values( %s,%s)", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_BOOL()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_CHAR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("CHAR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_SHORT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SHORT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_LONG()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("LONG");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DOUBLE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DOUBLE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_FLOAT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("FLOAT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_MONTH()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MONTH");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_MINUTE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MINUTE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_SECOND()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SECOND");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATETIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DATETIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    static class Handler_StreamDeserializer_array_UUID implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_UUID(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[uuid(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[uuid(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_UUID()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("UUID");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_UUID handler = new Handler_StreamDeserializer_array_UUID(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    static class Handler_StreamDeserializer_array_DATEHOUR implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_DATEHOUR(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[datehour(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[datehour(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATEHOUR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DATEHOUR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_DATEHOUR handler = new Handler_StreamDeserializer_array_DATEHOUR(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    static class Handler_StreamDeserializer_array_IPADDR implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_IPADDR(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[ipaddr(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[ipaddr(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_IPADDR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("IPADDR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_IPADDR handler = new Handler_StreamDeserializer_array_IPADDR(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    static class Handler_StreamDeserializer_array_INT128 implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_INT128(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[int128(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[int128(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_INT128()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("INT128");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_INT128 handler = new Handler_StreamDeserializer_array_INT128(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    static class Handler_StreamDeserializer_array_COMPLEX implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_COMPLEX(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String complex1 = message.getEntity(1).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split(",");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split("\\+");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                String dataType = complex1.replaceAll("i","");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[complex(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[complex(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_COMPLEX()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("COMPLEX");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_COMPLEX handler = new Handler_StreamDeserializer_array_COMPLEX(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    static class Handler_StreamDeserializer_array_POINT implements MessageHandler {
        private StreamDeserializer deserializer_;
        public Handler_StreamDeserializer_array_POINT(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
                dataType = dataType.substring(1, dataType.length() - 1);
                String[] dataType1 = dataType.split("\\),\\(");
                String dataType2 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<dataType1.length;i++){
                    dataType2 = dataType1[i];
                    String[] dataType3 = dataType2.split(",");
                    re1.append(dataType3[0]);
                    re1.append(' ');
                    re2.append(dataType3[1]);
                    re2.append(' ');
                }
                dataType = re1+","+re2;
                dataType = dataType.replaceAll("\\(","").replaceAll("\\)","");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[complex(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[complex(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_POINT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("POINT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array_POINT handler = new Handler_StreamDeserializer_array_POINT(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL32()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL32",3);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL64()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL64",7);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL128()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL128",10);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        client.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        Thread.sleep(30000);
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
}