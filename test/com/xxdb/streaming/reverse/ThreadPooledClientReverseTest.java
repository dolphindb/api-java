package com.xxdb.streaming.reverse;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import org.javatuples.Pair;
import org.junit.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.xxdb.data.Entity.DATA_TYPE.*;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DOUBLE;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.*;
import static org.junit.Assert.*;

public class ThreadPooledClientReverseTest {
    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT = 9002;

    private static ThreadPooledClient client;
    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\tstopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)\n" +
                "}");
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
    }
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
            client = new ThreadPooledClient(HOST, 0,10);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{client.unsubscribe(HOST, PORT, "Trades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades2");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "javaStreamingApi");}catch (Exception ex){}
        clear_env();
    }

    @After
    public void after() throws IOException, InterruptedException {
        save_batch_size.clear();
        try{client.unsubscribe(HOST,PORT,"Trades","subTread1");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTread2");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTread3");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTrades1");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTrades2");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTrades3");}catch (Exception e){}
        try{client.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        clear_env();
        try {
            conn.run("login(`admin,`123456);" +
                    "try{dropStreamTable('Trades')}catch(ex){};"+
                    "try{dropStreamTable('Receive')}catch(ex){};"+
                    "try{deleteUser(`test1)}catch(ex){};" +
                    "userlist=getUserList();" +
                    "grouplist=getGroupList();" +
                    "loop(deleteUser,userlist);" +
                    "loop(deleteGroup,grouplist)");
        } catch (Exception e) {
        }
        client.close();
        conn.close();
    }

    @AfterClass
    public static void clear_conn() {
    }
//    @BeforeClass
//    public static void login() {
//        conn = new DBConnection();
//        try {
//            if (!conn.connect(HOST, PORT, "admin", "123456")) {
//                throw new IOException("Failed to connect to dolphindb server");
//            }
//            client = new ThreadPooledClient(HOST, 0,10);
//        } catch (IOException ex) {
//            ex.printStackTrace();
//        }
//    }
//    @Before
//    public void setUp() throws IOException {
//        try {
//            String script0 = "login(`admin,`123456);" +
//                    "try{dropStreamTable('Trades')}catch(ex){};"+
//                    "try{dropStreamTable('Receive')}catch(ex){};";
//            conn.run(script0);
//            String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
//                    "enableTableShareAndPersistence(table=st1, tableName=`Trades, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
//                    + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
//            conn.run(script1);
//            String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
//                    "enableTableShareAndPersistence(table=st2, tableName=`Receive, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
//            conn.run(script2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    @After
//    public void drop() throws IOException {
//        save_batch_size.clear();
//        try{client.unsubscribe(HOST,PORT,"Trades","subTread1");}catch (Exception e){}
//        try{client.unsubscribe(HOST,PORT,"Trades","subTread2");}catch (Exception e){}
//        try{client.unsubscribe(HOST,PORT,"Trades","subTread3");}catch (Exception e){}
//        try {
//            conn.run("login(`admin,`123456);" +
//                    "try{dropStreamTable('Trades')}catch(ex){};"+
//                    "try{dropStreamTable('Receive')}catch(ex){};"+
//                    "try{deleteUser(`test1)}catch(ex){};" +
//                    "userlist=getUserList();" +
//                    "grouplist=getGroupList();" +
//                    "loop(deleteUser,userlist);" +
//                    "loop(deleteGroup,grouplist)");
//        } catch (Exception e) {
//        }
//
//    }
//
//    @AfterClass
//    public static void after() throws IOException {
//        client.close();
//        conn.close();
//    }
public static void PrepareStreamTable() throws IOException {
    try {
        String script0 = "login(`admin,`123456);" +
                "try{dropStreamTable('Trades')}catch(ex){};"+
                "try{dropStreamTable('Receive')}catch(ex){};";
        conn.run(script0);
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st1, tableName=`Trades, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Receive, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
        conn.run(script2);
    } catch (IOException e) {
        e.printStackTrace();
    }
}
public static void PrepareStreamTable_array(String dataType) throws IOException {
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
    public static void PrepareStreamTableDecimal_array(String dataType, int scale) throws IOException {
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
        BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
        BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
        assertEquals(except.rows(), res.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
        }
    }
    public static void checkResult1() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1 ");
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2 ");
            if (tmpNum.getInt()==(1000)&& tmpNum1.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(2000);
        }
        BasicTable except = (BasicTable)conn.run("select * from  pub_t1 order by timestampv");
        BasicTable res = (BasicTable)conn.run("select * from  sub1 order by timestampv");
        BasicTable except1 = (BasicTable)conn.run("select * from  pub_t2 order by timestampv");
        BasicTable res1 = (BasicTable)conn.run("select * from  sub2 order by timestampv");
        assertEquals(except.rows(), res.rows());
        assertEquals(except1.rows(), res1.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
            assertEquals(except1.getColumn(i).getString(), res1.getColumn(i).getString());
        }
    }
    static class Handler7 implements MessageHandler {
        private StreamDeserializer deserializer_;
        private List<BasicMessage> msg1 = new ArrayList<>();
        private List<BasicMessage> msg2 = new ArrayList<>();
        private static Lock lock = new ReentrantLock();

        public Handler7(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }
        public void batchHandler(List<IMessage> msgs) {
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                lock.lock();
                try{
                    if (message.getSym().equals("msg1")) {
                        msg1.add(message);
                    } else if (message.getSym().equals("msg2")) {
                        msg2.add(message);
                    }
                    }catch(Exception e){
                        e.printStackTrace();
                    }finally{
                        lock.unlock();
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
    @Test
    public void test_ThreadPooledClient_host_error() throws IOException, InterruptedException {
        ThreadPooledClient client2 = new ThreadPooledClient("host_error",10022,10);
        client2.close();
    }

    @Test
    public void test_ThreadPooledClient_port_error() throws IOException {
        ThreadPooledClient client2 = new ThreadPooledClient(121,10);
        client2.close();
    }

    @Test
    public void test_ThreadPooledClient_HOST_subport() throws IOException {
        ThreadPooledClient client2 = new ThreadPooledClient(HOST,10022,10);
        client2.close();
    }

    @Test
    public void test_ThreadPooledClient_only_subscribePort() throws IOException {
        ThreadPooledClient client2 = new ThreadPooledClient(10012);
        client2.close();
    }
    @Test
    public void test_ThreadPooledClient_threadCount_not_true() throws IOException {
        String re = null;
        try{
            ThreadPooledClient client2 = new ThreadPooledClient(-1);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(re,"The 'threadCount' parameter cannot be less than or equal to zero.");
    }
    @Test
    public void test_ThreadPooledClient_threadCount_0() throws IOException {
        ThreadPooledClient client2 = new ThreadPooledClient(1000);
        client2.close();
    }

    @Test
    public void test_subscribe_ex1() throws Exception {
        PrepareStreamTable();
        client.subscribe(HOST, PORT, "Trades", "subtrades", MessageHandler_handler);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(40000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subtrades");
        conn.run("dropStreamTable('Trades');dropStreamTable('Receive')");
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }

    }
    @Test
    public void test_subscribe_ex2() throws Exception {
        PrepareStreamTable();
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, true);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(20000);
        client.unsubscribe(HOST, PORT, "Trades","javaStreamingApi");
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        Thread.sleep(20000);
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }

    }

    @Test
    public void test_subscribe_ofst0() throws Exception {
        PrepareStreamTable();
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            int ofst = 0;
            client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(30000);
            BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
            BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
            client.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
            Thread.sleep(5000);
            assertEquals(re.rows(), tra.rows());
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
                assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            }
    }

    @Test(expected = IOException.class)
    public void test_subscribe_ofst_negative2() throws IOException {
        PrepareStreamTable();
        int ofst = -2;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        try {
            Thread.sleep(2000);
            client.unsubscribe(HOST, PORT, "Trades");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_subscribe_ofst_negative1() throws IOException, InterruptedException {
        PrepareStreamTable();
        int ofst = -1;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(2000);
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
        assertEquals(3000, re.rows());
    }

    @Test
    public void test_subscribe_ofst_morethan0() throws Exception {
        PrepareStreamTable();
        int ofst = 10;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(2000);
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades");
        assertEquals(3090, re.rows());
    }

    @Test(expected = IOException.class)
    public void test_subscribe_ofst_morethan_tablecount() throws IOException {
        PrepareStreamTable();
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
    }

    @Test
    public void test_subscribe_filter() throws Exception {
        PrepareStreamTable();
        String script2 = "tmp3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=tmp3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
        conn.run(script2);
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
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        BasicTable fil = (BasicTable) conn.run("select * from filter order by tag");
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

    @Test
    public void test_subscribe_batchSize_throttle() throws Exception {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(2000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_batchSize_throttle2() throws Exception {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(200, re.rows());
        for (int i = 0; i <re.rows();i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_unsubscribe_resubscribe() throws Exception {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        for (int i=0;i<10;i++){
            client.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, true, filter1, true);
            client.subscribe(HOST, PORT, "Trades", "subTrades2", MessageHandler_handler, -1, true, filter1, true);
            client.subscribe(HOST, PORT, "Trades", "subTrades3", MessageHandler_handler, -1, true, filter1, true);
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(5200);
            client.unsubscribe(HOST, PORT, "Trades", "subTrades1");
            client.unsubscribe(HOST, PORT, "Trades", "subTrades2");
            client.unsubscribe(HOST, PORT, "Trades", "subTrades3");
            Thread.sleep(5200);
        }
    }

    @Test(expected = IOException.class)
    public void test_subscribe_user_error() throws IOException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin_error","123456");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_password_error() throws IOException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","error_password");
    }

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(20000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        PrepareStreamTable();
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(20000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_other_user_unallow() throws IOException {
        PrepareStreamTable();
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});");

        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void test_subscribe_other_some_user() throws IOException {
        PrepareStreamTable();
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};try{deleteUser(`test2)}catch(ex){};try{deleteUser(`test3)}catch(ex){};createUser(`test1, '123456');createUser(`test2, '123456');createUser(`test3, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades\"});");
        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test2", "123456");

        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        PrepareStreamTable();
        conn.run("login('admin','123456');def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        client.subscribe(HOST,PORT,"tmp_st1","subTread1",MessageHandler_handler,-1,true,null,true,"test1","123456");
        client.subscribe(HOST,PORT,"tmp_st2","subTread1",MessageHandler_handler,-1,true,null,true,"test1","123456");
        try {
            client.subscribe(HOST, PORT, "tmp_st3", "subTread1", MessageHandler_handler, -1, true, null, true, "test1", "123456_error");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage()+"12345666");
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        Thread.sleep(40000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"tmp_st1","subTread1");
        client.unsubscribe(HOST,PORT,"tmp_st2","subTread1");
    }

    @Test
    public void test_subscribe_tn_handler() throws IOException, InterruptedException {
        PrepareStreamTable();
        client.subscribe(HOST,PORT,"Trades",MessageHandler_handler);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(8000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades");
    }

    @Test
    public void test_subscribe_tn_an_hd_offset_reconnect_filter_de() throws Exception {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler, -1, true, filter1, null);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(2000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_tn_an_handler_reconnect() throws Exception {
        PrepareStreamTable();
        client.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(50000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(20000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_tn_handler_ofst_reconnect() throws Exception {
        PrepareStreamTable();
        client.subscribe(HOST, PORT, "Trades",MessageHandler_handler, -1,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(40000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades");
        assertEquals(20000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_ThreadPooledClient_null() throws Exception {
        PrepareStreamTable();
        ThreadPooledClient client1 = new ThreadPooledClient();
        client1.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(15000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client1.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(20000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
        client1.close();
    }

    @Test
    public void test_ThreadPooledClient_subPort_thCount() throws Exception {
        PrepareStreamTable();
        ThreadPooledClient client1 = new ThreadPooledClient(0,1);
        client1.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(40000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client1.unsubscribe(HOST, PORT, "Trades", "subTrades");

        assertEquals(20000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_ThreadedPoolClient_doReconnect() throws SocketException {
        class MyThreadPollClient extends ThreadPooledClient{

            public MyThreadPollClient(String subscribeHost, int subscribePort, int threadCount) throws SocketException {
                super(subscribeHost, subscribePort, threadCount);
            }

            @Override
            protected boolean doReconnect(Site site) {
                return super.doReconnect(site);
            }
        }
        MyThreadPollClient mtpc = new MyThreadPollClient(HOST,10086,10);
        assertFalse(mtpc.doReconnect(null));
    }

    @Test(timeout = 120000)
    public void test_StreamDeserializer_dataType_filters_subscribe_haStreamTable() throws IOException, InterruptedException {
        //PrepareStreamTable();
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
        client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "outTables", "mutiSchema");
    }
    @Test
    public void test_ThreadPooledClient_threadCount() throws Exception {
        PrepareStreamTable();
        client = new ThreadPooledClient(10);
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(2000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_INT() throws IOException, InterruptedException {
        PrepareStreamTable_array("INT");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_BOOL() throws IOException, InterruptedException {
        PrepareStreamTable_array("BOOL");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_CHAR() throws IOException, InterruptedException {
        PrepareStreamTable_array("CHAR");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_SHORT() throws IOException, InterruptedException {
        PrepareStreamTable_array("SHORT");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_LONG() throws IOException, InterruptedException {
        PrepareStreamTable_array("LONG");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DOUBLE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DOUBLE");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_FLOAT() throws IOException, InterruptedException {
        PrepareStreamTable_array("FLOAT");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DATE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATE");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_MONTH() throws IOException, InterruptedException {
        PrepareStreamTable_array("MONTH");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_TIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIME");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_MINUTE() throws IOException, InterruptedException {
        PrepareStreamTable_array("MINUTE");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_SECOND() throws IOException, InterruptedException {
        PrepareStreamTable_array("SECOND");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DATETIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATETIME");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_NANOTIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIME");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_NANOTIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIMESTAMP");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_UUID() throws IOException, InterruptedException {
        PrepareStreamTable_array("UUID");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_DATEHOUR() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATEHOUR");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_IPADDR() throws IOException, InterruptedException {
        PrepareStreamTable_array("IPADDR");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_INT128() throws IOException, InterruptedException {
        PrepareStreamTable_array("INT128");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
                //System.out.println(complex1);
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
    public void Test_ThreadPooledClient_subscribe_arrayVector_COMPLEX() throws IOException, InterruptedException {
        PrepareStreamTable_array("COMPLEX");
        ThreadPooledClient client = new ThreadPooledClient(10);
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
                String complex1 = msg.getEntity(1).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split("\\),\\(");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split(",");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                complex1 = complex1.replaceAll("\\(","").replaceAll("\\)","");
                String script = String.format("insert into sub1 values( %s,[point(%s)])", msg.getEntity(0).getString(), complex1);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_POINT() throws IOException, InterruptedException {
        PrepareStreamTable_array("POINT");
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array_POINT, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DECIMAL32() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL32",3);
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_TDECIMAL64() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL64",4);
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DECIMAL128() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL128",7);
        ThreadPooledClient client = new ThreadPooledClient(10);
        client.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades");
    }
    class Handler_StreamDeserializer_array implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_BOOL()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_CHAR()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_SHORT()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_LONG()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DOUBLE()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_FLOAT()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_MONTH()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIME()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_MINUTE()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_SECOND()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATETIME()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIMESTAMP()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIME()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIMESTAMP()throws IOException, InterruptedException {
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
    class Handler_StreamDeserializer_array_UUID implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_UUID()throws IOException, InterruptedException {
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
    class Handler_StreamDeserializer_array_DATEHOUR implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATEHOUR()throws IOException, InterruptedException {
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
    class Handler_StreamDeserializer_array_IPADDR implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_IPADDR()throws IOException, InterruptedException {
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
    class Handler_StreamDeserializer_array_INT128 implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_INT128()throws IOException, InterruptedException {
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

    class Handler_StreamDeserializer_array_COMPLEX implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_COMPLEX()throws IOException, InterruptedException {
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
    class Handler_StreamDeserializer_array_POINT implements MessageHandler {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_POINT()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL32()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL64()throws IOException, InterruptedException {
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
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL128()throws IOException, InterruptedException {
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