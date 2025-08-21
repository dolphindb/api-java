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

import static com.xxdb.Prepare.*;
import static com.xxdb.data.Entity.DATA_TYPE.*;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DOUBLE;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.*;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

public class ThreadPooledClientReverseTest {
    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT = 9002;
    static int COMPUTENODE = Integer.parseInt(bundle.getString("COMPUTENODE"));
    static long total = 0;

    private static ThreadPooledClient threadPooledClient;

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
            threadPooledClient = new ThreadPooledClient(HOST, 0,10);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1", "subTrades2");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1", "subTrades1");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1", "javaStreamingApi");}catch (Exception ex){}
        try {clear_env();}catch (Exception e){}
    }

    @After
    public void after() throws IOException, InterruptedException {
        save_batch_size.clear();
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread2");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread3");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTrades1");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTrades2");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTrades3");}catch (Exception e){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "Trades1", "subTrades");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{threadPooledClient.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        try{clear_env();}catch (Exception ex){}
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
        threadPooledClient.close();
        conn.close();
    }

    @AfterClass
    public static void clear_conn() {
        try {clear_env_1();}catch (Exception e){}
    }

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

    public static void checkResult() throws IOException, InterruptedException {
        for (int i = 0; i < 20; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            if (tmpNum.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(100);
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
        for (int i = 0; i < 20; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1 ");
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2 ");
            if (tmpNum.getInt()==(1000)&& tmpNum1.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(100);
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient("host_error",10022,10);
        threadPooledClient.close();
    }

    @Test
    public void test_ThreadPooledClient_port_error() throws IOException {
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(121,10);
        threadPooledClient.close();
    }

    @Test
    public void test_ThreadPooledClient_HOST_subport() throws IOException {
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(HOST,10022,10);
        threadPooledClient.close();
    }

    @Test
    public void test_ThreadPooledClient_only_subscribePort() throws IOException {
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10012);
        threadPooledClient.close();
    }
    @Test
    public void test_ThreadPooledClient_threadCount_not_true() throws IOException {
        String re = null;
        try{
            ThreadPooledClient threadPooledClient = new ThreadPooledClient(-1);
        }catch(Exception e){
            re = e.getMessage();
        }
        Assert.assertEquals(re,"The 'threadCount' parameter cannot be less than or equal to zero.");
    }
    @Test
    public void test_ThreadPooledClient_threadCount_0() throws IOException {
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(1000);
        threadPooledClient.close();
    }

    @Test
    public void test_subscribe_ex1() throws Exception {
        PrepareStreamTable();
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subtrades", MessageHandler_handler);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(100);
        wait_data("Receive",1000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subtrades");
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
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, true);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(500);
        wait_data("Receive",5000);
        threadPooledClient.unsubscribe(HOST, PORT, "Trades","javaStreamingApi");
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
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
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        wait_data("Receive",20000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
             assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
             assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
             assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_ofst_negative2() throws IOException {
        PrepareStreamTable();
        int ofst = -2;
        String re = null;
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
//        try {
//            threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
//        } catch (Exception ex) {
//            re = ex.getMessage();
//        }
//        assertEquals(true, re.contains("Can't find the message with offset [-2]"));
    }

    @Test
    public void test_subscribe_ofst_negative1() throws IOException, InterruptedException {
        PrepareStreamTable();
        int ofst = -1;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",3000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
        assertEquals(3000, re.rows());
    }

    @Test
    public void test_subscribe_ofst_morethan0() throws Exception {
        PrepareStreamTable();
        int ofst = 10;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",3090);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
        assertEquals(3090, re.rows());
    }

    @Test
    public void test_subscribe_ofst_morethan_tablecount() throws IOException {
        PrepareStreamTable();
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        String re = null;
        try {
            threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        } catch (Exception ex) {
            re = ex.getMessage();
        }
        assertEquals(true, re.contains("Can't find the message with offset [1000]"));

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
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, filter1);
        Vector filter2 = (Vector) conn.run("2001..3000");
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades2", handler1, -1, filter2);
        conn.run("n=4000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive", 1000);
        wait_data("filter", 1000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        BasicTable fil = (BasicTable) conn.run("select * from filter order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades2");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades1");
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
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
        System.out.println("行数：" + re.rows());
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
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",200);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
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
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, true, filter1, true);
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades2", MessageHandler_handler, -1, true, filter1, true);
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades3", MessageHandler_handler, -1, true, filter1, true);
            conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(1500);
            threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades1");
            threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades2");
            threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades3");
            Thread.sleep(1500);
        }
    }

    @Test
    public void test_subscribe_user_error() throws IOException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        String re = null;
        try{
            threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin_error","123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("The user name or password is incorrect"));
    }

    @Test
    public void test_subscribe_password_error() throws IOException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        String re = null;
        try{
            threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","error_password");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("The user name or password is incorrect"));
    }

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..100000");
        threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        PrepareStreamTable();
        PrepareUser("test1","123456");
        Vector filter1 = (Vector) conn.run("1..100000");
        threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_authMode_scream() throws IOException, InterruptedException {
        PrepareStreamTable();
        PrepareUser_authMode("scramUser","123456","scram");
        Vector filter1 = (Vector) conn.run("1..100000");
        threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"scramUser","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_other_user_unallow() throws IOException {
        PrepareStreamTable();
        PrepareUser("test1","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});");
        Vector filter1 = (Vector) conn.run("1..100000");
        String re = null;
        try {
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test1", "123456");
        }catch (Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("No access to shared table [Trades]"));
    }

    @Test
    public void test_subscribe_other_some_user() throws IOException {
        PrepareStreamTable();
        PrepareUser("test1","123456");
        PrepareUser("test2","123456");
        PrepareUser("test3","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades\"});");
        Vector filter1 = (Vector) conn.run("1..100000");
        String re = null;
        try {
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test1", "123456");
        }catch (Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("No access to shared table [Trades]"));

        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, "test2", "123456");
        String re1 = null;
        try {
            threadPooledClient.subscribe(HOST, PORT, "Trades", "subTread2", MessageHandler_handler, -1, true, filter1, true, "test3", "123456");
        }catch (Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals(true, re1.contains("No access to shared table [Trades]"));
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        PrepareStreamTable();
        PrepareUser("test1","123456");
        conn.run("share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        threadPooledClient.subscribe(HOST,PORT,"tmp_st1","subTread1",MessageHandler_handler,-1,true,null,true,"test1","123456");
        threadPooledClient.subscribe(HOST,PORT,"tmp_st2","subTread1",MessageHandler_handler,-1,true,null,true,"test1","123456");
        String re = null;
        try {
            threadPooledClient.subscribe(HOST, PORT, "tmp_st3", "subTread1", MessageHandler_handler, -1, true, null, true, "test1", "123456_error");
        }catch (Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("The user name or password is incorrect."));

        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        wait_data("Receive",20000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        threadPooledClient.unsubscribe(HOST,PORT,"tmp_st1","subTread1");
        threadPooledClient.unsubscribe(HOST,PORT,"tmp_st2","subTread1");
    }

    @Test
    public void test_subscribe_tn_handler() throws IOException, InterruptedException {
        PrepareStreamTable();
        threadPooledClient.subscribe(HOST,PORT,"Trades",MessageHandler_handler);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",10000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades");
    }

    @Test
    public void test_subscribe_tn_an_hd_offset_reconnect_filter_de() throws Exception {
        PrepareStreamTable();
        Vector filter1 = (Vector) conn.run("1..1000");
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler, -1, true, filter1, null);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
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
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",20000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
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
        threadPooledClient.subscribe(HOST, PORT, "Trades",MessageHandler_handler, -1,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",20000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient();
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",20000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(20000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 1000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 1000));
            assertEquals(((Scalar)re.getColumn(2).get(i + 1000)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i + 1000)).getNumber().doubleValue(), 4);
        }
        threadPooledClient.close();
    }

    @Test
    public void test_ThreadPooledClient_subPort_thCount() throws Exception {
        PrepareStreamTable();
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(0,1);
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",20000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
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

        threadPooledClient.subscribe(StreamLeaderHost, StreamLeaderPort, "outTables", "mutiSchema", handler, 0, true);
//        DBConnection conn1 = new DBConnection();
//        conn1.connect(HOST, 18922, "admin", "123456");
//        conn1.run("streamCampaignForLeader(3)");
        List<BasicMessage> msg1 = null;
        List<BasicMessage> msg2 = null;
        for(int i=0;i<10;i++){
            msg1 = handler.getMsg1();
            msg2 = handler.getMsg2();
            if(msg1.size() == 10000){
                break;
            }
            Thread.sleep(100);
            i++;
        }
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        threadPooledClient.unsubscribe(StreamLeaderHost, StreamLeaderPort, "outTables", "mutiSchema");
    }
    @Test
    public void test_ThreadPooledClient_threadCount() throws Exception {
        PrepareStreamTable();
        threadPooledClient = new ThreadPooledClient(10);
        Vector filter1 = (Vector) conn.run("1..1000");
        threadPooledClient.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",2000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        threadPooledClient.unsubscribe(HOST, PORT, "Trades", "subTrades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_BOOL() throws IOException, InterruptedException {
        PrepareStreamTable_array("BOOL");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_CHAR() throws IOException, InterruptedException {
        PrepareStreamTable_array("CHAR");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_SHORT() throws IOException, InterruptedException {
        PrepareStreamTable_array("SHORT");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_LONG() throws IOException, InterruptedException {
        PrepareStreamTable_array("LONG");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DOUBLE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DOUBLE");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_FLOAT() throws IOException, InterruptedException {
        PrepareStreamTable_array("FLOAT");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DATE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATE");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_MONTH() throws IOException, InterruptedException {
        PrepareStreamTable_array("MONTH");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_TIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIME");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_MINUTE() throws IOException, InterruptedException {
        PrepareStreamTable_array("MINUTE");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_SECOND() throws IOException, InterruptedException {
        PrepareStreamTable_array("SECOND");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DATETIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATETIME");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_NANOTIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIME");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_NANOTIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIMESTAMP");
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_UUID, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_DATEHOUR, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_IPADDR, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_INT128, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_COMPLEX, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array_POINT, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DECIMAL32() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL32",3);
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_TDECIMAL64() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL64",4);
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }
    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_arrayVector_DECIMAL128() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL128",7);
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(10);
        threadPooledClient.subscribe(HOST, PORT, "Trades", Handler_array, -1);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        //write 1000 rows after subscribe
        checkResult();
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_CHAR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("CHAR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_SHORT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SHORT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_LONG()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("LONG");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DOUBLE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DOUBLE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_FLOAT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("FLOAT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_MONTH()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MONTH");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_MINUTE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MINUTE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_SECOND()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SECOND");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATETIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DATETIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
                    script = String.format("insert into sub1 values( %s,[point(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[point(%s)])", timestampv,dataType);
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
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL32()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL32",3);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL64()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL64",7);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_ThreadPooledClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL128()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL128",10);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_backupSites_port_not_true()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        threadPooledClient.subscribe(HOST, 11111, "outTables", "mutiSchema", handler, 0,false, null,null,false,"admin","123456",false, backupSites,10,true);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, 11111, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_backupSites_not_true()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+","+PORT));
        String re = null;
        try{
            threadPooledClient.subscribe(HOST, 11111, "outTables", "mutiSchema", handler, 0,false, null,null,false,"admin","123456",false,backupSites,10,true);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The format of backupSite "+HOST+","+PORT+" is incorrect, should be host:port, e.g. 192.168.1.1:8848", re);
    }

    @Test(timeout = 120000)
    public void Test_ThreadPooledClient_subscribe_backupSites_StreamDeserializer()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        threadPooledClient.subscribe(HOST, PORT, "outTables", "mutiSchema", handler, 0,false, null,null,false,"admin","123456",false,backupSites,10,true);
        //Thread.sleep(30000);
        checkResult1();
        threadPooledClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 180000)
    public void Test_ThreadPooledClient_subscribe_backupSites() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+111));
        threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler, -1,true,filter1, (StreamDeserializer) null,true,"admin","123456",false,backupSites,10,true);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }


    @Test(timeout = 180000)
    public void Test_ThreadPooledClient_subscribe_backupSites_unsubscribe() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        threadPooledClient.subscribe(HOST,11111,"Trades","subTread1",MessageHandler_handler, -1,true,filter1, (StreamDeserializer) null,true,"admin","123456",false,backupSites,10,false);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        threadPooledClient.unsubscribe(HOST,11111,"Trades","subTread1");
    }
    @Test(timeout = 180000)
    public void Test_ThreadPooledClient_subscribe_resubscribeInterval_not_true() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+111));
        threadPooledClient.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler, -1,true,filter1, (StreamDeserializer) null,true,"admin","123456",false,backupSites,-1,true);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        threadPooledClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    public static MessageHandler MessageHandler_handler_getOffset = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                conn.run(script);
                System.out.println("msg.getOffset is :" + msg.getOffset());
                System.out.println("total is :" + total);
                assertEquals(total, msg.getOffset());
                total++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 180000)
    public void test_subscribe_getOffset() throws Exception{
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        ThreadPooledClient threadPooledClient = new ThreadPooledClient(HOST, 0,1);
        threadPooledClient.subscribe(HOST, PORT, "Trades", MessageHandler_handler_getOffset, true);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        wait_data("Receive",1000);
        BasicTable re = (BasicTable) conn.run("select * from Receive");
        BasicTable tra = (BasicTable) conn.run("select * from Trades");
        assertEquals(1000, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }
        threadPooledClient.unsubscribe(HOST, PORT, "Trades");
    }

//    public static MessageHandler MessageHandler_handler_null = new MessageHandler() {
//        @Override
//        public void doEvent(IMessage msg) {
//            try {
//                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
//                DBConnection conn = new DBConnection();
//                conn.connect(HOST, PORT, "admin", "123456");
//                conn.run(script);
//                System.out.println(msg.getEntity(0).getString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    };
//    @Test(timeout = 180000)
//    public void test_subscribe_null() throws Exception{
//        DBConnection conn = new DBConnection();
//        conn.connect(HOST, PORT, "admin", "123456");
//        String script0 = "login(`admin,`123456);" +
//                "try{undef(`quote_commodity_stream, SHARED);}catch(ex){};"+
//                "try{undef(`Receive, SHARED);}catch(ex){};"+
//                "try{dropStreamTable('quote_commodity_stream')}catch(ex){};"+
//                "try{dropStreamTable('Receive')}catch(ex){};";
//        conn.run(script0);
//        String script1 = "quote_commodity_ts = streamTable(100:0, [`time, `sym, `exch_time, `exch, `source, `broker, `tot_notional, `tot_sz, `last_px, `bid_sz1, `bid_px1, `ask_px1, `ask_sz1, `open, `high, `low, `close, `pre_close, `oi, `pre_oi, `settlement_px, `pre_settlement_px, `rec_time, `pub_time, `bid_sz2, `bid_px2, `ask_px2, `ask_sz2, `bid_sz3, `bid_px3, `ask_px3, `ask_sz3, `bid_sz4, `bid_px4, `ask_px4, `ask_sz4, `bid_sz5, `bid_px5, `ask_px5, `ask_sz5, `upper_limit, `lower_limit, `pre_delta, `curr_delta, `trading_date, `settlement_id, `settlement_group_id], [NANOTIMESTAMP, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, SYMBOL, LONG, LONG, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, NANOTIMESTAMP, NANOTIMESTAMP, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DATE, LONG, LONG])\n" +
//                "enableTableShareAndPersistence(table=quote_commodity_ts, tableName=`quote_commodity_stream, cacheSize=50000, preCache = 50000,retentionMinutes=1440)";
//        conn.run(script1);
//        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
//                "enableTableShareAndPersistence(table=st2, tableName=`Receive, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
//        conn.run(script2);
//        ThreadPooledClient threadPooledClient = new ThreadPooledClient(HOST,PORT,3);
//        System.out.println("client created");
//        threadPooledClient.subscribe(HOST, PORT, "quote_commodity_stream", "subTrades", MessageHandler_handler_null, -1, true);
//        System.out.println("client created");
//    }

    public static MessageHandler MessageHandler_handler_orca = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                List<Entity> args = new ArrayList<>();
                args.add(msg.getEntity(0));
                args.add(msg.getEntity(1));
                args.add(msg.getEntity(2));
                conn1.run("tableInsert{Receive}", args);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };

    @Test(timeout = 180000)
    public void test_threadPooledClient_subscribe_orca_table() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST, COMPUTENODE,"admin","123456");
        String script1 = "if (existsCatalog(\"orca\")) {\n" +
                "\tdropCatalog(\"orca\")\n" +
                "}\n" +
                "go\n" +
                "createCatalog(\"orca\")\n" +
                "go\n" +
                "use catalog orca\n" +
                "g = createStreamGraph('engine')\n" +
                "g.source(\"trades\", 1000:0, [\"time\",\"sym\",\"volume\"], [TIMESTAMP, SYMBOL, INT])\n" +
                ".timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn=\"time\", useSystemTime=false, keyColumn=\"sym\", useWindowStartTime=false)\n" +
                ".sink(\"output\")\n" +
                "g.submit()\n" +
                "go\n" +
                "times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]\n" +
                "syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]\n" +
                "volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]\n" +
                "\n" +
                "tmp = table(times as time, syms as sym, volumes as volume)\n" +
                "appendOrcaStreamTable(\"trades\", tmp)";
        conn.run(script1);
        BasicTable port = (BasicTable)conn.run("select port from getClusterPerf() where name = (exec site from getOrcaStreamTableMeta() where fqn = \"orca.orca_table.output\")");
        int port1 = Integer.valueOf(port.getColumn(0).get(0).getString());
        conn1 = new DBConnection();
        conn1.connect(HOST,port1 ,"admin", "123456");
        String script2 = "share table(1:0,`time`sym`volume, [TIMESTAMP, STRING, INT]) as Receive;";
        conn1.run(script2);

        threadPooledClient.subscribe(HOST, port1, "orca.orca_table.output", MessageHandler_handler_orca,true);
        conn1.run("appendOrcaStreamTable( \"orca.orca_table.output\", table(timestamp(1..10) as time,take(`a`s`q,10) as sym, 1..10 as volume))");
        //wait_data("Receive",4);
        sleep(800);
        BasicTable re = (BasicTable) conn1.run("select * from Receive order by time,sym,volume");
        BasicTable tra = (BasicTable) conn1.run("select * from  orca.orca_table.output order by time,sym,sum_volume");
        assertEquals(14, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(re.getColumn(2).get(i).getString(), tra.getColumn(2).get(i).getString());
        }
        threadPooledClient.unsubscribe(HOST, port1, "orca.orca_table.output");
    }

    @Test(timeout = 180000)
    public void test_threadPooledClient_subscribe_orca_table_not_orca_node() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(HOST, COMPUTENODE,"admin","123456");
        String script1 = "if (existsCatalog(\"orca\")) {\n" +
                "\tdropCatalog(\"orca\")\n" +
                "}\n" +
                "go\n" +
                "createCatalog(\"orca\")\n" +
                "go\n" +
                "use catalog orca\n" +
                "g = createStreamGraph('engine')\n" +
                "g.source(\"trades\", 1000:0, [\"time\",\"sym\",\"volume\"], [TIMESTAMP, SYMBOL, INT])\n" +
                ".timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn=\"time\", useSystemTime=false, keyColumn=\"sym\", useWindowStartTime=false)\n" +
                ".sink(\"output\")\n" +
                "g.submit()\n" +
                "go\n" +
                "times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]\n" +
                "syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]\n" +
                "volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]\n" +
                "\n" +
                "tmp = table(times as time, syms as sym, volumes as volume)\n" +
                "appendOrcaStreamTable(\"trades\", tmp)";
        conn.run(script1);
        BasicTable port = (BasicTable)conn.run("select port from getClusterPerf() where name != (exec site from getOrcaStreamTableMeta() where fqn = \"orca.orca_table.output\") and mode = 0 limit 1");
        int port1 = Integer.valueOf(port.getColumn(0).get(0).getString());
        conn1 = new DBConnection();
        conn1.connect(HOST,port1 ,"admin", "123456");
        String script2 = "share table(1:0,`time`sym`volume, [TIMESTAMP, STRING, INT]) as Receive;";
        conn1.run(script2);

        threadPooledClient.subscribe(HOST, port1, "orca.orca_table.output", MessageHandler_handler_orca,true);
        conn1.run("appendOrcaStreamTable( \"orca.orca_table.output\", table(timestamp(1..10) as time,take(`a`s`q,10) as sym, 1..10 as volume))");

        //wait_data("Receive",4);
        sleep(500);
        BasicTable re = (BasicTable) conn1.run("select * from Receive order by time,sym,volume");
        BasicTable tra = (BasicTable) conn1.run("select * from  orca.orca_table.output order by time,sym,sum_volume");
        assertEquals(14, re.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(re.getColumn(2).get(i).getString(), tra.getColumn(2).get(i).getString());
        }
        threadPooledClient.unsubscribe(HOST, port1, "orca.orca_table.output");
    }
}