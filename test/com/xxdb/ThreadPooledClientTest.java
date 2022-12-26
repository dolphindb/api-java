package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Scalar;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import org.junit.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class ThreadPooledClientTest {
    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT = 9002;

    private static ThreadPooledClient client;

    @BeforeClass
    public static void login() {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
            client = new ThreadPooledClient(HOST, 8676,10);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
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

    @After
    public void drop() throws IOException {
        save_batch_size.clear();
        try{client.unsubscribe(HOST,PORT,"Trades","subTread1");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTread2");}catch (Exception e){}
        try{client.unsubscribe(HOST,PORT,"Trades","subTread3");}catch (Exception e){}
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

    }

    @AfterClass
    public static void after() throws IOException {
        client.close();
        conn.close();
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
    public void test_ThreadedClient_HOST_host_error() throws IOException, InterruptedException {
        ThreadedClient client2 = new ThreadedClient("host_error",10022);
        client2.close();
    }

    @Test
    public void test_ThreadedClient_HOST_port_error() throws IOException {
        ThreadedClient client2 = new ThreadedClient("host_error",0);
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
        ThreadedClient client2 = new ThreadedClient(10012);
        client2.close();
    }

    @Test
    public void test_subscribe_ex1() throws Exception {
            client.subscribe(HOST, PORT, "Trades", "subtrades", MessageHandler_handler);
            conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(20000);
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
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, true);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(10000);
        client.unsubscribe(HOST, PORT, "Trades","javaStreamingApi");
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        Thread.sleep(12000);
        assertEquals(re.rows(), tra.rows());
        for (int i = 0; i < re.rows(); i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(((Scalar)re.getColumn(2).get(i)).getNumber().doubleValue(), ((Scalar)tra.getColumn(2).get(i)).getNumber().doubleValue(), 4);
        }

    }

    @Test
    public void test_subscribe_ofst0() throws Exception {
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            int ofst = 0;
            client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(20000);
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
        int ofst = -1;
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(2000);
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
        assertEquals(3000, re.rows());
    }

    @Test
    public void test_subscribe_ofst_morethan0() throws Exception {
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
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
    }

    @Test
    public void test_subscribe_filter() throws Exception {
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
        Thread.sleep(5000);
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
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
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
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
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
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin_error","123456");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_password_error() throws IOException {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","error_password");
    }

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
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
    public void test_subscribe_other_some_user() throws IOException, InterruptedException {
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
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
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
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        Thread.sleep(20000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"tmp_st1","subTread1");
        client.unsubscribe(HOST,PORT,"tmp_st2","subTread1");
    }

    @Test
    public void test_subscribe_tn_handler() throws IOException, InterruptedException {
        client.subscribe(HOST,PORT,"Trades",MessageHandler_handler);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades");
    }

    @Test
    public void test_subscribe_tn_an_hd_offset_reconnect_filter_de() throws Exception {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler, -1, true, filter1, null);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
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
        client.subscribe(HOST, PORT, "Trades",MessageHandler_handler, -1,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(30000);
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
        ThreadPooledClient client1 = new ThreadPooledClient();
        client1.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicTable re = (BasicTable) conn.run("select * from Receive order by tag");
        BasicTable tra = (BasicTable) conn.run("select * from Trades order by tag");
        client1.unsubscribe(HOST, PORT, "Trades", "subTrades");
        //assertEquals(20000, re.rows());
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
        ThreadPooledClient client1 = new ThreadPooledClient(10002,10);
        client1.subscribe(HOST, PORT, "Trades", "subTrades",MessageHandler_handler,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(20000);
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
}