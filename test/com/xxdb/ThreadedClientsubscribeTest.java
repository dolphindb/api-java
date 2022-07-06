package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.BatchMessageHandler;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThreadedClientsubscribeTest {
    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    private static ThreadedClient client;

    @BeforeClass
    public static void login() {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
            client = new ThreadedClient(HOST, 8676);
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
    public void test_subscribe_ex1() throws IOException {
        try {
            client.subscribe(HOST, PORT, "Trades", "subtrades", MessageHandler_handler);
            conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(10000);
            BasicTable re = (BasicTable) conn.run("Receive");
            BasicTable tra = (BasicTable) conn.run("Trades");
            client.unsubscribe(HOST, PORT, "Trades", "subtrades");
            Thread.sleep(10000);
            conn.run("dropStreamTable('Trades');dropStreamTable('Receive')");
            assertEquals(re.rows(), tra.rows());
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
                assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void test_subscribe_ex2() throws IOException {

        try {
            client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, true);
            conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(10000);
            client.unsubscribe(HOST, PORT, "Trades","javaStreamingApi");
            BasicTable re = (BasicTable) conn.run("Receive");
            BasicTable tra = (BasicTable) conn.run("Trades");
            Thread.sleep(12000);
            assertEquals(re.rows(), tra.rows());
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
                assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_subscribe_ofst0() throws IOException {

        try {
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            int ofst = 0;
            client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(10000);
            BasicTable re = (BasicTable) conn.run("Receive");
            BasicTable tra = (BasicTable) conn.run("Trades");
            client.unsubscribe(HOST, PORT, "Trades", "javaStreamingApi");
            Thread.sleep(2000);
            assertEquals(re.rows(), tra.rows());
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
                assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
            }

        } catch (Exception e) {
            e.printStackTrace();
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
    public void test_subscribe_ofst_negative1() throws IOException {
        try {
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
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i + 100));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i + 100));
                assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i + 100).getNumber().doubleValue(), 4);
            }
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_subscribe_ofst_morethan0() throws IOException {

        try {
            int ofst = 10;
            conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(2000);
            client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
            conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(5000);
            BasicTable re = (BasicTable) conn.run("Receive");
            BasicTable tra = (BasicTable) conn.run("Trades");
            client.unsubscribe(HOST, PORT, "Trades");
            assertEquals(3090, re.rows());
            for (int i = 0; i < re.rows(); i++) {
                assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i + 10));
                assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i + 10));
                assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i + 10).getNumber().doubleValue(), 4);
            }
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(expected = IOException.class)
    public void test_subscribe_ofst_morethan_tablecount() throws IOException {
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        client.subscribe(HOST, PORT, "Trades", MessageHandler_handler, ofst);
    }

    @Test
    public void test_subscribe_filter() throws Exception {
        String script2 = "st3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
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
            assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
        }

        for (int i = 0; i < fil.rows(); i++) {
            assertEquals(fil.getColumn(0).get(i), tra.getColumn(0).get(i + 2000));
            assertEquals(fil.getColumn(1).get(i), tra.getColumn(1).get(i + 2000));
            assertEquals(fil.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i + 2000).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_batchSize_throttle() throws Exception {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(2000, re.rows());
        for (int i = 0; i < 1000; i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
            assertEquals(re.getColumn(0).get(i + 1000), tra.getColumn(0).get(i + 10000));
            assertEquals(re.getColumn(1).get(i + 1000), tra.getColumn(1).get(i + 10000));
            assertEquals(re.getColumn(2).get(i + 1000).getNumber().doubleValue(), tra.getColumn(2).get(i + 10000).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_batchSize_throttle2() throws Exception {
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicTable re = (BasicTable) conn.run("Receive");
        BasicTable tra = (BasicTable) conn.run("Trades");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades");
        assertEquals(200, re.rows());
        for (int i = 0; i <re.rows();i++) {
            assertEquals(re.getColumn(0).get(i), tra.getColumn(0).get(i));
            assertEquals(re.getColumn(1).get(i), tra.getColumn(1).get(i));
            assertEquals(re.getColumn(2).get(i).getNumber().doubleValue(), tra.getColumn(2).get(i).getNumber().doubleValue(), 4);
        }
    }

    @Test
    public void test_subscribe_unsubscribe_resubscribe() throws Exception {
        Vector filter1 = (Vector) conn.run("1..1000");
        for (int i=0;i<10;i++){
        client.subscribe(HOST, PORT, "Trades", "subTrades1", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades2", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades3", MessageHandler_handler, -1, true, filter1, true, 10000, 5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5200);
        client.unsubscribe(HOST, PORT, "Trades", "subTrades1");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades2");
        client.unsubscribe(HOST, PORT, "Trades", "subTrades3");
        Thread.sleep(5200);
        }
    }

    @Test
    public void test_subscribe_user_error() throws IOException {
        Vector filter1 = (Vector) conn.run("1..1000");
        try{
            client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin_error","123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
    }

    @Test
    public void test_subscribe_password_error() throws IOException {
        Vector filter1 = (Vector) conn.run("1..1000");
        try{
            client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin","error_password");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
    }

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_other_user_allow_unsubscribe_login() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "grant(`test1,TABLE_READ,`Trades)");
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "deny(`test1,TABLE_READ,`Trades)");

        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]' script: 'publishTable'",e.getMessage());
        }
    }

    @Test
    public void test_subscribe_other_some_user() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "createUser(`test2, '123456');" +
                "createUser(`test3, '123456');" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "deny(`test1,TABLE_READ,`Trades);" +
                "grant(`test2,TABLE_READ,`Trades);");
        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]' script: 'publishTable'",e.getMessage());
        }

        client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test2", "123456");

        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]' script: 'publishTable'",e.getMessage());
        }

    }

    @Test
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        client.subscribe(HOST,PORT,"tmp_st1","subTread1",MessageHandler_handler,-1,true,null,true,100,5,"test1","123456");
        client.subscribe(HOST,PORT,"tmp_st2","subTread1",MessageHandler_handler,-1,true,null,true,100,5,"test1","123456");
        try {
            client.subscribe(HOST, PORT, "tmp_st3", "subTread1", MessageHandler_handler, -1, true, null, true, 100, 5, "test1", "123456_error");
            fail("no exception thrown");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"tmp_st1","subTread1");
        client.unsubscribe(HOST,PORT,"tmp_st2","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,1024,5);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(1024,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_not_set_batchSize() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(true,save_batch_size.isEmpty());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_single_msg() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100,1);
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(1,row_num.getInt());
        assertEquals(1,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_mul_single_msg() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100,100000);
        for(int n = 0;n<10000;n++) {
            conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        }
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(100,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_batchSize_over_meg_size() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100000,100);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(10000,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_batchSize_over_meg_size_small_throttle() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,100000,1);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
        assertEquals(1024,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_batchSize_time_interval_over_throttle() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",BatchMessageHandler_handler,-1,true,filter1,true,1024,2);
        for(int x = 0;x<10;x++) {
            conn.run("n=10;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(3000);
        }
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        System.out.println(save_batch_size.toString());
        assertEquals(10000,row_num.getInt());
        assertEquals(10,save_batch_size.toArray()[0]);
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_func_BatchMessageHandler_mul_subscribe() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST, PORT, "Trades", "subTread1", BatchMessageHandler_handler, -1, true, filter1, true, 1000, 2);
        client.subscribe(HOST, PORT, "Trades", "subTread2", BatchMessageHandler_handler, -1, true, filter1, true, 1000, 2);
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt) conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000, row_num.getInt());
        assertEquals(1024, save_batch_size.toArray()[0]);
        assertEquals(20, save_batch_size.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
        client.unsubscribe(HOST,PORT,"Trades","subTread2");
    }

    public class MyThread extends Thread {
        @Override
        public void run() {
            try {
                while (true) {
                    conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                    Thread.sleep(4000);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    @Test
    public void test_subscribe_reconnect_successful() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",MessageHandler_handler,-1,true,filter1,true,100,5,"admin","123456");
        System.out.println("Successful subscribe");
        MyThread write_data  = new MyThread ();
        write_data.start();
        Thread.sleep(2000);
        conn.run("stopPublishTable('192.168.1.32',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        conn.run("stopPublishTable('192.168.1.32',8676,'Trades','subTread1')");
        Thread.sleep(2000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_reconnect_fail() throws IOException, InterruptedException {
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST, PORT, "Trades", "subTread1", MessageHandler_handler, -1, true, filter1, true, 100, 5, "admin", "123456");
        System.out.println("Successful subscribe");
        Thread.sleep(2000);
        conn.run("stopPublishTable('192.168.1.32',8676,'Trades','subTread1');" +
                "try{dropStreamTable('Trades')}catch(ex){};");
        Thread.sleep(5000);
        assertEquals(1,client.getAllReconnectSites().size());
        assertEquals(1,client.getAllReconnectTopic().size());
    }

}