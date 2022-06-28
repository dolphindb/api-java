package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

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
                throw new IOException("Failed to connect to 2xdb server");
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
        try {
            conn.run("login(`admin,`123456);" +
                    "dropStreamTable('Trades')"+
                    "dropStreamTable('Receive')"+
                    "deleteUser(`test1)");
           // client.close();
        } catch (Exception e) {

        }
    }

    @AfterClass
    public static void after() throws IOException {
        client.close();
        conn.close();
    }

    @Test
    public void test_ThreadedClient_HOST_subport() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        ThreadedClient client2 = new ThreadedClient(HOST,10022);
    }
    @Test
    public void test_ThreadedClient_null() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        ThreadedClient client2 = new ThreadedClient();
    }

    @Test
    public void test_ThreadedClient_only_subscribePort() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        ThreadedClient client3 = new ThreadedClient(10012);
    }

    @Test
    public void test_subscribe_ex1() throws IOException {
        MessageHandler handler = new MessageHandler() {
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
        try {
            client.subscribe(HOST, PORT, "Trades", "subtrades", handler);
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
        MessageHandler handler = new MessageHandler() {
            @Override
            public void doEvent(IMessage msg) {
                try {
                    String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                    conn.run(script);
                    // System.out.println(msg.getEntity(0).getString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        try {
            client.subscribe(HOST, PORT, "Trades", handler, true);
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
        MessageHandler handler = new MessageHandler() {
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
        try {
            conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            int ofst = 0;
            client.subscribe(HOST, PORT, "Trades", handler, ofst);
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
        MessageHandler handler = new MessageHandler() {
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
        int ofst = -2;
        client.subscribe(HOST, PORT, "Trades", handler, ofst);
        try {
            Thread.sleep(2000);
            client.unsubscribe(HOST, PORT, "Trades");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_subscribe_ofst_negative1() throws IOException {
        MessageHandler handler = new MessageHandler() {
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
        try {
            int ofst = -1;
            conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(2000);
            client.subscribe(HOST, PORT, "Trades", handler, ofst);
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
        MessageHandler handler = new MessageHandler() {
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
        try {
            int ofst = 10;
            conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
            Thread.sleep(2000);
            client.subscribe(HOST, PORT, "Trades", handler, ofst);
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
        MessageHandler handler = new MessageHandler() {
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
        conn.run("n=100;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        int ofst = 1000;
        client.subscribe(HOST, PORT, "Trades", handler, ofst);


    }

    @Test
    public void test_subscribe_filter() throws Exception {
        String script2 = "st3 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st3, tableName=`filter, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
        conn.run(script2);
        MessageHandler handler = new MessageHandler() {
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
        client.subscribe(HOST, PORT, "Trades", "subTrades1", handler, -1, filter1);
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
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", handler, -1, true, filter1, true, 10000, 5);
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
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..1000");
        client.subscribe(HOST, PORT, "Trades", "subTrades", handler, -1, true, filter1, true, 10000, 5);
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
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..1000");
        for (int i=0;i<10;i++){
        client.subscribe(HOST, PORT, "Trades", "subTrades1", handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades2", handler, -1, true, filter1, true, 10000, 5);
        client.subscribe(HOST, PORT, "Trades", "subTrades3", handler, -1, true, filter1, true, 10000, 5);
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
        MessageHandler handler = new MessageHandler() {
            @Override
            public void doEvent(IMessage msg) {
                try {
                    String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                    conn.run(script);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Vector filter1 = (Vector) conn.run("1..1000");
        try{
        client.subscribe(HOST,PORT,"Trades","subTread1",handler,-1,true,filter1,true,100,5,"admin_error","123456");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
    }

    @Test
    public void test_subscribe_password_error() throws IOException {
        MessageHandler handler = new MessageHandler() {
            @Override
            public void doEvent(IMessage msg) {
                try {
                    String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                    conn.run(script);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Vector filter1 = (Vector) conn.run("1..1000");
        try{
            client.subscribe(HOST,PORT,"Trades","subTread1",handler,-1,true,filter1,true,100,5,"admin","error_password");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
    }

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",handler,-1,true,filter1,true,100,5,"admin","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
    }
    @Test
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');");
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..100000");
        client.subscribe(HOST,PORT,"Trades","subTread1",handler,-1,true,filter1,true,100,5,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(10000,row_num.getInt());
    }

    @Test
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "deny(`test1,TABLE_READ,`Trades)");
        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", handler, -1, true, filter1, true, 100, 5, "test1", "123456");
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

        MessageHandler handler = new MessageHandler() {
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
        Vector filter1 = (Vector) conn.run("1..100000");
        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", handler, -1, true, filter1, true, 100, 5, "test1", "123456");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]' script: 'publishTable'",e.getMessage());
        }

        client.subscribe(HOST, PORT, "Trades", "subTread1", handler, -1, true, filter1, true, 100, 5, "test2", "123456");

        try {
            client.subscribe(HOST, PORT, "Trades", "subTread1", handler, -1, true, filter1, true, 100, 5, "test3", "123456");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'No access to shared table [Trades]' script: 'publishTable'",e.getMessage());
        }
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "try{deleteUser(`test2)}catch(ex){};"+
                "try{deleteUser(`test3)}catch(ex){};");

    }

    @Test
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        conn.run("try{deleteUser(`test1)}catch(ex){};"+
                "createUser(`test1, '123456');" +
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        MessageHandler handler = new MessageHandler() {
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
        client.subscribe(HOST,PORT,"tmp_st1","subTread1",handler,-1,true,null,true,100,5,"test1","123456");
        client.subscribe(HOST,PORT,"tmp_st2","subTread1",handler,-1,true,null,true,100,5,"test1","123456");
        try {
            client.subscribe(HOST, PORT, "tmp_st3", "subTread1", handler, -1, true, null, true, 100, 5, "test1", "123456_error");
        }catch (Exception e){
            assertEquals(HOST+":"+PORT+" Server response: 'The user name or password is incorrect.' script: 'login'",e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        Thread.sleep(5000);
        BasicInt row_num = (BasicInt)conn.run("(exec count(*) from Receive)[0]");
        assertEquals(20000,row_num.getInt());
    }


}