package com.xxdb.restart;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.StreamDeserializer;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static com.xxdb.Prepare.*;
import static org.junit.Assert.assertEquals;

public class ThreadedClientTest {

    public static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();
    static String controller_host = bundle.getString("CONTROLLER_HOST");
    static int controller_port = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    //static int PORT = 9002;
    private static ThreadedClient threadedClient;
    static long total = 0;

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
            threadedClient = new ThreadedClient(HOST, 0);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{threadedClient.unsubscribe(HOST, PORT, "Trades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades2");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        try {clear_env();}catch (Exception e){}
    }

    @After
    public void after() throws IOException, InterruptedException {
        try{threadedClient.unsubscribe(HOST, PORT, "Trades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades2");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "Trades", "subTrades");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");}catch (Exception ex){}
        try{threadedClient.unsubscribe(HOST, PORT, "outTables", "javaStreamingApi");}catch (Exception ex){}
        try {clear_env();}catch (Exception e){}
        //Thread.sleep(2000);
        threadedClient.close();
        conn.close();
        //Thread.sleep(2000);
    }

    @AfterClass
    public static void clear_conn() {
        try {clear_env_1();}catch (Exception e){}
    }

    public static MessageHandler MessageHandler_handler = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                conn.run(script);
                System.out.println(msg.getEntity(0).getString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };


    @Test(timeout = 180000)
    public void test_ThreadClient_subscribe_backupSites_server_disconnect() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "try{undef(`Trades,SHARED)}catch(ex){};\n" +
                "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        Vector filter1 = (Vector) conn.run("1..50000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler, -1,true,filter1, (StreamDeserializer) null,true,1000, 1,"admin","123456",backupSites,10,true);
        System.out.println("Successful subscribe");
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn1.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(8000);
        conn.run("t=table(5001..5500 as tag,now()+5001..5500 as ts,rand(100.0,500) as data);" + "Trades.append!(t)");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(2000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("5500",row_num.getColumn(0).get(0).getString());
        threadedClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_ThreadClient_subscribe_backupSites_server_disconnect_backupSites_disconnect_subOnce_false() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "try{undef(`Trades,SHARED)}catch(ex){};\n" +
                "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler, 0,true,filter1, (StreamDeserializer) null,true,1000, 1,"admin","123456",backupSites,10,false);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(8000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(10000);

        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("3000",row_num.getColumn(0).get(0).getString());
        threadedClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_ThreadClient_subscribe_backupSites_server_disconnect_backupSites_disconnect_subOnce_true() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "try{undef(`Trades,SHARED)}catch(ex){};\n" +
                "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler, 0,true,filter1, (StreamDeserializer) null,true,1000, 1,"admin","123456",backupSites,10,true);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(8000);

        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        assertEquals("2000",row_num.getColumn(0).get(0).getString());
        DBConnection conn4 = new DBConnection();
        conn4.connect(HOST,port_list[2],"admin","123456");
        conn4.run(script1);
        conn4.run(script2);
        //client.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    public static MessageHandler MessageHandler_handler1 = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String script = String.format("insert into Receive values(%d,%s,%f,%d)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()),System.currentTimeMillis());
                conn.run(script);
                System.out.println(msg.getEntity(0).getString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };
    @Test(timeout = 180000)
    public void test_ThreadClient_subscribe_backupSites_resubscribeInterval() throws Exception {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "try{undef(`Trades,SHARED)}catch(ex){};\n" +
                "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data`now,[INT,TIMESTAMP,DOUBLE,TIMESTAMP])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[2],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        Vector filter1 = (Vector) conn.run("1..10000000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler1, -1,true,filter1, (StreamDeserializer) null,true,1, 1,"admin","123456",backupSites,20000,true);
        System.out.println("Successful subscribe");
        class MyThread extends Thread {
            @Override
            public void run() {
                try {
                    conn3.run("for(n in 1..1000){\n" +
                            "    insert into Trades values(n,now()+n,n);\n" +
                            "    sleep(100);\n" +
                            "}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println( e.getMessage());
                }
            }
        }
        class MyThread1 extends Thread {
            @Override
            public void run() {
                try {
                    conn1.run("for(n in 1..1000){\n" +
                            "    insert into Trades values(n,now()+n,n);\n" +
                            "    sleep(100);\n" +
                            "}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println( e.getMessage());
                }
            }
        }
        class MyThread2 extends Thread {
            @Override
            public void run() {
                try {
                    controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println(e.getMessage());
                }
            }
        }
        MyThread thread = new MyThread();
        MyThread1 thread1 = new MyThread1();
        MyThread2 thread2 = new MyThread2();
        thread.start();
        thread1.start();
        Thread.sleep(2000);
        thread2.start();
        thread.join();
        Thread.sleep(5000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        BasicTable re = (BasicTable)conn.run("select tag ,now,deltas(now) from Receive  order by  deltas(now) desc \n");
        System.out.println(re.getString());
        Assert.assertEquals(true,Integer.valueOf(re.getColumn(2).get(0).toString())>20000);
        Assert.assertEquals(1000,re.rows());
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[1],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        threadedClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void Test_ThreadClient_subscribe_resubscribeInterval_subOnce_not_set() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "try{undef(`Trades,SHARED)}catch(ex){};\n" +
                "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler, 0,true,filter1, (StreamDeserializer) null,true,1000, 1,"admin","123456",backupSites);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(5000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(5000);
        wait_data("Receive",3000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        assertEquals("3000",row_num.getColumn(0).get(0).getString());
        threadedClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    //@Test(timeout = 180000)//该case为了验证重连次数和遍历测试（人工检查日志）
    public void test_ThreadClient_subscribe_backupSites_server_disconnect_1() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        Vector filter1 = (Vector) conn.run("1..50000");
        List<String> backupSites = Arrays.asList(new String[]{"192.168.0.69:18921", "192.168.0.69:18922", "192.168.0.69:18923"});
        threadedClient.subscribe(HOST,port_list[1],"Trades","subTread1",MessageHandler_handler, -1,true,filter1, (StreamDeserializer) null,true,1000, 1,"admin","123456",backupSites);
        System.out.println("这里可以手工断掉这个集群下所有可用节点http://192.168.0.69:18920/?view=overview-old");
        Thread.sleep(1000000);
    }

}
