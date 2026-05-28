package com.xxdb.restart;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DBConnectionTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));

    @Before
    public void setUp() throws IOException {
        conn = new DBConnection(false,false,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
    @Test
    public void Test_connect_EnableHighAvailability_false() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,CONTROLLER_PORT,"admin","123456",null,false);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(6000);
        //DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        String e = null;
        try{
            conn1.run("a=1;\n a");
        }catch(Exception ex)
        {
            e = ex.getMessage();
            System.out.println(ex);
        }
        assertNotNull(e);
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(3000);
        conn1.connect(HOST,PORT,"admin","123456",null,false);
        conn1.run("a=1;\n a");
        assertEquals(true, conn1.isConnected());
        conn1.close();
    }
    @Test
    public void Test_connect_EnableHighAvailability_true() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,CONTROLLER_PORT,"admin","123456",null,false);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,true);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(6000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(3000);
        assertEquals(true, conn1.isConnected());
    }
    //@Test //AJ-287
    public void Test_getConnection_highAvailability_false() throws SQLException, ClassNotFoundException, IOException {
        String script = "def restart(n)\n" +
                "{\n" +
                "try{\n" +
                "stopDataNode(\""+HOST+":"+PORT+"\");\n" +
                "}catch(ex)\n"+
                "{print(ex)}\n"+
                "sleep(n);\n"+
                "try{\n" +
                "stopDataNode(\""+HOST+":"+PORT+"\");\n" +
                "}catch(ex)\n"+
                "{print(ex)}\n"+
                "}\n" +
                "submitJob(\"restart\",\"restart\",restart,1000);";
        conn = new DBConnection(false, false, false);
        conn.connect(HOST, CONTROLLER_PORT, "admin", "123456",null,false,null,true);
        conn.run(script);
        DBConnection conn1 = new DBConnection(false, false, false);
        conn1.connect(HOST, PORT, "admin", "123456",null,false,null,true);
        conn1.close();
        conn.close();
    }

    @Test
    public void Test_connect_EnableHighAvailability_true_1() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,true);
        conn.connect(HOST, CONTROLLER_PORT, "admin", "123456");
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(6000);
        System.out.println("-----------------------------------");
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(3000);
        assertEquals(true, conn1.isConnected());
    }
    @Test
    public void Test_reConnect_true() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false,null,true);
        conn.connect(HOST, CONTROLLER_PORT, "admin", "123456");
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(6000);
        //conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("stopDataNode(\""+HOST+":"+PORT+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(3000);

        class MyThread extends Thread {
            @Override
            public void run() {
                try {
                    conn1.connect(HOST,PORT,"admin","123456",null,false,null,true);
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println(e.getMessage());
                }
            }
        }
        class MyThread1 extends Thread {
            @Override
            public void run() {
                try {
                    conn.run("startDataNode(\""+HOST+":"+PORT+"\")");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        MyThread thread = new MyThread();
        thread.start();
        Thread.sleep(3000);
        MyThread1 thread1 = new MyThread1();
        thread1.start();
        thread1.join();
        Thread.sleep(8000);
        conn1.run("a=1;\n a");
        assertEquals(true, conn1.isConnected());
    }
    @Test //reConnect is not valid
    public void Test_reConnect_false() throws IOException, InterruptedException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,PORT,"admin","123456",null,false,null,false);
        BasicString nodeAliasTmp = (BasicString)conn1.run("getNodeAlias()");
        String nodeAlias = nodeAliasTmp.getString();
        try{
            conn.run("stopDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(6000);
        conn1.run("a=1;\n a");
        //The connection switches to a different node to execute the code
        try{
            conn.run("startDataNode(\""+nodeAlias+"\")");
        }catch(Exception ex)
        {
            System.out.println(ex);
        }
        sleep(3000);
        assertEquals(true, conn1.isConnected());
    }

    //    @Test
//    public void test_node_disConnect() throws IOException, InterruptedException {
//        DBConnection contr = new DBConnection();
//        contr.connect("192.168.0.69",28920,"admin","123456");
//        List<DBTask> tasks = new ArrayList<>();
//        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,18921, "admin", "123456", 20, false, true);
//        class MyThread extends Thread {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        // 创建任务
//                        BasicDBTask task = new BasicDBTask("1..10");
//                        // 执行任务
//                        pool.execute(task);
//                        BasicIntVector data = null;
//                        if (task.isSuccessful()) {
//                            data = (BasicIntVector)task.getResult();
//                        } else {
//                            throw new Exception(task.getErrorMsg());
//                        }
//                        System.out.print(data.getString()+"\n");
//
//                        // 等待1秒
//                        Thread.sleep(1000);
//                    } catch (Exception e) {
//                        // 捕获异常并打印错误信息
//                        System.err.println("Error executing task: " + e.getMessage());
//                    }
//                }
//            }
//        }
//        class MyThread1 extends Thread {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        contr.run("try{stopDataNode('"+HOST+":18921')}catch(ex){}");
//                        contr.run("try{stopDataNode('"+HOST+":18922')}catch(ex){}");
//                        contr.run("try{stopDataNode('"+HOST+":18923')}catch(ex){}");
//                        Thread.sleep(1000);
//                        contr.run("try{stopDataNode('"+HOST+":18924')}catch(ex){}");
//                        Thread.sleep(5000);
//                        contr.run("try{startDataNode('"+HOST+":18921')}catch(ex){}");
//                        contr.run("try{startDataNode('"+HOST+":18922')}catch(ex){}");
//                        contr.run("try{startDataNode('"+HOST+":18923')}catch(ex){}");
//                        contr.run("try{startDataNode('"+HOST+":18924')}catch(ex){}");
//                        Thread.sleep(5000);
//                    } catch (Exception e) {
//                        // 捕获异常并打印错误信息
//                        System.err.println(e.getMessage());
//                    }
//                }
//            }
//        }
//
//        MyThread thread = new MyThread();
//        MyThread1 thread1 = new MyThread1();
//        thread.start();
//        Thread.sleep(5000);
//        thread1.start();
//        thread.join();
//    }

    @Ignore
    public void test_Disconnection_dfs() throws Exception {
        String HOST = "192.168.0.69";
        int PORT = 18921;
        String[] ha_site = new String[]{"192.168.0.69:18922", "192.168.0.69:18923", "192.168.0.69:18924"};
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456",
                "", true, ha_site);
        connection.run("\n" +
                "dbPath = \"dfs://tableUpsert_test1\"\n" +
                "if(existsDatabase(dbPath)){\n" +
                "\tdropDatabase(dbPath)\n" +
                "}\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "db  = database(dbPath, RANGE,1 50 10000)\n" +
                "pt = db.createPartitionedTable(t,`pt,`id).append!(t)");
        BasicTable bt = (BasicTable) connection.run("table( take(1000,3000000) as id, 1..3000000 as id2, 1..3000000 as value);");
        List<Entity> values = new ArrayList<>();
        values.add(bt);
        System.out.println("---------------------------------Read data end------------------------------------");
        Thread.sleep(5000);
        System.out.println("Start Write!!!!!!!!!!!!!!!!!");//这句话出现后进行断网操作
        for (int i = 0; i < 100; i++) {
            connection.run("tableInsert{loadTable(\"dfs://tableUpsert_test1\",\"pt\")}", values);
            System.out.println("数据插入" + i + "次");
        }
        System.out.println("End Write!!!!!!!!!!!!!!!!!!!");
        BasicTable res = (BasicTable) connection.run("select count(*) from loadTable(\"dfs://tableUpsert_test1\",\"pt\")");
        //assertEquals(300000100,res.rows());
        System.out.println("The result is: \n" + res.getString());
        connection.close();
    }
}
