package com.xxdb;

import com.xxdb.data.BasicLong;
import com.xxdb.data.Entity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;
public class ConcurrentTest {
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    @After
    public  void after() throws IOException, InterruptedException {
        conn.close();
    }

    class ConcurrentRead extends Thread {
        @Override
        public void run() {
            DBConnection conn1= new DBConnection();
            StringBuilder sb = new StringBuilder();
            sb.append("exec count(*) from t where qty=");
            int i =  new Random().nextInt(1000);
            sb.append(i);
            try {
                Thread.sleep(1000);
                if (!conn1.connect(HOST, PORT, "admin", "123456")) {
                    throw new IOException("Failed to connect to 2xdb server");
                }
            } catch (IOException | InterruptedException ex) {
                ex.printStackTrace();
            }
            try {
                conn1.run("t= loadTable(\"dfs://db1\", `trades)");
                Entity res = conn1.run(sb.toString());
                System.out.println(res);
            } catch (IOException e) {
                e.printStackTrace();
            }
            conn1.close();
        }
    }
    @Test
    public void test_ConcurrentRead() throws IOException, InterruptedException {
        String script = "n=1000000;" +
                "date=rand(2018.08.01..2018.08.03,n);" +
                "sym=rand(`AAPL`MS`C`YHOO,n);" +
                "qty=rand(1..1000,n);" +
                "price=rand(100.0,n);" +
                "t=table(date,sym,qty,price);" +
                "if(existsDatabase(\"dfs://db1\")){\n" +
                "\tdropDatabase(\"dfs://db1\")\n" +
                "};" +
                "db=database(\"dfs://db1\",VALUE,2018.08.01..2018.08.03);" +
                "trades=db.createPartitionedTable(t,`trades,`date).append!(t);";
        conn.run(script);
        for (int i=0;i<100;i++){
            new Thread(new ConcurrentRead()).start();
        }
        Thread.sleep(1000);
    }

    class ConcurrentWrite extends Thread {
        private final int id;

        public ConcurrentWrite(int i) {
            this.id=i;
        }
        @Override
        public void run() {
            DBConnection conn1= new DBConnection();
            StringBuilder sb = new StringBuilder();
            sb.append("t=table(take("+this.id+",100) as id, take(`w,100) as val );");
            sb.append("tb= loadTable(\"dfs://writeJava\", `pt);");
            sb.append("tb.append!(t);exec count(*) from tb");
            try {
                if (!conn1.connect(HOST, PORT, "admin", "123456")) {
                    throw new IOException("Failed to connect to 2xdb server");
                }
                BasicLong bit = (BasicLong) conn1.run(sb.toString());
                System.out.println(id+"="+bit.getLong());
            } catch (IOException ex) {
                ex.printStackTrace();
            }finally {
                conn1.close();
            }
        }
    }
    @Test
    public void test_ConcurrentWrite() throws IOException, InterruptedException {
        String script = "t=table(1 as id,`q as val);" +
                "if(existsDatabase(\"dfs://writeJava\")){\n" +
                "\tdropDatabase(\"dfs://writeJava\")\n" +
                "};" +
                "db=database(\"dfs://writeJava\",VALUE,1..1000);" +
                "pt=db.createPartitionedTable(t,`pt,`id).append!(t);";
        conn.run(script);
        for (int i=0;i<100;i++){
            new Thread(new ConcurrentWrite(i)).start();
        }
        Thread.sleep(5000);
        conn.run( "\tdropDatabase(\"dfs://writeJava\")\n" );
    }
}
