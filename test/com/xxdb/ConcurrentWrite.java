package com.xxdb;

import com.xxdb.data.Entity;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class ConcurrentWrite implements Runnable{
    private static DBConnection conn;
    public static String HOST ;
    public static Integer PORT ;

    public static void setUp() throws IOException {
        conn = new DBConnection();
        try {
            Properties props = new Properties();
            FileInputStream in= new FileInputStream( "test/com/xxdb/setup/settings.properties");
            props.load(in);
            PORT =Integer.parseInt(props.getProperty ("PORT"));
            HOST  =props.getProperty ("HOST");
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("n=1000000;");
        sb.append("date=rand(2018.08.01..2018.08.03,n);");
        sb.append("sym=rand(`AAPL`MS`C`YHOO,n);");
        sb.append("qty=rand(1..1000,n);");
        sb.append("price=rand(100.0,n);");
        sb.append("t=table(date,sym,qty,price);");
        sb.append("if(existsDatabase(\"dfs://db1\")){\n" +
                "\tdropDatabase(\"dfs://db1\")\n" +
                "};");
        sb.append("db=database(\"dfs://db1\",VALUE,2018.08.01..2018.08.03);");
        sb.append("trades=db.createPartitionedTable(t,`trades,`date).append!(t);");
        conn.run(sb.toString());
    }
    @Override
    public void run() {
        DBConnection conn1= new DBConnection();
        StringBuilder sb = new StringBuilder();
        sb.append("t=table(rand(2018.08.01,10) as date,rand(`AAPL`MS`C`YHOO,10) as sym,rand(1..1000,10) as qty,rand(100.0,10) as price);");
        sb.append("tb= loadTable(\"dfs://db1\", `trades);");
        sb.append("tb.append!(t);");
        int i =  new Random().nextInt(1000);
        sb.append(i);
        try {
            if (!conn1.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            conn1.run(sb.toString());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        conn1.close();

    }

    public static void main(String[] args) throws InterruptedException, IOException {
        setUp();
        for (int i=0;i<100;i++){
            new Thread(new ConcurrentWrite()).start();
        }
        Thread.sleep(1000);
    }
}
