package com.xxdb;

import com.xxdb.data.BasicLong;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.ResourceBundle;

public class ConcurrentWriteTest implements Runnable{
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    private final int id;

    public ConcurrentWriteTest(int i) {
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

    public static void main(String[] args) throws InterruptedException, IOException {
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
        sb.append("t=table(1 as id,`q as val);");
        sb.append("if(existsDatabase(\"dfs://writeJava\")){\n" +
                "\tdropDatabase(\"dfs://writeJava\")\n" +
                "};");
        sb.append("db=database(\"dfs://writeJava\",VALUE,1..1000);");
        sb.append("pt=db.createPartitionedTable(t,`pt,`id).append!(t);");
        conn.run(sb.toString());
        for (int i=0;i<100;i++){
            new Thread(new ConcurrentWriteTest(i)).start();
        }
        Thread.sleep(5000);
        conn.run( "\tdropDatabase(\"dfs://writeJava\")\n" );
        conn.close();
    }
}
