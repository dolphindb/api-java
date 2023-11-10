package com.dolphindb.examples;

import com.xxdb.DBConnection;
import com.xxdb.streaming.client.ThreadPooledClient;
import com.xxdb.streaming.client.ThreadedClient;

import java.io.IOException;

/**
 * when single thread client can not effectively process streaming data, you can use it.
 * ThreadPooledClient has a fixed thread pool.
 *
 */
public class ThreadPooledClientDemo {

    private static DBConnection conn = new DBConnection();
    public static String host = "localhost";
    public static Integer port = 8848;
    public static Integer subscribePort = 8892;

    public static void main(String[] args) throws IOException, InterruptedException {
        createStreamTable();
        ThreadPooledClient client = new ThreadPooledClient(subscribePort,10);
        client.subscribe(host, port, "Trades", new MyHandler(), 0);
    }

    public static void createStreamTable() throws IOException {
        conn.connect(host, port);
        conn.login("admin", "123456", false);
        try{
            conn.run("dropStreamTable(\"Trades\")");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("share streamTable(30000:0,`id`time`sym`qty`price,[INT,TIMESTAMP,SYMBOL,INT,DOUBLE]) as Trades\n");
        conn.run("data=table(1..1000 as id, now()+1..1000 as time, take(`aaa,1000) as sym," +
                "rand(100..1000, 1000) as qty,rand(100.0, 1000) as price)");
        conn.run("Trades.tableInsert(data)");
    }
}


