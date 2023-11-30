package com.dolphindb.examples;

import com.xxdb.data.*;
import com.xxdb.DBConnection;
import com.xxdb.streaming.client.*;
import java.io.IOException;
import java.util.*;

public class PollingClientDemo {
    private static DBConnection conn = new DBConnection();
    public static String host = "localhost";
    public static Integer port = 8848;
    public static ThreadedClient client;
    public static Integer subscribePort = 8892;

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

    public static void PollingClient() throws IOException {
        com.xxdb.streaming.client.PollingClient client = new com.xxdb.streaming.client.PollingClient(subscribePort);
        try {
            TopicPoller poller1 = client.subscribe(host, port, "Trades","PollingClient",0);
            while (true) {
                ArrayList<IMessage> msgs = poller1.poll(1000);
                if (msgs.size() > 0) {
                    for(IMessage m : msgs){
                        int id = ((BasicInt)m.getEntity(0)).getInt();
                        String time = m.getEntity(1).getString();
                        String sym =  m.getEntity(2).getString();
                        int qty = ((BasicInt)m.getEntity(3)).getInt();
                        double price = ((BasicDouble)m.getEntity(4)).getDouble();
                        String sf = String.format("id: %d, time: %s, sym: %s, " +
                                "qty: %d, price: %f", id, time, sym, qty, price);
                        System.out.println(sf);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        PollingClientDemo.createStreamTable();
        PollingClientDemo.PollingClient();
    }
}
