package com.xxdb.streaming.sample;

import com.xxdb.DBConnection;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadPooledClient;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.SocketException;

public class ThreadPooledClientTester {
    public static DBConnection conn;
    public static String HOST = "192.168.0.21";
    public static Integer PORT = 9002;
    private static ThreadPooledClient client;
    public static void main(String args[]) {
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
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1, tableName=`Trades, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n"
                    + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
            conn.run(script1);
            String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st2, tableName=`Receive, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
            conn.run(script2);
            client = new ThreadPooledClient(9050,10);
            client.subscribe("192.168.0.21", 9002, "Trades", handler);

        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }
}
