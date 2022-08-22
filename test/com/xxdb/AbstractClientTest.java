package com.xxdb;

import com.xxdb.streaming.client.PollingClient;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.ResourceBundle;

public class AbstractClientTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    //static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int PORT=9002;
    @Test
    public void test_AbstractClient_Basic() throws SocketException {
        PollingClient client = new PollingClient(9006);
        client.setNeedReconnect("dolphindb/",2);
        long time1 = client.getReconnectTimestamp("dolphindb");
        assertEquals(0,client.getNeedReconnect("OceanBase"));
        assertEquals(0,client.getReconnectTimestamp("kingBase"));
        System.out.println(client.getNeedReconnect("dolphindb"));
        client.setReconnectTimestamp("dolphindb",8);
        assertNotEquals(client.getReconnectTimestamp("dolphindb"),time1);
        assertNull(client.getSiteByName("MongoDB"));
        client.setNeedReconnect("dolphindb/",2);
    }

    @Test
    public void test_AbstractClient() throws IOException {
        PollingClient client = new PollingClient(9006);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
        client.subscribe(HOST,PORT,"Trades","subTrades");
        assertTrue(client.isRemoteLittleEndian(HOST));
        client.unsubscribe(HOST,PORT,"Trades","subTrades");
        assertFalse(client.isRemoteLittleEndian("192.168.11.5"));
        conn.run("dropStreamTable(`Trades);");
    }

    @Test
    public void test_AbstractClient_TryConnect() throws IOException {
        class MyClient extends PollingClient{

            public MyClient(String subscribeHost, int subscribePort) throws SocketException {
                super(subscribeHost, subscribePort);
            }

            @Override
            protected void unsubscribeInternal(String host, int port, String tableName) throws IOException {
                super.unsubscribeInternal(host, port, tableName);
            }
        }
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades, asynWrite=true, " +
                "compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
        MyClient mc = new MyClient(HOST,9009);
        mc.subscribe(HOST,PORT,"Trades");
        mc.unsubscribeInternal(HOST,PORT,"Trades");
        conn.run("dropStreamTable(`Trades);");
    }
}
