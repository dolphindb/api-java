package com.xxdb.streaming.reverse;

import com.xxdb.DBConnection;
import com.xxdb.streaming.client.PollingClient;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketException;
import java.util.ResourceBundle;

import static org.junit.Assert.*;


public class AbstractClientTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT=9002;
    @Test
    public void test_AbstractClient_Basic() throws SocketException {
        PollingClient client = new PollingClient(0);
        client.setNeedReconnect("dolphindb/",2);
        long time1 = client.getReconnectTimestamp("dolphindb");
        assertEquals(0,client.getNeedReconnect("OceanBase"));
        assertEquals(0,client.getReconnectTimestamp("kingBase"));
        System.out.println(client.getNeedReconnect("dolphindb"));
        client.setReconnectTimestamp("dolphindb",8);
        assertNotEquals(client.getReconnectTimestamp("dolphindb"),time1);
        assertNull(client.getSiteByName("MongoDB"));
        client.setNeedReconnect("dolphindb/",4);
    }

    @Test
    public void test_AbstractClient() throws IOException {
        PollingClient client = new PollingClient(0);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("try{dropStreamTable('Trades_AbstractClient')}catch(ex){};st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades_AbstractClient, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
        client.subscribe(HOST,PORT,"Trades_AbstractClient","subTrades");
        assertTrue(client.isRemoteLittleEndian(HOST));
        client.unsubscribe(HOST,PORT,"Trades_AbstractClient","subTrades");
        assertFalse(client.isRemoteLittleEndian("192.168.11.5"));
        conn.run("dropStreamTable(`Trades_AbstractClient);");
//        conn.run("x=1..100000000;y=compress(x,\"delta\");");
//        Entity res = conn.run("y;");
//        System.out.println(res.getString());

    }

}
