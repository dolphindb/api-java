package com.xxdb.streaming.client;

import com.xxdb.DBConnection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;

public class MessageParserTest {
    private MessageDispatcher dispatcher;
    private int listeningPort = 0;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test
    public void test_MessageParser_run() throws IOException {
        MessageParser.DBConnectionAndSocket dBConnectionAndSocket = new MessageParser.DBConnectionAndSocket();
        AbstractClient abstractClient = new AbstractClient() {
            @Override
            protected boolean doReconnect(Site site) {
                return false;
            }
        };
        Socket socket = null;
        dBConnectionAndSocket.socket = socket;
        MessageParser listener = new MessageParser(dBConnectionAndSocket, abstractClient, listeningPort);
        String re = null;
        try{
            listener.run();
        }catch(Exception ex){
            re = ex.toString();
        }
        Assert.assertEquals("java.lang.NullPointerException",re);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        dBConnectionAndSocket.socket = conn.getSocket();
        //dBConnectionAndSocket.conn = conn;
        MessageParser listener1 = new MessageParser(dBConnectionAndSocket, abstractClient, listeningPort);
       // listener1.run();
    }

}
