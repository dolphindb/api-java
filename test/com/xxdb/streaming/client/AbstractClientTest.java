package com.xxdb.streaming.client;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.streaming.client.MessageDispatcher;
import com.xxdb.streaming.client.PollingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import static org.junit.Assert.*;


public class AbstractClientTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    public static PollingClient client;

    private StreamDeserializer deserializer_;

    public static DBConnection conn ;
    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
                "}");
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
    }

    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            client = new PollingClient(HOST,0);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {client.unsubscribe(HOST, PORT, "Trades1", "subtrades");}catch (Exception e){}
        try {client.unsubscribe(HOST, PORT, "Trades1", "subtrades1");}catch (Exception e){}
        try {client.unsubscribe(HOST, PORT, "Trades1", "subtrades2");}catch (Exception e){}
        try {client.unsubscribe(HOST, PORT, "Trades1");}catch (Exception e){}
        try {client.unsubscribe(HOST, PORT, "Trades", "subTread1");}catch (Exception e){}
        clear_env();
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades1, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
    }

    @After
    public  void after() throws IOException, InterruptedException {
        try {
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades");
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades1");
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades2");
            client.unsubscribe(HOST, PORT, "Trades1");
        }catch (Exception e){
        }
        try {
            conn.run("login(`admin,`123456);" +
                    "try{dropStreamTable('Trades1')}catch(ex){};"+
                    "try{dropStreamTable('Receive')}catch(ex){};"+
                    "try{deleteUser(`test1)}catch(ex){};" +
                    "userlist=getUserList();" +
                    "grouplist=getGroupList();" +
                    "loop(deleteUser,userlist);" +
                    "loop(deleteGroup,grouplist)");
        } catch (Exception e) {
        }
        try{conn.run("dropStreamTable(`Trades1)");}catch (Exception e){}
        // clear_env();
        //client.close();
        conn.close();
        Thread.sleep(2000);
    }
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
        client.setNeedReconnect("",4);
    }
    @Test(expected = RuntimeException.class)
    public void test_AbstractClient_error() throws SocketException {
        PollingClient client = new PollingClient(0);
        client.setNeedReconnect("dolphindb/",2);
        client.getReconnectTimestamp(null);
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
    @Test
    public void test_AbstractClient_putTopic() throws IOException {
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
    }
    @Test
    public void test_AbstractClient_dispatch() throws Exception {
        PollingClient client = new PollingClient(0);
        HashMap<String,Integer> map = new HashMap<>();
        map.put("dolphindb",1);
        map.put("mongodb",2);
        map.put("gaussdb",3);
        map.put("goldendb",4);
        BasicAnyVector bav = new BasicAnyVector(4);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity res = conn.run("blob(\"hello\")");
        bav.set(0,new BasicInt(5));
        bav.set(1,new BasicString("DataBase"));
        bav.set(2, (Scalar) res);
        bav.set(3,new BasicDouble(15.48));
        IMessage message =new BasicMessage(0L,"first",bav,map);
        client.dispatch(message);
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100);
        client.dispatch(message);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");

    }
}
