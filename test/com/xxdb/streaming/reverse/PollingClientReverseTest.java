/* ==============server publish data sample========== 
   n=20000000
   t=table(n:0,`time`sym`qty`price`exch`index,[TIMESTAMP,SYMBOL,INT,DOUBLE,SYMBOL,LONG])
   share t as trades
   setStream(trades,true)
   t=NULL
   rows = 1
   timev = take(now(), rows)
   symv = take(`MKFT, rows)
   qtyv = take(112, rows)
   pricev = take(53.75, rows)
   exchv = take(`N, rows)
   for(x in 0:2000000){
   insert into trades values(timev, symv, qtyv, pricev, exchv,x)
   }
   insert into trades values(timev, symv, take(-1, 1), pricev, exchv,x)
 */
package com.xxdb.streaming.reverse;


import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import org.javatuples.Pair;
import org.junit.*;
import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.xxdb.Prepare.*;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.PrepareStreamTableDecimal_StreamDeserializer;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.PrepareStreamTable_StreamDeserializer;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.*;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.port_list;
import static org.junit.Assert.*;

public class PollingClientReverseTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT=9002;
    public static PollingClient pollingClient;
    private StreamDeserializer deserializer_;

    @BeforeClass
    public static void setUp() throws IOException {
        try {clear_env_1();}catch (Exception e){}
    }
    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            pollingClient = new PollingClient(HOST,0);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades1");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades2");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades", "subTread1");}catch (Exception e){}
        try {clear_env();}catch (Exception e){}
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades1, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
    }

    @After
    public  void after() throws IOException, InterruptedException {
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades1");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades2");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades1");}catch (Exception e){}
        try {pollingClient.unsubscribe(HOST, PORT, "Trades", "subTread1");}catch (Exception e){}
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
        try {clear_env();}catch (Exception e){}
        //client.close();
        conn.close();
    }

    @AfterClass
    public static void clear_conn() {
        try {clear_env_1();}catch (Exception e){}
    }

    public static void checkResult() throws IOException, InterruptedException {
        for (int i = 0; i < 20; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            if (tmpNum.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(100);
        }
        BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
        BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
        assertEquals(except.rows(), res.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
        }
    }
    public static void checkResult1() throws IOException, InterruptedException {
        for (int i = 0; i < 20; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1 ");
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2 ");
            if (tmpNum.getInt()==(1000)&& tmpNum1.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(100);
        }
        BasicTable except = (BasicTable)conn.run("select * from  pub_t1 order by timestampv");
        BasicTable res = (BasicTable)conn.run("select * from  sub1 order by timestampv");
        BasicTable except1 = (BasicTable)conn.run("select * from  pub_t2 order by timestampv");
        BasicTable res1 = (BasicTable)conn.run("select * from  sub2 order by timestampv");
        assertEquals(except.rows(), res.rows());
        assertEquals(except1.rows(), res1.rows());
        for (int i = 0; i < except.columns(); i++) {
            System.out.println("col" + res.getColumnName(i));
            assertEquals(except.getColumn(i).getString(), res.getColumn(i).getString());
            assertEquals(except1.getColumn(i).getString(), res1.getColumn(i).getString());
        }
    }
    @Test(expected = IOException.class)
    public  void error_size1() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        msgs = poller1.poll(1000,0);
       }
    @Test(expected = IOException.class)
    public  void error_size2() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs = poller1.poll(1000,-10);
    }

    @Test(timeout = 120000)
    public  void test_size() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        try {
            for (int i=0;i<10;i++){//data<size
                conn.run("n=50;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    assertEquals(50, msgs.size());
                }
            }
            for (int i=0;i<10;i++){//data>size
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1000);
                }
            }
            for (int i=0;i<10;i++){//data=size
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(5000,5000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1000);
                }
            }
            for (int i=0;i<10;i++){//bigsize
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,1000000000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1000);
                }
            }
            for (int i=0;i<2;i++){//bigdata
                conn.run("n=10000000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,10000);
               /* if (msgs==null){
                    continue;
                }*/
                 if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=10000);
                }
            }
            for (int i=0;i<10;i++){//smalldata
                conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,10000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1);
                }
            }
            for (int i=0;i<10;i++){//append Many times
                conn.run("n=10;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                conn.run("n=20;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                conn.run("n=30;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                conn.run("n=40;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(1000,10000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=100);
                }
            }
            for (int i=0;i<10;i++){//no data
                msgs = poller1.poll(100,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                   // System.out.println(msgs.size());
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        pollingClient.unsubscribe(HOST, PORT, "Trades1","subtrades");
    }

    @Test(timeout = 120000)
    public  void test_size_resubscribe() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1", "subtrades1", -1, true);
            TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1", "subtrades2", -1, true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs1 = poller1.poll(1000, 10000);
                msgs2 = poller2.poll(1000, 1000);
                if (msgs1 == null) {
                    continue;
                } else if (msgs1.size() > 0) {
                    BasicTable t = (BasicTable) conn.run("t");
                    assertEquals(5000, msgs1.size());
                  for (int k=0;k<msgs1.size();k++){
                      assertEquals(t.getColumn(0).get(k),msgs1.get(k).getEntity(0));
                      assertEquals(t.getColumn(1).get(k),msgs1.get(k).getEntity(1));
                      assertEquals(t.getColumn(2).get(k),msgs1.get(k).getEntity(2));
                  }

                }
                if (msgs2 == null) {
                    continue;
                } else if (msgs2.size() > 0) {
                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
                    assertTrue(msgs2.size() >= 1000);
                }

            }
            pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades1");
            pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades2");
        }
    }
    @Test(timeout = 120000)
    public void test_subscribe_admin_login() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1", "subtrades1", -1, true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msgs1 = poller1.poll(1000, 10000);
        assertEquals(5000, msgs1.size());
        pollingClient.unsubscribe(HOST, PORT, "Trades1", "subtrades1");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_user_error() throws IOException {
            TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin_error","123456");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_password_error() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin","error_password");

    }

    @Test(timeout = 120000)
    public void test_subscribe_admin() throws IOException, InterruptedException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",-1,true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msgs1 = poller1.poll(100, 10000);
        assertEquals(5000, msgs1.size());
        pollingClient.unsubscribe(HOST, PORT, "Trades1", "subTread1");
    }
    @Test(timeout = 120000)
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        PrepareUser("test1","123456");
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",-1,true,null,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        //Thread.sleep(5000);
        ArrayList<IMessage> msgs1 = poller1.poll(500, 10000);
        assertEquals(10000, msgs1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
        PrepareUser("test1","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades1\"});");
        try {
            TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1", "subTread1", -1, true, null, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void test_subscribe_other_some_user() throws IOException, InterruptedException {
        PrepareUser("test1","123456");
        PrepareUser("test2","123456");
        PrepareUser("test3","123456");
        conn.run("colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades1\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades1\"});");
        try {
            TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1", "subTread1",  -1, true, null, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(111+e.getMessage());
        }
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1", "subTread1",-1, true, null, "test2", "123456");

        try {
            TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1", "subTread1", -1, true,null, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        ArrayList<IMessage> msgs1 = poller1.poll(10000, 10000);
        assertEquals(10000, msgs1.size());
        pollingClient.unsubscribe(HOST, PORT, "Trades1", "subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        PrepareUser("test1","123456");
        conn.run("share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"tmp_st1","subTread1",-1,true,null,"test1","123456");
        TopicPoller poller2 = pollingClient.subscribe(HOST,PORT,"tmp_st2","subTread1",-1,true,null,"test1","123456");
        try {
            TopicPoller poller3 = pollingClient.subscribe(HOST, PORT, "tmp_st3", "subTread1",-1, true, null, "test1", "123456_error");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        ArrayList<IMessage> msgs1 = poller1.poll(1000, 10000);
        assertEquals(10000, msgs1.size());
        ArrayList<IMessage> msgs2 = poller2.poll(1000, 10000);
        assertEquals(10000, msgs1.size());
    }

    @Test(timeout = 60000)
    public void test_subscribe_offset() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",0);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5000, msg1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_defaultActionName_offset() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1",-1);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        assertEquals(5000, msg1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_TableName() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5000, msg1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_tableName_ActionName() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        assertEquals(5000, msg1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_tableName_ActionName_offset_filter() throws IOException {
        conn.run("setStreamTableFilterColumn(Trades1,`tag);" +
                "filter=1 2 3 4 5");
        BasicIntVector filter = (BasicIntVector) conn.run("filter");
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",-1,filter);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5, msg1.size());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_tableName_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1",true);
            //PollingClient pollingClient1 = new PollingClient(HOST,9069);
            //TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1",true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs1 = poller1.poll(1000, 10000);
         //       msgs2 = poller2.poll(100, 1000);
                if (msgs1 == null) {
                    continue;
                } else if (msgs1.size() > 0) {
                    BasicTable t = (BasicTable) conn.run("t");
                    assertEquals(5000, msgs1.size());
                    for (int k=0;k<msgs1.size();k++){
                        assertEquals(t.getColumn(0).get(k),msgs1.get(k).getEntity(0));
                        assertEquals(t.getColumn(1).get(k),msgs1.get(k).getEntity(1));
                        assertEquals(t.getColumn(2).get(k),msgs1.get(k).getEntity(2));
                    }

                }
//                if (msgs2 == null) {
//                    continue;
//                } else if (msgs2.size() > 0) {
//                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
//                    assertTrue(msgs2.size() >= 1000);
//                }

            }
            pollingClient.unsubscribe(HOST, PORT, "Trades1");
          //  pollingClient1.unsubscribe(HOST, PORT, "Trades1");
        }

    }

    @Test(timeout = 200000)
    public void test_subscribe_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1",-1,true);
            PollingClient pollingClient1 = new PollingClient(HOST,0);
            //TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1",-1,true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs1 = poller1.poll(1000, 10000);
               // msgs2 = poller2.poll(100, 1000);
                if (msgs1 == null) {
                    continue;
                } else if (msgs1.size() > 0) {
                    BasicTable t = (BasicTable) conn.run("t");
                    assertEquals(5000, msgs1.size());
                    for (int k=0;k<msgs1.size();k++){
                        assertEquals(t.getColumn(0).get(k),msgs1.get(k).getEntity(0));
                        assertEquals(t.getColumn(1).get(k),msgs1.get(k).getEntity(1));
                        assertEquals(t.getColumn(2).get(k),msgs1.get(k).getEntity(2));
                    }

                }
//                if (msgs2 == null) {
//                    continue;
//                } else if (msgs2.size() > 0) {
//                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
//                    assertTrue(msgs2.size() >= 1000);
//                }

            }
            pollingClient1.unsubscribe(HOST, PORT, "Trades1");
//            pollingClient.unsubscribe(HOST, PORT, "Trades1");
        }
    }

    @Test(timeout = 200000)
    public void test_subscribe_tableName_actionName_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1","subTrades1",-1,true);
            TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1","subTrades2",-1,true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs1 = poller1.poll(500, 10000);
                msgs2 = poller2.poll(500, 10000);
                if (msgs1 == null) {
                    continue;
                } else if (msgs1.size() > 0) {
                    BasicTable t = (BasicTable) conn.run("t");
                    assertEquals(1000, msgs1.size());
                    for (int k=0;k<msgs1.size();k++){
                        assertEquals(t.getColumn(0).get(k),msgs1.get(k).getEntity(0));
                        assertEquals(t.getColumn(1).get(k),msgs1.get(k).getEntity(1));
                        assertEquals(t.getColumn(2).get(k),msgs1.get(k).getEntity(2));
                    }

                }
                if (msgs2 == null) {
                    continue;
                } else if (msgs2.size() > 0) {
                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
                    assertEquals(1000, msgs1.size());
                }
            }
            pollingClient.unsubscribe(HOST, PORT, "Trades1","subTrades1");
            pollingClient.unsubscribe(HOST, PORT, "Trades1","subTrades2");
            conn.run("sleep(5000)");
        }
    }

    @Test(timeout = 120000)
    public void test_subscribe_tableName_actionName_reconnect() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1","subTrades1",true);
        PollingClient pollingClient1 = new PollingClient(HOST,0);
        TopicPoller poller2 = pollingClient.subscribe(HOST, PORT, "Trades1","subTrades2",true);
        for (int j=0;j<10;j++) {

            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs1 = poller1.poll(500, 10000);
                msgs2 = poller2.poll(500, 1000);
                if (msgs1 == null) {
                    continue;
                } else if (msgs1.size() > 0) {
                    BasicTable t = (BasicTable) conn.run("t");
                    assertEquals(5000, msgs1.size());
                    for (int k=0;k<msgs1.size();k++){
                        assertEquals(t.getColumn(0).get(k),msgs1.get(k).getEntity(0));
                        assertEquals(t.getColumn(1).get(k),msgs1.get(k).getEntity(1));
                        assertEquals(t.getColumn(2).get(k),msgs1.get(k).getEntity(2));
                    }

                }
                if (msgs2 == null) {
                    continue;
                } else if (msgs2.size() > 0) {
                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
                    //assertTrue(msgs2.size() >= 1000);
                    System.out.println("-----111111-----------");
                    System.out.println("i的值为"+i);
                    System.out.println("j的值为"+j);
                    System.out.println(msgs2.size());
                    System.out.println("-----222222-----------");
                }

            }
        }
        pollingClient.unsubscribe(HOST, PORT, "Trades1","subTrades1");
        pollingClient.unsubscribe(HOST, PORT, "Trades1","subTrades2");
    }

    @Test(timeout = 60000)
    public void test_TopicPoller_take() throws Exception {
        List<IMessage> list = new ArrayList<>();
        HashMap<String,Integer> map = new HashMap<>();
        map.put("dolphindb",1);
        map.put("mongodb",2);
        map.put("gaussdb",3);
        map.put("goldendb",4);
        BasicAnyVector bav = new BasicAnyVector(4);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity res = conn.run("blob(\"hello\")");
        System.out.println(res.getDataType());
        bav.set(0,new BasicInt(5));
        bav.set(1,new BasicString("DataBase"));
        bav.set(2, (Scalar) res);
        bav.set(3,new BasicDouble(15.48));
        IMessage message =new BasicMessage(0L,"first",bav,map);
        list.add(message);
        BlockingQueue<List<IMessage>> queue = new ArrayBlockingQueue<>(2);
        queue.add(list);
        TopicPoller poller1 = new TopicPoller(queue);
        System.out.println(poller1.take().getTopic());
    }

    @Test
    public void test_TopicPoller_poll() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(expected = NullPointerException.class)
    public void test_TopicPoller_setQueue() throws IOException {
        TopicPoller poller1 = pollingClient.subscribe(HOST, PORT, "Trades1", "subTread1", true);
        pollingClient.unsubscribe(HOST, PORT, "Trades", "subTread1");
        poller1.setQueue(null);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
    }

    @Test(timeout = 60000)
    public void test_PollingClient_doReconnect() throws SocketException {
        class MyPollingClient extends PollingClient{

            public MyPollingClient(String subscribeHost, int subscribePort) throws SocketException {
                super(subscribeHost, subscribePort);
            }

            @Override
            protected boolean doReconnect(Site site) {
                return super.doReconnect(site);
            }
        }
        MyPollingClient mpl = new MyPollingClient(HOST,10086);
        assertFalse(mpl.doReconnect(null));
    }
    @Test
    public void test_PollingClient_no_parameter() throws IOException {
        pollingClient = new PollingClient();
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }
    public void Handler_array(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into sub1 values( %s,%s)", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' '));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_INT() throws IOException, InterruptedException {
        PrepareStreamTable_array("INT");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_BOOL() throws IOException, InterruptedException {
        PrepareStreamTable_array("BOOL");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_CHAR() throws IOException, InterruptedException {
        PrepareStreamTable_array("CHAR");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_SHORT() throws IOException, InterruptedException {
        PrepareStreamTable_array("SHORT");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_LONG() throws IOException, InterruptedException {
        PrepareStreamTable_array("LONG");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DOUBLE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DOUBLE");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_FLOAT() throws IOException, InterruptedException {
        PrepareStreamTable_array("FLOAT");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_TIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIME");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_MINUTE() throws IOException, InterruptedException {
        PrepareStreamTable_array("MINUTE");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_SECOND() throws IOException, InterruptedException {
        PrepareStreamTable_array("SECOND");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DATETIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATETIME");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_TIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIMESTAMP");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_NANOTIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIME");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_NANOTIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIMESTAMP");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_UUID(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into sub1 values( %s,[uuid(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_UUID() throws IOException, InterruptedException {
        PrepareStreamTable_array("UUID");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_UUID(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_DATEHOUR(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into sub1 values( %s,[datehour(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DATEHOUR() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATEHOUR");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_DATEHOUR(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_IPADDR(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into sub1 values( %s,[ipaddr(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_IPADDR() throws IOException, InterruptedException {
        PrepareStreamTable_array("IPADDR");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_IPADDR(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_INT128(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into sub1 values( %s,[int128(%s)])", msg.getEntity(0).getString(), msg.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL"));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_INT128() throws IOException, InterruptedException {
        PrepareStreamTable_array("INT128");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_INT128(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_COMPLEX(List<IMessage> messages) throws IOException {
        for(int j=0;j<messages.size();j++){
            try {
                IMessage msg = messages.get(j);
                String complex1 = msg.getEntity(1).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
                //System.out.println(complex1);
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split(",");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split("\\+");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                complex1 = complex1.replaceAll("i","");
                String script = String.format("insert into sub1 values( %s,[complex(%s)])", msg.getEntity(0).getString(), complex1);
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_COMPLEX() throws IOException, InterruptedException {
        PrepareStreamTable_array("COMPLEX");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_COMPLEX(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    public void Handler_array_POINT(List<IMessage> messages) throws IOException {
        for(int j=0;j<messages.size();j++){
            try {
                IMessage msg = messages.get(j);
                String complex1 = msg.getEntity(1).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split("\\),\\(");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split(",");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                complex1 = complex1.replaceAll("\\(","").replaceAll("\\)","");
                String script = String.format("insert into sub1 values( %s,[point(%s)])", msg.getEntity(0).getString(), complex1);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_POINT() throws IOException, InterruptedException {
        PrepareStreamTable_array("POINT");
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array_POINT(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL32() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL32",3);
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL64() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL64",4);
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL128() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL128",7);
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(500,1000);
        Handler_array(messages);
        checkResult();
        pollingClient.unsubscribe(HOST, PORT, "Trades","subTread1");
    }

    class Handler_StreamDeserializer_array implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
                try {
                    BasicMessage message = deserializer_.parse(msg);
                    String timestampv = message.getEntity(0).getString();
                    String dataType = message.getEntity(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
                    String script = null;
                    if (message.getSym().equals("msg1")) {
                        script = String.format("insert into sub1 values( %s,%s)", timestampv, dataType);
                    } else if (message.getSym().equals("msg2")) {
                        script = String.format("insert into sub2 values( %s,%s)", timestampv, dataType);
                    }
                    conn.run(script);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_BOOL()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_CHAR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("CHAR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_SHORT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SHORT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_LONG()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("LONG");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DOUBLE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DOUBLE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_FLOAT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("FLOAT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_MONTH()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MONTH");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_MINUTE()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("MINUTE");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_SECOND()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("SECOND");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATETIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DATETIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_TIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("TIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIME()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIME");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_NANOTIMESTAMP()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("NANOTIMESTAMP");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    class Handler_StreamDeserializer_array_UUID implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_UUID(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[uuid(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[uuid(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_UUID()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("UUID");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_UUID handler = new Handler_StreamDeserializer_array_UUID(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    class Handler_StreamDeserializer_array_DATEHOUR implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_DATEHOUR(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[datehour(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[datehour(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DATEHOUR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("DATEHOUR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_DATEHOUR handler = new Handler_StreamDeserializer_array_DATEHOUR(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    class Handler_StreamDeserializer_array_IPADDR implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_IPADDR(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[ipaddr(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[ipaddr(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_IPADDR()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("IPADDR");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_IPADDR handler = new Handler_StreamDeserializer_array_IPADDR(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    class Handler_StreamDeserializer_array_INT128 implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_INT128(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[int128(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[int128(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_INT128()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("INT128");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_INT128 handler = new Handler_StreamDeserializer_array_INT128(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    class Handler_StreamDeserializer_array_COMPLEX implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_COMPLEX(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String complex1 = message.getEntity(1).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
                complex1 = complex1.substring(1, complex1.length() - 1);
                String[] complex2 = complex1.split(",");
                String complex3 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<complex2.length;i++){
                    complex3 = complex2[i];
                    String[] complex4 = complex3.split("\\+");
                    re1.append(complex4[0]);
                    re1.append(' ');
                    re2.append(complex4[1]);
                    re2.append(' ');
                }
                complex1 = re1+","+re2;
                String dataType = complex1.replaceAll("i","");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[complex(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[complex(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_COMPLEX()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("COMPLEX");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_COMPLEX handler = new Handler_StreamDeserializer_array_COMPLEX(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    class Handler_StreamDeserializer_array_POINT implements MessageHandler {
        private StreamDeserializer deserializer_;

        public Handler_StreamDeserializer_array_POINT(StreamDeserializer deserializer) {
            deserializer_ = deserializer;
        }

        public void doEvent(IMessage msg) {
            try {
                BasicMessage message = deserializer_.parse(msg);
                String timestampv = message.getEntity(0).getString();
                String dataType = message.getEntity(1).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
                dataType = dataType.substring(1, dataType.length() - 1);
                String[] dataType1 = dataType.split("\\),\\(");
                String dataType2 = null;
                StringBuilder re1 = new StringBuilder();
                StringBuilder re2 = new StringBuilder();
                for(int i=0;i<dataType1.length;i++){
                    dataType2 = dataType1[i];
                    String[] dataType3 = dataType2.split(",");
                    re1.append(dataType3[0]);
                    re1.append(' ');
                    re2.append(dataType3[1]);
                    re2.append(' ');
                }
                dataType = re1+","+re2;
                dataType = dataType.replaceAll("\\(","").replaceAll("\\)","");
                String script = null;
                if (message.getSym().equals("msg1")) {
                    script = String.format("insert into sub1 values( %s,[point(%s)])", timestampv,dataType);
                } else if (message.getSym().equals("msg2")) {
                    script = String.format("insert into sub2 values( %s,[point(%s)])", timestampv,dataType);
                }
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_POINT()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("POINT");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_POINT handler = new Handler_StreamDeserializer_array_POINT(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL32()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL32",3);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL64()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL64",7);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
    //@Test(timeout = 120000)
    public void test_PollingClient_subscribe_StreamDeserializer_streamTable_arrayVector_DECIMAL128()throws IOException, InterruptedException {
        PrepareStreamTableDecimal_StreamDeserializer("DECIMAL128",10);
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        BasicInt re = (BasicInt)conn.run("exec count(*) from outTables");
        System.out.println(re.getString());
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(500, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
//    @Test
//    public void test_subscribe_msgAsTable_true() throws IOException {
//        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
//        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
//
//        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",0,true,null,null,"","",true);
//        ArrayList<IMessage> msg1;
//        List<String> colNames =  Arrays.asList("tag","ts","data");
//        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
//        BasicTable bt = new BasicTable(colNames,colData);
//        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
//                "Trades1.append!(t)");
//        msg1 = poller1.poll(1000, 10000);
//        System.out.println(bt.rows());
//        assertEquals(5000, msg1.size());
//        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
//    }
//    @Test
//    public void test_subscribe_msgAsTable_false() throws IOException {
//        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
//        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
//
//        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",0,true,null,null,"","",false);
//        ArrayList<IMessage> msg1;
//        List<String> colNames =  Arrays.asList("tag","ts","data");
//        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
//        BasicTable bt = new BasicTable(colNames,colData);
//        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
//                "Trades1.append!(t)");
//        msg1 = poller1.poll(1000, 10000);
//        System.out.println(bt.rows());
//        assertEquals(5000, msg1.size());
//        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
//    }
//    @Test
//    public void test_subscribe_msgAsTable_true_allDataType() throws IOException {
//
//    }
//

    public void MessageHandler_handler(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                //System.out.println(script);
                conn.run(script);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void MessageHandler_handler1(List<IMessage> messages) throws IOException {
        for(int i=0;i<messages.size();i++){
            try {
                IMessage msg = messages.get(i);
                String script = String.format("insert into Receive values(%d,%s,%f,%d)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()),System.currentTimeMillis());
                conn.run(script);
                System.out.println(msg.getEntity(0).getString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_backupSites_port_not_true()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        TopicPoller poller = pollingClient.subscribe(HOST, 11111, "outTables", "mutiSchema", 0,false, null,null,"admin","123456",false,backupSites,100, false);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, 11111, "outTables", "mutiSchema");
    }

    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_backupSites_not_true()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+","+PORT));
        String re = null;
        try{
            TopicPoller poller = pollingClient.subscribe(HOST, 11111, "outTables", "mutiSchema", 0,false, null,null,"admin","123456",false,backupSites,10,true);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The format of backupSite "+HOST+","+PORT+" is incorrect, should be host:port, e.g. 192.168.1.1:8848", re);
    }

    @Test(timeout = 120000)
    public void test_PollingClient_subscribe_backupSites_StreamDeserializer()throws IOException, InterruptedException {
        PrepareStreamTable_StreamDeserializer("BOOL");
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("", "pub_t1"));
        tables.put("msg2", new Pair<>("", "pub_t2"));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        TopicPoller poller = pollingClient.subscribe(HOST, PORT, "outTables", "mutiSchema", 0,false, null,null,"admin","123456",false,backupSites,10,true);
        //Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        pollingClient.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }

    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+111));
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,10,true);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        //Thread.sleep(5000);
        List<IMessage> messages = poller.poll(1000,1000);
        MessageHandler_handler(messages);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_server_disconnect() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        Vector filter1 = (Vector) conn.run("1..6000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,10,true);
        System.out.println("Successful subscribe");
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        conn1.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        //List<IMessage> messages = poller.poll(5000,5000);
        //MessageHandler_handler(messages);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(8000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        conn.run("t=table(5001..5500 as tag,now()+5001..5500 as ts,rand(100.0,500) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        List<IMessage> messages1 = poller.poll(5000,5500);
        MessageHandler_handler(messages1);
        //Thread.sleep(5000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("5500",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test//(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_server_disconnect_backupSites_disconnect_subOnce_false() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,10,false);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        //List<IMessage> messages = poller.poll(1000,1000);
        //MessageHandler_handler(messages);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(2000);
        //List<IMessage> messages1 = poller.poll(2000,2000);
        //MessageHandler_handler(messages1);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(2000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(10000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        List<IMessage> messages2 = poller.poll(3000,3000);
        MessageHandler_handler(messages2);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(5000);

        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("3000",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_server_disconnect_backupSites_disconnect_subOnce_true() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,10,true);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        //List<IMessage> messages = poller.poll(1000,1000);
        //MessageHandler_handler(messages);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        //List<IMessage> messages1 = poller.poll(2000,2000);
        //MessageHandler_handler(messages1);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        List<IMessage> messages2 = poller.poll(3000,3000);
        MessageHandler_handler(messages2);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(5000);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("2000",row_num.getColumn(0).get(0).getString());
        DBConnection conn4 = new DBConnection();
        conn4.connect(HOST,port_list[2],"admin","123456");
        conn4.run(script1);
        conn4.run(script2);
        //client.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_unsubscribe() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+PORT));
        TopicPoller poller = pollingClient.subscribe(HOST,11111,"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,10,false);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        List<IMessage> messages = poller.poll(5000,5000);
        MessageHandler_handler(messages);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,11111,"Trades","subTread1");
    }
    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_resubTimeout_not_true() throws IOException, InterruptedException {
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+111));
        TopicPoller poller = pollingClient.subscribe(HOST,PORT,"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,-10,true);
        System.out.println("Successful subscribe");
        conn.run("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        List<IMessage> messages = poller.poll(1000,1000);
        MessageHandler_handler(messages);
        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("1000",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_resubTimeout() throws Exception {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data`now,[INT,TIMESTAMP,DOUBLE,TIMESTAMP])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[2],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        Vector filter1 = (Vector) conn.run("1..10000000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites,1000,true);
        System.out.println("Successful subscribe");
        class MyThread extends Thread {
            @Override
            public void run() {
                try {
                    conn3.run("for(n in 1..1000){\n" +
                            "    insert into Trades values(n,now()+n,n);\n" +
                            "    sleep(50);\n" +
                            "}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println( e.getMessage());
                }
            }
        }
        class MyThread1 extends Thread {
            @Override
            public void run() {
                try {
                    conn1.run("for(n in 1..1000){\n" +
                            "    insert into Trades values(n,now()+n,n);\n" +
                            "    sleep(50);\n" +
                            "}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println( e.getMessage());
                }
            }
        }
        class MyThread2 extends Thread {
            @Override
            public void run() {
                try {
                    controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
                } catch (Exception e) {
                    // 捕获异常并打印错误信息
                    System.err.println(e.getMessage());
                }
            }
        }
        MyThread thread = new MyThread();
        MyThread1 thread1 = new MyThread1();
        MyThread2 thread2 = new MyThread2();
        thread.start();
        thread1.start();
        Thread.sleep(1000);
        thread2.start();
        List<IMessage> messages = poller.poll(1000,1000);
        System.out.println("messages" + messages.size());
        MessageHandler_handler1(messages);
        Thread.sleep(1000);
        thread.join();
        Thread.sleep(5000);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        //Thread.sleep(1000);
        List<IMessage> messages1 = poller.poll(1000,1000);
        //Thread.sleep(1000);
        System.out.println(messages1.size());
        //Assert.assertEquals(1000,messages1.size());
        MessageHandler_handler1(messages1);
        //Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select tag ,now,deltas(now) from Receive  order by  deltas(now) desc \n");
        System.out.println(re.getString());
        Assert.assertEquals(1000,re.rows());
        Assert.assertEquals(true,Integer.valueOf(re.getColumn(2).get(0).toString())>1000);
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[1],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        pollingClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    @Test//(timeout = 180000)
    public void test_PollingClient_subscribe_resubTimeout_subOnce_not_set() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(5000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,port_list[2],"admin","123456");
        conn2.run(script1);
        conn2.run(script2);
        Vector filter1 = (Vector) conn.run("1..100000");
        List<String> backupSites = new ArrayList<>(Collections.singleton(HOST+":"+port_list[2]));
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites);
        System.out.println("Successful subscribe");
        conn1.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        conn2.run("n=1000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        //Thread.sleep(1000);
        //List<IMessage> messages = poller.poll(1000,1000);
        //MessageHandler_handler(messages);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        System.out.println(port_list[1]+"断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn2.run("n=2000;t=table(1001..n as tag,timestamp(1001..n) as ts,take(100.0,1000) as data);" + "Trades.append!(t)");
        Thread.sleep(1000);
        //List<IMessage> messages1 = poller.poll(2000,2000);
       // MessageHandler_handler(messages1);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        Thread.sleep(5000);
        DBConnection conn3 = new DBConnection();
        conn3.connect(HOST,port_list[1],"admin","123456");
        conn3.run(script1);
        conn3.run(script2);
        controller_conn.run("try{stopDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        System.out.println(port_list[2]+"节点断掉啦---------------------------------------------------");
        Thread.sleep(8000);
        conn3.run("n=3000;t=table(1..n as tag,timestamp(1..n) as ts,take(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        List<IMessage> messages2 = poller.poll(3000,3000);
        MessageHandler_handler(messages2);
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[2]+"')}catch(ex){}");
        Thread.sleep(5000);

        BasicTable row_num = (BasicTable)conn.run("select count(*) from Receive");
        System.out.println(row_num.getColumn(0).get(0));
        assertEquals("3000",row_num.getColumn(0).get(0).getString());
        pollingClient.unsubscribe(HOST,port_list[1],"Trades","subTread1");
    }

    //@Test(timeout = 180000)
    public void test_PollingClient_subscribe_backupSites_server_disconnect_1() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        controller_conn.run("try{startDataNode('"+HOST+":"+port_list[1]+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        String script1 = "st1 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st1,`Trades)\t\n"
                + "setStreamTableFilterColumn(objByName(`Trades),`tag)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "share(st2, `Receive)\t\n";
        conn.run(script2);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,port_list[1],"admin","123456");
        conn1.run(script1);
        conn1.run(script2);

        Vector filter1 = (Vector) conn.run("1..50000");
        List<String> backupSites = Arrays.asList(new String[]{"192.168.0.69:18921", "192.168.0.69:18922", "192.168.0.69:18923"});
        TopicPoller poller = pollingClient.subscribe(HOST,port_list[1],"Trades","subTread1", -1,true,filter1, (StreamDeserializer) null,"admin","123456",false,backupSites);
        System.out.println("这里可以手工断掉这个集群下所有可用节点http://192.168.0.69:18920/?view=overview-old");
        Thread.sleep(1000000);
    }
    @Test//(timeout = 60000)
    public void test_subscribe_getOffset() throws IOException {
        conn.run("setStreamTableFilterColumn(Trades1,`tag);" +
                "filter=1..5");
        BasicIntVector filter = (BasicIntVector) conn.run("filter");
        TopicPoller poller1 = pollingClient.subscribe(HOST,PORT,"Trades1","subTread1",-1,filter);
        ArrayList<IMessage> msg1;
        conn.run("n=5;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 5);
        assertEquals(5, msg1.size());
        System.out.println("msg1.get(0).getOffset() :" + msg1.get(0).getOffset());
        System.out.println("msg1.get(1).getOffset() :" + msg1.get(1).getOffset());
        System.out.println("msg1.get(2).getOffset() :" + msg1.get(2).getOffset());
        System.out.println("msg1.get(3).getOffset() :" + msg1.get(3).getOffset());
        System.out.println("msg1.get(4).getOffset() :" + msg1.get(4).getOffset());
        assertEquals(0, msg1.get(0).getOffset());
        assertEquals(1, msg1.get(1).getOffset());
        assertEquals(2, msg1.get(2).getOffset());
        assertEquals(3, msg1.get(3).getOffset());
        assertEquals(4, msg1.get(4).getOffset());
        pollingClient.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }
}
