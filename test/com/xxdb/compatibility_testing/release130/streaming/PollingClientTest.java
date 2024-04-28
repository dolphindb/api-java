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
package com.xxdb.compatibility_testing.release130.streaming;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;
import org.junit.*;
import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import static org.junit.Assert.*;

public class PollingClientTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/compatibility_testing/release130/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT=9002;
    public static PollingClient client;
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
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            client = new PollingClient(HOST,9053);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        clear_env();
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
    }

    @After
    public  void after() throws IOException, InterruptedException {
        try {
            client.unsubscribe(HOST, PORT, "Trades", "subtrades");
            client.unsubscribe(HOST, PORT, "Trades", "subtrades1");
            client.unsubscribe(HOST, PORT, "Trades", "subtrades2");
            client.unsubscribe(HOST, PORT, "Trades");
        }catch (Exception e){
        }
        try {
            conn.run("login(`admin,`123456);" +
                    "try{dropStreamTable('Trades')}catch(ex){};"+
                    "try{dropStreamTable('Receive')}catch(ex){};"+
                    "try{deleteUser(`test1)}catch(ex){};" +
                    "userlist=getUserList();" +
                    "grouplist=getGroupList();" +
                    "loop(deleteUser,userlist);" +
                    "loop(deleteGroup,grouplist)");
        } catch (Exception e) {
        }
        try{conn.run("dropStreamTable(`Trades)");}catch (Exception e){}
        clear_env();
        client.close();
        conn.close();
        Thread.sleep(2000);
    }

    public void wait_data(String table_name,int data_row) throws IOException, InterruptedException {
        BasicInt row_num;
        while(true){
            row_num = (BasicInt)conn.run("(exec count(*) from "+table_name+")[0]");
//            System.out.println(row_num.getInt());
            if(row_num.getInt() == data_row){
                break;
            }
            Thread.sleep(100);
        }
    }

    @Test(expected = RuntimeException.class)
    public  void error_size1() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        msgs = poller1.poll(1000,0);
      }
    @Test(expected = RuntimeException.class)
    public  void error_size2() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs = poller1.poll(1000,-10);
    }

    @Test(timeout = 120000)
    public  void test_size() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        try {
            for (int i=0;i<10;i++){//data<size
                conn.run("n=50;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs = poller1.poll(100,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    assertEquals(50, msgs.size());
                }
            }
            for (int i=0;i<10;i++){//data>size
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs = poller1.poll(100000,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1000);
                }
            }
            for (int i=0;i<10;i++){//data=size
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs = poller1.poll(1000000,5000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    BasicInt value = (BasicInt) msgs.get(0).getEntity(0);
                    assertTrue(msgs.size()>=1000);
                }
            }
            for (int i=0;i<10;i++){//bigsize
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
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
                conn.run("n=10000000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
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
                conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
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
                conn.run("n=10;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                conn.run("n=20;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                conn.run("n=30;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                conn.run("n=40;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
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
        client.unsubscribe(HOST, PORT, "Trades","subtrades");
    }

    @Test(timeout = 120000)
    public  void test_size_resubscribe() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades", "subtrades1", -1, true);
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades", "subtrades2", -1, true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs1 = poller1.poll(100, 10000);
                msgs2 = poller2.poll(100, 1000);
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
            client.unsubscribe(HOST, PORT, "Trades", "subtrades1");
            client.unsubscribe(HOST, PORT, "Trades", "subtrades2");
        }
    }
    @Test(timeout = 120000)
    public void test_subscribe_admin_login() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades", "subtrades1", -1, true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msgs1 = poller1.poll(100, 10000);
        assertEquals(5000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades", "subtrades1");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_user_error() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin_error","123456");
    }

    @Test(expected = IOException.class)
    public void test_subscribe_password_error() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin","error_password");

    }

    @Test(timeout = 120000)
    public void test_subscribe_admin() throws IOException, InterruptedException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msgs1 = poller1.poll(100, 10000);
        assertEquals(5000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }
    @Test(timeout = 120000)
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);");
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        Thread.sleep(5000);
        ArrayList<IMessage> msgs1 = poller1.poll(100, 10000);
        assertEquals(10000, msgs1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});");
        try {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades", "subTread1", -1, true, null, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Test(timeout = 120000)
    public void test_subscribe_other_some_user() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};try{deleteUser(`test2)}catch(ex){};try{deleteUser(`test3)}catch(ex){};createUser(`test1, '123456');createUser(`test2, '123456');createUser(`test3, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades\"});");
        try {
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades", "subTread1",  -1, true, null, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades", "subTread1",-1, true, null, "test2", "123456");

        try {
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades", "subTread1", -1, true,null, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
        ArrayList<IMessage> msgs1 = poller1.poll(100, 10000);
        assertEquals(10000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_one_user_some_table() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st1;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st2;"+
                "share streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE]) as tmp_st3;");
        TopicPoller poller1 = client.subscribe(HOST,PORT,"tmp_st1","subTread1",-1,true,null,"test1","123456");
        TopicPoller poller2 = client.subscribe(HOST,PORT,"tmp_st2","subTread1",-1,true,null,"test1","123456");
        try {
            TopicPoller poller3 = client.subscribe(HOST, PORT, "tmp_st3", "subTread1",-1, true, null, "test1", "123456_error");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st1.append!(t)");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "tmp_st2.append!(t)");
        ArrayList<IMessage> msgs1 = poller1.poll(100, 10000);
        assertEquals(10000, msgs1.size());
        ArrayList<IMessage> msgs2 = poller2.poll(100, 10000);
        assertEquals(10000, msgs1.size());
    }

    @Test
    public void test_subscribe_offset() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_defaultActionName_offset() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades",-1);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades");
    }

    @Test
    public void test_subscribe_TableName() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades");
    }

    @Test
    public void test_subscribe_tableName_ActionName() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test
    public void test_subscribe_tableName_ActionName_offset_filter() throws IOException {
        conn.run("setStreamTableFilterColumn(Trades,`tag);" +
                "filter=1 2 3 4 5");
        BasicIntVector filter = (BasicIntVector) conn.run("filter");
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,filter);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_tableName_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades",true);
            //PollingClient client1 = new PollingClient(HOST,9069);
            //TopicPoller poller2 = client1.subscribe(HOST, PORT, "Trades",true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs1 = poller1.poll(100, 10000);
                //msgs2 = poller2.poll(100, 1000);
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
                //if (msgs2 == null) {
//                    continue;
//                } else if (msgs2.size() > 0) {
//                    BasicInt value = (BasicInt) msgs2.get(0).getEntity(0);
//                    assertTrue(msgs2.size() >= 1000);
//                }

            }
            client.unsubscribe(HOST, PORT, "Trades");
            //client1.unsubscribe(HOST, PORT, "Trades");
        }

    }

    @Test
    public void test_subscribe_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades",-1,true);
            //PollingClient client1 = new PollingClient(HOST,9069);
            //TopicPoller poller2 = client1.subscribe(HOST, PORT, "Trades",-1,true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs1 = poller1.poll(100, 10000);
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
            client.unsubscribe(HOST, PORT, "Trades");
            //client1.unsubscribe(HOST, PORT, "Trades");
        }
    }

    @Test
    public void test_subscribe_tableName_actionName_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subTrades1",-1,true);
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades","subTrades2",-1,true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs1 = poller1.poll(100, 10000);
                msgs2 = poller2.poll(100, 1000);
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
            client.unsubscribe(HOST, PORT, "Trades","subTrades1");
            client.unsubscribe(HOST, PORT, "Trades","subTrades2");
        }
    }

    @Test
    public void test_subscribe_tableName_actionName_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subTrades1",true);
            PollingClient client1 = new PollingClient(HOST,9069);
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades","subTrades2",true);
            ArrayList<IMessage> msgs1;
            ArrayList<IMessage> msgs2;
            for (int i = 0; i < 10; i++) {
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades.append!(t)");
                msgs1 = poller1.poll(100, 10000);
                msgs2 = poller2.poll(100, 1000);
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
            client.unsubscribe(HOST, PORT, "Trades","subTrades1");
            client.unsubscribe(HOST, PORT, "Trades","subTrades2");
        }
    }

    @Test
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
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }

    @Test(expected = NullPointerException.class)
    public void test_TopicPoller_setQueue() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades", "subTread1", true);
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
        poller1.setQueue(null);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(100, 10000);
        assertEquals(5000, msg1.size());
    }

    @Test
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
    //@Test //bug
    public void test_subscribe_msgAsTable_true() throws IOException {
        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {

        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",0,true,null,null,"","",true);
        ArrayList<IMessage> msg1;
        List<String> colNames =  Arrays.asList("tag","ts","data");
        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
        BasicTable bt = new BasicTable(colNames,colData);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        System.out.println(bt.rows());
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_msgAsTable_false() throws IOException {
        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {

        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",0,true,null,null,"","",false);
        ArrayList<IMessage> msg1;
        List<String> colNames =  Arrays.asList("tag","ts","data");
        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
        BasicTable bt = new BasicTable(colNames,colData);
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        System.out.println(bt.rows());
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades","subTread1");
    }
    @Test
    public void test_subscribe_msgAsTable_true_allDataType() throws IOException {

    }
}
