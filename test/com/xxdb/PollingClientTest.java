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
package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.PollingClient;
import com.xxdb.streaming.client.TopicPoller;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class PollingClientTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    public static PollingClient client;
    @BeforeClass
    public static void set() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
            client = new PollingClient(HOST,8676);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\tstopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)\n" +
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
        clear_env();
        conn.run("st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades, asynWrite=true, compress=true, cacheSize=20000, retentionMinutes=180)\t\n");
    }

    @After
    public  void after() throws IOException {
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
    }

    @AfterClass
    public static void cls() throws IOException {
        try {
            conn.close();
            client.close();
        }catch (Exception e){
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

    @Test
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

    @Test
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
    @Test
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

    @Test
    public void test_subscribe_admin() throws IOException, InterruptedException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades","subTread1",-1,true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades.append!(t)");
        msgs1 = poller1.poll(100, 10000);
        assertEquals(5000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
    }
    @Test
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

    @Test
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

    @Test
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

    @Test
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

}
