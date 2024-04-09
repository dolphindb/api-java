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

import com.xxdb.BasicDBTask;
import com.xxdb.DBConnection;
import com.xxdb.DBTask;
import com.xxdb.ExclusiveDBConnectionPool;
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

import static com.xxdb.Prepare.clear_env;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.PrepareStreamTableDecimal_StreamDeserializer;
import static com.xxdb.streaming.reverse.ThreadedClientsubscribeReverseTest.PrepareStreamTable_StreamDeserializer;
import static org.junit.Assert.*;

public class PollingClientReverseTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT=9002;
    public static PollingClient client;
    private StreamDeserializer deserializer_;

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
    public static void PrepareStreamTable_array(String dataType) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n"+
                "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n"+
                "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n"+
                "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n"+
                "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n"+"" +
                "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n"+
                "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n"+
                "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n"+
                "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n"+
                "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n"+
                "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n"+
                "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n"+
                "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n"+
                "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n"+
                "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n"+
                "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n"+
                "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n"+
                "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType +"[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareStreamTableDecimal_array(String dataType, int scale) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"("+scale+")[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_DECIMAL32 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL64 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL128 =   array(DECIMAL128(8)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType +"("+scale+")[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void checkResult() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            if (tmpNum.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(1000);
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
        for (int i = 0; i < 10; i++)
        {
            BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1 ");
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2 ");
            if (tmpNum.getInt()==(1000)&& tmpNum1.getInt()==(1000))
            {
                break;
            }
            Thread.sleep(3000);
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
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        msgs = poller1.poll(1000,0);
       }
    @Test(expected = IOException.class)
    public  void error_size2() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades","subtrades",-1,true);
        ArrayList<IMessage> msgs = poller1.poll(1000,-10);
    }

    @Test(timeout = 120000)
    public  void test_size() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1","subtrades",-1,true);
        ArrayList<IMessage> msgs;
        try {
            for (int i=0;i<10;i++){//data<size
                conn.run("n=50;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
                msgs = poller1.poll(100,1000);
                if (msgs==null){
                    continue;
                }
                else if (msgs.size() > 0) {
                    assertEquals(50, msgs.size());
                }
            }
            for (int i=0;i<10;i++){//data>size
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
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
                conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
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
        client.unsubscribe(HOST, PORT, "Trades1","subtrades");
    }

    @Test(timeout = 120000)
    public  void test_size_resubscribe() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1", "subtrades1", -1, true);
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1", "subtrades2", -1, true);
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
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades1");
            client.unsubscribe(HOST, PORT, "Trades1", "subtrades2");
        }
    }
    @Test(timeout = 120000)
    public void test_subscribe_admin_login() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1", "subtrades1", -1, true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msgs1 = poller1.poll(1000, 10000);
        assertEquals(5000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades1", "subtrades1");
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
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",-1,true,null,"admin","123456");
        ArrayList<IMessage> msgs1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msgs1 = poller1.poll(100, 10000);
        assertEquals(5000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades1", "subTread1");
    }
    @Test(timeout = 120000)
    public void test_subscribe_other_user() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);");
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",-1,true,null,"test1","123456");
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        Thread.sleep(5000);
        ArrayList<IMessage> msgs1 = poller1.poll(500, 10000);
        assertEquals(10000, msgs1.size());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_other_user_unallow() throws IOException, InterruptedException {
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);" +
                "colNames =`id`timestamp`sym`qty`price;" +
                "colTypes = [INT,TIMESTAMP,SYMBOL,INT,DOUBLE];" +
                "t2=streamTable(1:0,colNames,colTypes);"+
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades1\"});");
        try {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1", "subTread1", -1, true, null, "test1", "123456");
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
                "rpc(getControllerAlias(),deny{`test1,TABLE_READ,getNodeAlias()+\":Trades1\"});"+
                "rpc(getControllerAlias(),grant{`test2,TABLE_READ,getNodeAlias()+\":Trades1\"});");
        try {
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1", "subTread1",  -1, true, null, "test1", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(111+e.getMessage());
        }
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1", "subTread1",-1, true, null, "test2", "123456");

        try {
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1", "subTread1", -1, true,null, "test3", "123456");
            fail("no exception thrown");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        conn.run("n=10000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + "Trades1.append!(t)");
        ArrayList<IMessage> msgs1 = poller1.poll(10000, 10000);
        assertEquals(10000, msgs1.size());
        client.unsubscribe(HOST, PORT, "Trades1", "subTread1");
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
        ArrayList<IMessage> msgs1 = poller1.poll(1000, 10000);
        assertEquals(10000, msgs1.size());
        ArrayList<IMessage> msgs2 = poller2.poll(1000, 10000);
        assertEquals(10000, msgs1.size());
    }

    @Test(timeout = 60000)
    public void test_subscribe_offset() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",0);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_defaultActionName_offset() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1",-1);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_TableName() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_tableName_ActionName() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1");
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(1000, 10000);
        assertEquals(5000, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 60000)
    public void test_subscribe_tableName_ActionName_offset_filter() throws IOException {
        conn.run("setStreamTableFilterColumn(Trades1,`tag);" +
                "filter=1 2 3 4 5");
        BasicIntVector filter = (BasicIntVector) conn.run("filter");
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",-1,filter);
        ArrayList<IMessage> msg1;
        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(500, 10000);
        assertEquals(5, msg1.size());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(timeout = 120000)
    public void test_subscribe_tableName_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1",true);
            //PollingClient client1 = new PollingClient(HOST,9069);
            //TopicPoller poller2 = client1.subscribe(HOST, PORT, "Trades1",true);
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
            client.unsubscribe(HOST, PORT, "Trades1");
          //  client1.unsubscribe(HOST, PORT, "Trades1");
        }

    }

    @Test(timeout = 200000)
    public void test_subscribe_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1",-1,true);
            PollingClient client1 = new PollingClient(HOST,0);
            //TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1",-1,true);
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
            client.unsubscribe(HOST, PORT, "Trades1");
//            client1.unsubscribe(HOST, PORT, "Trades1");
        }
    }

    @Test(timeout = 200000)
    public void test_subscribe_tableName_actionName_offset_reconnect() throws IOException {
        for (int j=0;j<10;j++) {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1","subTrades1",-1,true);
            TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1","subTrades2",-1,true);
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
            client.unsubscribe(HOST, PORT, "Trades1","subTrades1");
            client.unsubscribe(HOST, PORT, "Trades1","subTrades2");
            conn.run("sleep(5000)");
        }
    }

    @Test(timeout = 120000)
    public void test_subscribe_tableName_actionName_reconnect() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1","subTrades1",true);
        PollingClient client1 = new PollingClient(HOST,0);
        TopicPoller poller2 = client.subscribe(HOST, PORT, "Trades1","subTrades2",true);
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
            client.unsubscribe(HOST, PORT, "Trades1","subTrades1");
            client.unsubscribe(HOST, PORT, "Trades1","subTrades2");
        }
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
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
    }

    @Test(expected = NullPointerException.class)
    public void test_TopicPoller_setQueue() throws IOException {
        TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades1", "subTread1", true);
        client.unsubscribe(HOST, PORT, "Trades", "subTread1");
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
        client = new PollingClient();
        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",true);
        ArrayList<IMessage> msg1;
        conn.run("n=1;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
                "Trades1.append!(t)");
        msg1 = poller1.poll(100);
        assertEquals(1, msg1.size());
        BasicTable bt = (BasicTable) conn.run("getStreamingStat().pubTables;");
        System.out.println(bt.getString());
        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_BOOL() throws IOException, InterruptedException {
        PrepareStreamTable_array("BOOL");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_CHAR() throws IOException, InterruptedException {
        PrepareStreamTable_array("CHAR");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_SHORT() throws IOException, InterruptedException {
        PrepareStreamTable_array("SHORT");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_LONG() throws IOException, InterruptedException {
        PrepareStreamTable_array("LONG");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DOUBLE() throws IOException, InterruptedException {
        PrepareStreamTable_array("DOUBLE");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_FLOAT() throws IOException, InterruptedException {
        PrepareStreamTable_array("FLOAT");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_TIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIME");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_MINUTE() throws IOException, InterruptedException {
        PrepareStreamTable_array("MINUTE");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_SECOND() throws IOException, InterruptedException {
        PrepareStreamTable_array("SECOND");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DATETIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("DATETIME");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_TIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("TIMESTAMP");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_NANOTIME() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIME");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_NANOTIMESTAMP() throws IOException, InterruptedException {
        PrepareStreamTable_array("NANOTIMESTAMP");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_UUID(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_DATEHOUR(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_IPADDR(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_INT128(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_COMPLEX(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array_POINT(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL32() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL32",3);
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL64() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL64",4);
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
    }
    @Test(timeout = 60000)
    public void Test_PollingClient_subscribe_arrayVector_DECIMAL128() throws IOException, InterruptedException {
        PrepareStreamTableDecimal_array("DECIMAL128",7);
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTread1",0);
        String script2 = "Trades.append!(pub_t);";
        conn.run(script2);
        List<IMessage> messages = poller.poll(1000,1000);
        Handler_array(messages);
        checkResult();
        client.unsubscribe(HOST, PORT, "Trades","subTread1");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_UUID handler = new Handler_StreamDeserializer_array_UUID(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_DATEHOUR handler = new Handler_StreamDeserializer_array_DATEHOUR(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(2000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_IPADDR handler = new Handler_StreamDeserializer_array_IPADDR(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_INT128 handler = new Handler_StreamDeserializer_array_INT128(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_COMPLEX handler = new Handler_StreamDeserializer_array_COMPLEX(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array_POINT handler = new Handler_StreamDeserializer_array_POINT(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
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
        TopicPoller poller = client.subscribe(HOST, PORT, "outTables", "mutiSchema", 0);
        Thread.sleep(30000);
        List<IMessage> messages = poller.poll(1000, 2000);
        System.out.println(messages.size());
        Handler_StreamDeserializer_array handler = new Handler_StreamDeserializer_array(streamFilter);
        for (int i = 0; i < messages.size(); i++) {
            IMessage msg = messages.get(i);
            handler.doEvent(msg);
        }
        checkResult1();
        client.unsubscribe(HOST, PORT, "outTables", "mutiSchema");
    }
//    @Test
//    public void test_subscribe_msgAsTable_true() throws IOException {
//        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
//        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
//
//        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",0,true,null,null,"","",true);
//        ArrayList<IMessage> msg1;
//        List<String> colNames =  Arrays.asList("tag","ts","data");
//        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
//        BasicTable bt = new BasicTable(colNames,colData);
//        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
//                "Trades1.append!(t)");
//        msg1 = poller1.poll(1000, 10000);
//        System.out.println(bt.rows());
//        assertEquals(5000, msg1.size());
//        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
//    }
//    @Test
//    public void test_subscribe_msgAsTable_false() throws IOException {
//        //public TopicPoller subscribe(String host, int port, String tableName, String actionName, long offset, boolean reconnect, Vector filter, StreamDeserializer
//        //deserializer, String userName, String passWord, boolean msgAsTable) throws IOException {
//
//        TopicPoller poller1 = client.subscribe(HOST,PORT,"Trades1","subTread1",0,true,null,null,"","",false);
//        ArrayList<IMessage> msg1;
//        List<String> colNames =  Arrays.asList("tag","ts","data");
//        List<Vector> colData = Arrays.asList(new BasicIntVector(0),new BasicTimestampVector(0),new BasicDoubleVector(0));
//        BasicTable bt = new BasicTable(colNames,colData);
//        conn.run("n=5000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" +
//                "Trades1.append!(t)");
//        msg1 = poller1.poll(1000, 10000);
//        System.out.println(bt.rows());
//        assertEquals(5000, msg1.size());
//        client.unsubscribe(HOST,PORT,"Trades1","subTread1");
//    }
//    @Test
//    public void test_subscribe_msgAsTable_true_allDataType() throws IOException {
//
//    }
//
//    @Test
//    public void test_11() throws IOException {
//        List<DBTask> tasks = new ArrayList<>();
//        long startTime = System.currentTimeMillis();
//        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT, "admin", "123456", 20, false, true);
//        while (true) {
//            try {
//                // 创建任务
//                BasicDBTask task = new BasicDBTask("1..10");
//                // 执行任务
//                pool.execute(task);
//                BasicIntVector data = null;
//                if (task.isSuccessful()) {
//                    data = (BasicIntVector)task.getResult();
//                } else {
//                    throw new Exception(task.getErrorMsg());
//                }
//                System.out.print(data.getString()+"\n");
//
//                // 等待1秒
//                Thread.sleep(1000);
//            } catch (Exception e) {
//                // 捕获异常并打印错误信息
//                System.err.println("Error executing task: " + e.getMessage());
//            }
//        }
//    }
}
