package com.xxdb.restart;

import com.xxdb.*;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicTable;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class LoadBalanceTest {
    static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String[] host_list= bundle.getString("HOSTS").split(",");
    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();
    static String controller_host = bundle.getString("CONTROLLER_HOST");
    static int controller_port = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        for(int i = 0; i<ipports.length; i++){
            String site = ipports[i];
            controller_conn.run("try{startDataNode('" + site + "')}catch(ex){}");
            System.out.println(site);
        }
    }

    @Test(timeout = 120000)
    public void test_load_balance_create_connect_highAvailabilitySites_error() throws IOException {
        conn = new DBConnection();
        String[] tmp_ipports = new String[]{"192.168.1.167:23"};
        conn.connect(host_list[0],port_list[0],"admin","123456","",true,tmp_ipports);
        assertEquals(host_list[0],conn.getHostName());
        assertEquals(port_list[0],conn.getPort());
    }

    @Test(timeout = 120000)
    public void test_load_balance_create_session() throws IOException, InterruptedException {
        List<String> hosts = Arrays.asList(host_list);
        List<Integer> ports = Arrays.stream(port_list).boxed().collect(Collectors.toList());
        for(int i = 0;i<20;i++) {
            Thread.sleep(1000);
            conn = new DBConnection();
            conn.connect(controller_host,controller_port,"admin","123456","",true,ipports);
            String now_host = conn.getHostName();
            Integer now_port = conn.getPort();
            System.out.println("now host is "+conn.getHostName());
            System.out.println("now port is "+conn.getPort());
            assertEquals(true,hosts.contains(conn.getHostName()));
            assertEquals(true,ports.contains(conn.getPort()));
        }
        conn.close();
    }

    public static class test_connect implements Runnable {
        @Override
        public void run() {
            while(true){
                try {
                    conn.run("1+1");
                } catch (IOException e) {
                    System.out.println(e);
                }
            }
        }
    }


    @Ignore
    public void test_load_balance_change_nodes() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        conn = new DBConnection();
        conn.connect(host_list[0],port_list[0],"admin","123456","",true,ipports);
        test_connect test_connect1 = new test_connect();
        new Thread(test_connect1).start();
        List<String> hosts = Arrays.asList(host_list);
        List<Integer> ports = Arrays.stream(port_list).boxed().collect(Collectors.toList());
        for(int i = 0;i<20;i++) {
            Thread.sleep(1000);
            String now_host = conn.getHostName();
            Integer now_port = conn.getPort();
            controller_conn.run("try{stopDataNode('"+now_host+":"+now_port+"')}catch(ex){}");
            controller_conn.run("sleep(8000)");
            System.out.println("now host is "+conn.getHostName());
            System.out.println("now port is "+conn.getPort());
            controller_conn.run("try{startDataNode('"+now_host+":"+now_port+"')}catch(ex){}");
            assertEquals(true,hosts.contains(conn.getHostName()));
            assertEquals(true,ports.contains(conn.getPort()));
            BasicInt a = (BasicInt) conn.run("1+1");
            assertEquals(2,a.getInt());
        }
        assertEquals(true,hosts.contains(conn.getHostName()));
        assertEquals(true,ports.contains(conn.getPort()));
        BasicInt a = (BasicInt) conn.run("1+1");
        assertEquals(2,a.getInt());
        controller_conn.close();
        conn.close();
    }

    public static class add_data_to_haStream implements Runnable {
        @Override
        public void run() {
            while(true){
                try {
                    conn.run("n = 100000;t1 = table(100:0, `timestampv`sym`qty`price1, [TIMESTAMP, SYMBOL,INT, DOUBLE]);\n" +
                            "share t1 as table1;\n" +
                            "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, take(`a`b`c,n), rand(100,n),rand(1.0, n));\n" +
                            "leader_node = getStreamingLeader(11)\n" +
                            "rpc(leader_node,replay,table1,`ha_stream,`timestampv,`timestampv)");
                } catch (IOException e) {
                    System.out.println(e);
                }
            }
        }
    }

    public static MessageHandler MessageHandler_handler = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
//                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
//                conn.run(script);
                  System.out.println(msg.getEntity(0).getString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    //@Test(timeout = 120000)
    public void test_load_balance_ha_stream() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        conn = new DBConnection();
        ThreadedClient client = new ThreadedClient("127.0.0.1", 0);
        conn.connect(host_list[0],port_list[0],"admin","123456","",true,ipports);
        conn.run("haTableName='ha_stream'; " +
                "try{ dropStreamTable(haTableName); }catch(ex){}\n " +
                "t = table(1:0, `timestamp`sym`qty`price,[TIMESTAMP,SYMBOL,INT,DOUBLE]);" +
                "haStreamTable(11,t,haTableName,1000000);");
        conn.run("n = 100000;t1 = table(100:0, `timestampv`sym`qty`price1, [TIMESTAMP, SYMBOL,INT, DOUBLE]);\n" +
                "share t1 as table1;\n" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, take(`a`b`c,n), rand(100,n),rand(1.0, n));\n" +
                "leader_node = getStreamingLeader(11)\n" +
                "rpc(leader_node,replay,table1,`ha_stream,`timestampv,`timestampv)");
        //conn.run("select * from ha_stream");
        conn.run("leader_node = getStreamingLeader(11)\n" +
                "re = select host,port from rpc(getControllerAlias(),getClusterPerf) where name = leader_node\n");
        BasicString leader_host = (BasicString) conn.run("re.host[0]");
        BasicInt leader_port = (BasicInt) conn.run("re.port[0]");
        client.subscribe(leader_host.getString(), leader_port.getInt(), "ha_stream","test_ha_sub",MessageHandler_handler);

    }

    //@Test(timeout = 120000)
    public void test_load_balance_select_session() throws IOException, InterruptedException {
        for(int i = 0;i < 30;i++) {
            for(int x = 3;x > 0;x--) {
                DBConnection tmp_conn = new DBConnection();
                tmp_conn.connect(host_list[x], port_list[x], "admin", "123456");
            }
        }
        List<String> hosts = Arrays.asList(host_list);
        List<Integer> ports = Arrays.stream(port_list).boxed().collect(Collectors.toList());
        ArrayList<String> now_hosts = new ArrayList<String>();
        ArrayList<Integer> now_ports = new ArrayList<Integer>();
        for(int i = 0;i<10;i++) {
            conn = new DBConnection();
            conn.connect("", 1, "admin", "123456", "", true, ipports);
            String now_host = conn.getHostName();
            Integer now_port = conn.getPort();
            System.out.println("now host is " + now_host);
            System.out.println("now port is " + now_port);
            now_hosts.add(now_host);
            now_ports.add(now_port);
        }

        for(int i = 0;i<10;i++) {
            assertEquals(host_list[0], now_hosts.toArray()[i]);
            assertEquals(port_list[0], now_ports.toArray()[i]);
        }
    }
    //@Test(timeout = 120000) //port memory need high load,then connect to ipports‘s node
    public void Test_getConnection_enableHighAvailability_true_memory_high_load() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 460; i++) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        BasicTable re = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");

        for (int i = 0; i < re.rows()-1; ++i) {
            System.out.println("port:"+ re.getColumn(0).get(i)+" connectionNum:"+re.getColumn(1).get(i));
        }
    }

    //@Test(timeout = 60000)
    public void Test_getConnection_enableHighAvailability_true_all_note_memory_high_load_1() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 460; i++) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        BasicTable re = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");

        for (int i = 0; i < re.rows()-1; ++i) {
            System.out.println("port:"+ re.getColumn(0).get(i)+" connectionNum:"+re.getColumn(1).get(i));
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_conn_high_load() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 420; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", false);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, controller_port, "admin", "123456",true);
        BasicTable re = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:"+ re.getColumn(0).get(i)+" connectionNum:"+re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
//            if(Integer.valueOf(port)==PORT){
//                assertEquals(true,Integer.valueOf(connectionNum)>=460);
//            }else{
//                assertEquals(true,Integer.valueOf(connectionNum)<20);
//            }
        }
        List<DBConnection> list1 = new ArrayList<>();
        for (int i = 0; i < 460; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn);
        }
        connection1.run("sleep(3000)");
        BasicTable re1 = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re1.rows(); i++) {
            System.out.println("port:"+ re1.getColumn(0).get(i)+" connectionNum:"+re1.getColumn(1).get(i));
            String port = re1.getColumn(0).get(i).toString();
            String connectionNum = re1.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==PORT){
                System.out.println(Integer.valueOf(connectionNum));
                assertEquals(true,Integer.valueOf(connectionNum)>=460);
            }else{
                assertEquals(true,Integer.valueOf(connectionNum)>100);
                assertEquals(true,Integer.valueOf(connectionNum)<200);
            }
        }
    }

    @Test
    public void Test_getConnection_enableHighAvailability_true_all_note_conn_high_load_1() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, controller_port, "admin", "123456",false);
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC port from rpc(getControllerAlias(),getClusterPerf) where mode=0");
        for(int i = 0; i < re.rows(); i++) {
            for (int j = 0; j < 420; j++) {
                DBConnection conn = new DBConnection();
                conn.connect(HOST, re.getInt(i), "admin", "123456", "", false);
                list.add(conn);
            }
        }
        BasicTable re1 = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re1.rows(); i++) {
            System.out.println("port:"+ re1.getColumn(0).get(i)+" connectionNum:"+re1.getColumn(1).get(i));
            String port = re1.getColumn(0).get(i).toString();
            String connectionNum = re1.getColumn(1).get(i).toString();
           // assertEquals(true,Integer.valueOf(connectionNum)>=420);
        }
        List<DBConnection> list1 = new ArrayList<>();
        for (int i = 0; i < 120; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn);
        }
        connection1.run("sleep(3000)");
        BasicTable re2 = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re2.rows(); i++) {
            System.out.println("port:"+ re2.getColumn(0).get(i)+" connectionNum:"+re2.getColumn(1).get(i));
            String port = re2.getColumn(0).get(i).toString();
            String connectionNum = re2.getColumn(1).get(i).toString();
            assertEquals(true,Integer.valueOf(connectionNum)>=435);
        }
    }
    @Test(timeout = 120000)
    public void Test_getConnection_enableHighAvailability_false_1() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", false);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, controller_port, "admin", "123456",true);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:"+ re.getColumn(0).get(i)+" connectionNum:"+re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==PORT){
                assertEquals(true,Integer.valueOf(connectionNum)>100);
            }else{
                assertEquals(true,Integer.valueOf(connectionNum)<20);
            }
        }
    }
    @Test(timeout = 120000)
    public void Test_getConnection_enableHighAvailability_true_site_null_all_note_low_load() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:"+ re.getColumn(0).get(i)+" connectionNum:"+re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true,Integer.valueOf(connectionNum)>=20);
            assertEquals(true,Integer.valueOf(connectionNum)<50);
        }
    }


    @Test(timeout = 120000)
    public void Test_getConnection_enableHighAvailability_true_site_not_null_all_note_low_load() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true, ipports);
            list.add(conn);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456", true);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true, Integer.valueOf(connectionNum) > 15);
            assertEquals(true, Integer.valueOf(connectionNum) < 50);
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_false_enableLoadBalance_false() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,false,null,false,false);
            list.add(connection);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC connectionNum from rpc(getControllerAlias(),getClusterPerf) where port="+PORT);
        System.out.println(re.getInt(0));
        assertEquals(true,re.getInt(0)>100);
    }
    @Test
    public void Test_getConnection_enableHighAvailability_false_enableLoadBalance_null() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,false,null,false);
            list.add(connection);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC connectionNum from rpc(getControllerAlias(),getClusterPerf) where port="+PORT);
        System.out.println(re.getInt(0));
        assertEquals(true,re.getInt(0)>100);
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_false() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,null,false,false);
            list.add(connection);
           // BasicInt re = (BasicInt)connection.run("getNodePort()");
           // System.out.println("current node is："+re);
           // System.out.println("stop current node");
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC connectionNum from rpc(getControllerAlias(),getClusterPerf) where port="+PORT);
        System.out.println(re.getInt(0));
        assertEquals(true,re.getInt(0)>100);
    }

    @Test//The current node is unavailable
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_false_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,ipports,false,false);
            list.add(connection);
        }
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000);");
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        int port1 = port_list[1];
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==port1) {
                assertEquals(true, Integer.valueOf(connectionNum) >= 100);
            }
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,null,false,true);
            list.add(connection);
        }
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true, Integer.valueOf(connectionNum) >= 20);
            assertEquals(true, Integer.valueOf(connectionNum) < 50);
        }
    }
    @Test//The current node is unavailable
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_true_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,ipports,false,true);
            list.add(connection);
        }
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)!=PORT) {
                assertEquals(true, Integer.valueOf(connectionNum) > 25);
                assertEquals(true, Integer.valueOf(connectionNum) < 50);
            }
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_false_enableLoadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnection connection = new DBConnection();
        String re = null;
        try{
            connection.connect(HOST, PORT, "admin", "123456",null,false,null,false,true);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Cannot only enable loadbalance but not enable highAvailablity.",re);
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_false_site_not_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        DBConnection connection = new DBConnection();
        String[] ipportArray = new String[1];
        ipportArray[0] = ipports[2];
        connection.connect(HOST, PORT, "admin", "123456",null,true,ipportArray,false,false);
        BasicInt node1 = (BasicInt)connection.run("getNodePort()");
        System.out.println(node1.getString());
        Assert.assertEquals(ipports[2].split(":")[1],node1.getString());
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("2000");
        controller_conn.run("try{stopDataNode('"+HOST+":"+node1.getInt()+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        BasicInt node2 = (BasicInt)connection.run("getNodePort()");
        System.out.println(node2.getString());
        Assert.assertEquals(PORT,node2.getInt());
        controller_conn.run("try{startDataNode('"+HOST+":"+node1.getInt()+"')}catch(ex){}");
        controller_conn.run("2000");
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_false_loadBalance_false() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,false,false);
        Thread.sleep(1000);
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC connectionNum from rpc(getControllerAlias(),getClusterPerf) where port="+PORT);
        System.out.println(re.getInt(0));
        assertEquals(true,re.getInt(0)>=100);
        connection1.close();
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,false,true);
        Thread.sleep(1000);
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicIntVector re = (BasicIntVector)connection1.run("EXEC connectionNum from rpc(getControllerAlias(),getClusterPerf) where port="+PORT);
        System.out.println(re.getInt(0));
        assertEquals(true,re.getInt(0)>100);
        connection1.close();
        pool1.shutdown();
    }

    @Test//The current node is unavailable
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,false,true,ipports,null, false, false, false);

        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000);");
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        int port1 = port_list[1];
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==port1) {
                assertEquals(true, Integer.valueOf(connectionNum) >= 100);
            }
        }
    }
    //@Test//The current node is unavailable
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false_2() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        class MyThread extends Thread {
            @Override
            public void run() {
                try {
                    DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,false,true,null,null, false, false, false);
                    Thread.sleep(1000);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        class MyThread1 extends Thread {
            @Override
            public void run() {
                    try {
                        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
                        Thread.sleep(6000);
                    } catch (Exception e) {
                        // 捕获异常并打印错误信息
                        System.err.println("Error executing task: " + e.getMessage());
                    }
            }
        }
        MyThread thread = new MyThread();
        MyThread1 thread1 = new MyThread1();
        thread.start();
        Thread.sleep(20);
        System.err.println("thread1开始运行 ");
        thread1.start();
        thread.join();
        thread1.join();
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true_highAvailabilitySites_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,true,true,null,null, false, false, false);
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true, Integer.valueOf(connectionNum) >= 20);
            assertEquals(true, Integer.valueOf(connectionNum) < 50);
        }
        pool1.shutdown();
    }

    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,true,true,ipports,null, false, false, false);
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",true);
        connection1.run("sleep(2000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true, Integer.valueOf(connectionNum) >= 20);
            assertEquals(true, Integer.valueOf(connectionNum) < 50);
        }
        pool1.shutdown();
    }
    @Test//The current node is unavailable
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,true,true,ipports,null, false, false, false);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(1000)");
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicTable node1 = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port ="+ipports[1].split(":")[1]);
        System.out.println(node1.getString());
        Assert.assertEquals(true, Integer.valueOf(node1.getColumn(1).get(0).toString())>=50);

        BasicTable node2 = (BasicTable)connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port ="+ipports[2].split(":")[1]);
        System.out.println(node2.getString());
        Assert.assertEquals(true, Integer.valueOf(node2.getColumn(1).get(0).toString())>=25);
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_false_loadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,true,false,null,null, false, false, false);
        DBConnection connection1 = new DBConnection();
        connection1.connect(HOST, PORT, "admin", "123456",false);
        connection1.run("sleep(3000)");
        BasicTable re = (BasicTable) connection1.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode in [0,4];");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)!=PORT) {
                assertEquals(true, Integer.valueOf(connectionNum) > 20);
                assertEquals(true, Integer.valueOf(connectionNum) < 50);
            }
        }
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false_site_not_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(8000)");
        String[] ipportArray = new String[1];
        ipportArray[0] = ipports[2];
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",100,false,true,ipportArray,null, false, false, false);
        controller_conn.run("20000");
        BasicTable node1 = (BasicTable)controller_conn.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port ="+ipportArray[0].split(":")[1]);
        System.out.println(node1.getString());
        Assert.assertEquals(true, Integer.valueOf(node1.getColumn(1).get(0).toString())>=100);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("2000");
        controller_conn.run("try{stopDataNode('"+HOST+":"+ipportArray[0].split(":")[1]+"')}catch(ex){}");
        controller_conn.run("8000");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            BasicDBTask task = new BasicDBTask("getNodePort();");
            tasks.add(task);
        }
        pool1.execute(tasks);
        pool1.waitForThreadCompletion();
        BasicTable node2 = (BasicTable)controller_conn.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port ="+PORT);
        System.out.println(node2.getString());
        Assert.assertEquals(true, Integer.valueOf(node2.getColumn(1).get(0).toString())>=100);
        controller_conn.run("try{startDataNode('"+HOST+":"+ipportArray[0].split(":")[1]+"')}catch(ex){}");
        controller_conn.run("2000");
        pool1.shutdown();
    }
}
