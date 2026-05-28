package com.xxdb.restart;

import com.xxdb.*;
import com.xxdb.data.*;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.xxdb.Prepare.getDataNodeConnectionNums;
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
//            Thread.sleep(1000);
            conn = new DBConnection();
            conn.connect(controller_host,controller_port,"admin","123456","",true,ipports);
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
        List<Integer> list1 = new ArrayList<>();
        for (int i = 0; i < 60; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn.getPort());
            conn.close();
        }
        Map<Integer, Long> counts = list1.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < ipports.length; i++) {
            int port = Integer.parseInt(ipports[i].split(":")[1]);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            if (port == PORT) {
                assertEquals(0,cnt);
            } else {
                Assert.assertTrue("delta per data node should be >=10 and <30, port=" + port, cnt >= 10 && cnt < 30);
            }
        }
    }

    //@Test(timeout = 60000)
    public void Test_getConnection_enableHighAvailability_true_all_note_memory_high_load_1() throws SQLException, ClassNotFoundException, IOException {
        List<Integer> list1 = new ArrayList<>();
        for (int i = 0; i < 60; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn.getPort());
            conn.close();
        }
        Map<Integer, Long> counts = list1.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < ipports.length; i++) {
            int port = Integer.parseInt(ipports[i].split(":")[1]);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            Assert.assertTrue("delta per data node should be >=8 and <25, port=" + port, cnt >= 8 && cnt < 25);
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_conn_high_load() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456",true);
        List<DBConnection> list = new ArrayList<>();
        for (int i = 0; i < 460; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", false);
            list.add(conn);
        }
        controller.run("sleep(1000)");
        List<Integer> list1 = new ArrayList<>();
        for (int i = 0; i < 60; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn.getPort());
            conn.close();
        }
        Map<Integer, Long> counts = list1.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < ipports.length; i++) {
            int port = Integer.parseInt(ipports[i].split(":")[1]);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            if (port == PORT) {
                assertEquals(0,cnt);
            } else {
                Assert.assertTrue("delta per data node should be >=10 and <30, port=" + port, cnt >= 10 && cnt < 30);
            }
        }
        for (DBConnection c : list) {
            try { c.close(); } catch (Exception ignored) {}
        }
        try { controller.close(); } catch (Exception ignored) {}
    }

    @Test
    public void Test_getConnection_enableHighAvailability_true_all_note_conn_high_load_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456",false);
        BasicIntVector re = (BasicIntVector)controller.run("EXEC port from rpc(getControllerAlias(),getClusterPerf) where mode=0");
        List<DBConnection> list = new ArrayList<>();
        for(int i = 0; i < re.rows(); i++) {
            for (int j = 0; j < 460; j++) {
                DBConnection conn = new DBConnection();
                conn.connect(HOST, re.getInt(i), "admin", "123456", "", false);
                list.add(conn);
            }
        }
        controller.run("sleep(2000)");

        List<Integer> list1 = new ArrayList<>();
        for (int i = 0; i < 60; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true,ipports);
            list1.add(conn.getPort());
            conn.close();
        }
        Map<Integer, Long> counts = list1.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < re.rows(); i++) {
            int port = re.getInt(i);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            Assert.assertTrue("delta per data node should be >=8 and <25, port=" + port, cnt >= 8 && cnt < 25);
        }
        for (DBConnection c : list) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    @Test(timeout = 12000)
    public void Test_getConnection_enableHighAvailability_false_1() throws IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456", true);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", false);
            list.add(conn.getPort());
            conn.close();
        }
        Assert.assertEquals(20, list.size());
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long cnt = counts.getOrDefault(PORT, 0L);
        Assert.assertEquals(20, cnt);
    }

    @Test(timeout = 120000)
    public void Test_getConnection_enableHighAvailability_true_site_null_all_note_low_load() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, PORT, "admin", "123456", true);
        List<Integer> list = new ArrayList<>();
        final int created = 50;
        for (int i = 0; i < created; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true);
            list.add(conn.getPort());
            conn.close();
        }
        BasicIntVector re = (BasicIntVector) controller.run("EXEC port from rpc(getControllerAlias(),getClusterPerf) where mode=0");
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < re.rows(); i++) {
            int port = re.getInt(i);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            Assert.assertTrue("delta per data node should be >=5 and <25, port=" + port, cnt >= 5 && cnt < 25);
        }
    }

    @Test(timeout = 120000)
    public void Test_getConnection_enableHighAvailability_true_site_not_null_all_note_low_load() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456", false);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456", "", true, ipports);
            list.add(conn.getPort());
            conn.close();
        }
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < ipports.length; i++) {
            int port = Integer.parseInt(ipports[i].split(":")[1]);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            Assert.assertTrue("delta per data node should be >=5 and <25, port=" + port, cnt >= 5 && cnt < 25);
        }
    }
    @Test
    public void Test_getConnection_enableHighAvailability_false_enableLoadBalance_false() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456", false);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,false,null,false,false);
            list.add(connection.getPort());
            connection.close();
        }
        Assert.assertEquals(10, list.size());
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long cnt = counts.getOrDefault(PORT, 0L);
        Assert.assertEquals(10, cnt);
    }
    @Test
    public void Test_getConnection_enableHighAvailability_false_enableLoadBalance_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456", false);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,false,null,false);
            list.add(connection.getPort());
            connection.close();
        }
        Assert.assertEquals(10, list.size());
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long cnt = counts.getOrDefault(PORT, 0L);
        Assert.assertEquals(10, cnt);
    }
    @Test
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_false() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456", false);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,null,false,false);
            list.add(connection.getPort());
            connection.close();
        }
        Assert.assertEquals(10, list.size());
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long cnt = counts.getOrDefault(PORT, 0L);
        Assert.assertEquals(10, cnt);
    }

    @Test//The current node is unavailable
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_false_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,ipports,false,false);
            list.add(connection.getPort());
            connection.close();
        }
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000);");
        int port1 = port_list[1];
        Assert.assertEquals(5, list.size());
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        long cnt = counts.getOrDefault(port1, 0L);
        Assert.assertEquals(5, cnt);
    }

    @Test
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, PORT, "admin", "123456", true);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,null,false,true);
            list.add(connection.getPort());
            connection.close();
        }
        Assert.assertEquals(60, list.size());
        BasicIntVector re = (BasicIntVector) controller.run("EXEC port from rpc(getControllerAlias(),getClusterPerf) where mode=0");
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < re.rows(); i++) {
            int port = re.getInt(i);
            long cnt = counts.getOrDefault(port, 0L);
                System.out.println(port+":"+cnt);
            Assert.assertTrue("delta per data node should be >=8 and <25, port=" + port, cnt >= 8 && cnt < 25);
        }
    }

    @Test//The current node is unavailable
    public void Test_getConnection_enableHighAvailability_true_enableLoadBalance_true_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            DBConnection connection = new DBConnection();
            connection.connect(HOST, PORT, "admin", "123456",null,true,ipports,false,true);
            list.add(connection.getPort());
            connection.close();
        }
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (int i = 0; i < ipports.length; i++) {
            int port = Integer.parseInt(ipports[i].split(":")[1]);
            long cnt = counts.getOrDefault(port, 0L);
            System.out.println(port+":"+cnt);
            if (port == PORT) {
                assertEquals(0,cnt);
            } else {
                Assert.assertTrue("delta per data node should be >=3 and <15, port=" + port, cnt >= 5 && cnt < 15);
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
        controller_conn.run("sleep(6000)");
        DBConnection connection = new DBConnection();
        String[] ipportArray = new String[1];
        ipportArray[0] = ipports[2];
        connection.connect(HOST, PORT, "admin", "123456",null,true,ipportArray,false,false);
        BasicInt node1 = (BasicInt)connection.run("getNodePort()");
        System.out.println(node1.getString());
        Assert.assertEquals(ipports[2].split(":")[1],node1.getString());
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");
        controller_conn.run("try{stopDataNode('"+HOST+":"+node1.getInt()+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        BasicInt node2 = (BasicInt)connection.run("getNodePort()");
        System.out.println(node2.getString());
        Assert.assertEquals(PORT,node2.getInt());
        controller_conn.run("try{startDataNode('"+HOST+":"+node1.getInt()+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_false_loadBalance_false() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",20,false,false);
        controller_conn.run("sleep(2000)");
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        int beforeNum = before.getOrDefault(PORT, 0);
        int afterNum = after.getOrDefault(PORT, 0);
        int delta = afterNum - beforeNum;
        System.out.println("beforeNum  afterNum  delta:"+beforeNum+"  "+afterNum+"  "+delta);
        assertEquals(true, delta >= 20);
        controller_conn.close();
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",20,false,true);
        controller_conn.run("sleep(2000)");
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        int beforeNum = before.getOrDefault(PORT, 0);
        int afterNum = after.getOrDefault(PORT, 0);
        int delta = afterNum - beforeNum;
        assertEquals(true, delta >= 20);
        controller_conn.close();
        pool1.shutdown();
    }

    @Test//The current node is unavailable
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",20,false,true,ipports,null, false, false, false);

        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000);");
        int port1 = port_list[1];
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        int beforeNum = before.getOrDefault(port1, 0);
        int afterNum = after.getOrDefault(port1, 0);
        int delta = afterNum - beforeNum;
        Assert.assertTrue("delta per data node should be >=20, port=" + port1 + ", delta=" + delta, delta >= 20);
        Assert.assertTrue("delta per data node should be <25, port=" + port1 + ", delta=" + delta, delta < 25);
        pool1.shutdown();
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
        controller_conn.close();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true_highAvailabilitySites_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456",true);
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",50,true,true,null,null, false, false, false);
        controller.run("sleep(2000)");
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller);
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
            Assert.assertTrue("delta per data node should be >=7, port=" + port + ", delta=" + delta, delta >= 7);
            Assert.assertTrue("delta per data node should be <30, port=" + port + ", delta=" + delta, delta < 30);
        }
        pool1.shutdown();
    }

    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller = new DBConnection();
        controller.connect(HOST, controller_port, "admin", "123456",true);
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",50,true,true,ipports,null, false, false, false);
        controller.run("sleep(2000)");
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller);
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
            Assert.assertTrue("delta per data node should be >=7, port=" + port + ", delta=" + delta, delta >= 7);
            Assert.assertTrue("delta per data node should be <30, port=" + port + ", delta=" + delta, delta < 30);
        }
        pool1.shutdown();
    }
    @Test//The current node is unavailable ：node1全部会切换到node2
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_true_1() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",50,true,true,ipports,null, false, false, false);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");

        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
            if(port!=PORT){
                Assert.assertTrue("delta per data node should be >=10, port=" + port + ", delta=" + delta, delta >= 10);
                Assert.assertTrue("delta per data node should be <30, port=" + port + ", delta=" + delta, delta < 35);
            }
        }
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_false_loadBalance_true() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",50,true,false,null,null, false, false, false);
        controller_conn.run("sleep(2000)");
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
            Assert.assertTrue("delta per data node should be >=10, port=" + port + ", delta=" + delta, delta >= 10);
            Assert.assertTrue("delta per data node should be <20, port=" + port + ", delta=" + delta, delta < 20);
        }
        pool1.shutdown();
    }
    @Test
    public void Test_DBConnectionPool_enableHighAvailability_true_loadBalance_false_site_not_null() throws SQLException, ClassNotFoundException, IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        String[] ipportArray = new String[1];
        ipportArray[0] = ipports[2];
        DBConnectionPool pool1 = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",20,false,true,ipportArray,null, false, false, false);
        controller_conn.run("sleep(2000)");
        java.util.Map<Integer,Integer> nodeMap = getDataNodeConnectionNums(controller_conn);
        int nodePort = Integer.parseInt(ipportArray[0].split(":")[1]);
        System.out.println("nodePort: " + nodePort + " -> " + nodeMap.getOrDefault(nodePort,0));
        Assert.assertEquals(true, nodeMap.getOrDefault(nodePort,0) >= 20);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");
        controller_conn.run("try{stopDataNode('"+HOST+":"+ipportArray[0].split(":")[1]+"')}catch(ex){}");
        controller_conn.run("sleep(6000)");
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++){
            BasicDBTask task = new BasicDBTask("getNodePort();");
            tasks.add(task);
        }
        pool1.execute(tasks);
        pool1.waitForThreadCompletion();
        java.util.Map<Integer,Integer> nodeMap2 = getDataNodeConnectionNums(controller_conn);
        System.out.println("port " + PORT + " -> " + nodeMap2.getOrDefault(PORT,0));
        Assert.assertEquals(true, nodeMap2.getOrDefault(PORT,0) >= 20);
        controller_conn.run("try{startDataNode('"+HOST+":"+ipportArray[0].split(":")[1]+"')}catch(ex){}");
        controller_conn.run("sleep(3000)");
        pool1.shutdown();
    }
}
