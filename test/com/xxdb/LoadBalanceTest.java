package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
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
    }

    @Test
    public void test_load_balance_create_connect_highAvailabilitySites_error() throws IOException {
        conn = new DBConnection();
        String[] tmp_ipports = new String[]{"192.168.1.32:23"};
        conn.connect(host_list[0],port_list[0],"admin","123456","",true,tmp_ipports);
        assertEquals(host_list[0],conn.getHostName());
        assertEquals(port_list[0],conn.getPort());
    }

    @Test
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

    @Test
    public void test_load_balance_ha_stream() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host,controller_port,"admin","123456");
        conn = new DBConnection();
        ThreadedClient client = new ThreadedClient("127.0.0.1", 8676);
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

    @Test
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

}