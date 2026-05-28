package com.xxdb.restart;

import com.xxdb.DBConnection;
import com.xxdb.SimpleDBConnectionPool;
import com.xxdb.SimpleDBConnectionPoolConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ResourceBundle;

import static com.xxdb.Prepare.getDataNodeConnectionNums;
import static org.junit.Assert.assertEquals;

public class SimpleDBConnectionPoolTest {
    private static SimpleDBConnectionPoolConfig config;
    private static SimpleDBConnectionPool pool;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String controller_host = bundle.getString("CONTROLLER_HOST");
    static int controller_port = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");
    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();

    @Before
    public void setUp() throws IOException {
        config = new SimpleDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
    }

    @After
    public void tearDown() throws Exception {
        try{
            pool.close();
        }catch(Exception e){
        }
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        for(int i = 0; i<ipports.length; i++){
            String site = ipports[i];
            controller_conn.run("try{startDataNode('" + site + "')}catch(ex){}");
            System.out.println(site);
        }
    }
    @Test
    public void test_SimpleDBConnectionPool_config_HighAvailability_true_LoadBalance_false() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        Thread.sleep(8000);
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setEnableHighAvailability(true);
        config1.setInitialPoolSize(20);
        config1.setHighAvailabilitySites(ipports);
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        pool = new SimpleDBConnectionPool(config1);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        Thread.sleep(3000);
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        assertEquals(20,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isLoadBalance());
        int port1 = port_list[1];
        int beforeNum = before.getOrDefault(port1, 0);
        int afterNum = after.getOrDefault(port1, 0);
        int delta = afterNum - beforeNum;
        Assert.assertTrue("delta per data node should be >=15, port=" + port1 + ", delta=" + delta, delta >= 20);
        pool.close();
        controller_conn.close();
    }

    @Test//The current node is unavailable
    public void test_SimpleDBConnectionPool_config_HighAvailability_true_LoadBalance_true() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        Thread.sleep(8000);
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setEnableHighAvailability(true);
        config1.setLoadBalance(true);
        config1.setInitialPoolSize(30);
        config1.setHighAvailabilitySites(ipports);
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        pool = new SimpleDBConnectionPool(config1);
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        Thread.sleep(3000);
        assertEquals(30,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isLoadBalance());
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            if(Integer.valueOf(port) != PORT) {
                System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
                Assert.assertTrue("delta per data node should be >=5, port=" + port + ", delta=" + delta, delta >= 5);
                Assert.assertTrue("delta per data node should be <20, port=" + port + ", delta=" + delta, delta < 20);
            }
        }
        pool.close();
        controller_conn.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_config_HighAvailability_true_LoadBalance_true_1() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setEnableHighAvailability(true);
        config1.setLoadBalance(true);
        config1.setMinimumPoolSize(50);
        config1.setMaximumPoolSize(200);
        config1.setHighAvailabilitySites(ipports);
        java.util.Map<Integer,Integer> before = getDataNodeConnectionNums(controller_conn);
        pool = new SimpleDBConnectionPool(config1);
        Thread.sleep(2000);
        java.util.Map<Integer,Integer> after = getDataNodeConnectionNums(controller_conn);
        assertEquals(50,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isLoadBalance());
        for (java.util.Map.Entry<Integer, Integer> en : after.entrySet()) {
            int port = en.getKey();
            int beforeNum = before.getOrDefault(port, 0);
            int afterNum = en.getValue();
            int delta = afterNum - beforeNum;
            System.out.println("port:" + port + " delta:" + delta + " before:" + beforeNum + " after:" + afterNum);
            Assert.assertTrue("delta per data node should be >=10, port=" + port + ", delta=" + delta, delta >= 10);
            Assert.assertTrue("delta per data node should be <25, port=" + port + ", delta=" + delta, delta < 25);
        }
        pool.close();
        controller_conn.close();
    }
}
