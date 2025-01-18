package com.xxdb.restart;

import com.xxdb.DBConnection;
import com.xxdb.SimpleDBConnectionPool;
import com.xxdb.SimpleDBConnectionPoolConfig;
import com.xxdb.data.BasicTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ResourceBundle;

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
        Thread.sleep(10000);
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setEnableHighAvailability(true);
        config1.setInitialPoolSize(100);
        config1.setHighAvailabilitySites(ipports);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(100,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        int port1 = port_list[1];
        //poolEntity.run("sleep(2000)");
        BasicTable re = (BasicTable) poolEntity.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port) == port1) {
                assertEquals(true, Integer.valueOf(connectionNum) >= 100);
            }
        }
        controller_conn.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_config_HighAvailability_true_LoadBalance_true() throws IOException, InterruptedException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(controller_host, controller_port, "admin", "123456");
        controller_conn.run("try{stopDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        controller_conn.run("8000");
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setEnableHighAvailability(true);
        config1.setLoadBalance(true);
        config1.setInitialPoolSize(100);
        config1.setHighAvailabilitySites(ipports);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(100,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
        controller_conn.run("try{startDataNode('"+HOST+":"+PORT+"')}catch(ex){}");
        int port1 = port_list[1];
        poolEntity.run("sleep(8000)");
        BasicTable re = (BasicTable) poolEntity.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port) != PORT) {
                assertEquals(true, Integer.valueOf(connectionNum) > 25);
                assertEquals(true, Integer.valueOf(connectionNum) <= 60);
            }
        }
        controller_conn.close();
    }
}
