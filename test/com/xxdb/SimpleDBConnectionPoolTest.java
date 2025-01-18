package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
//        DBConnection controller_conn = new DBConnection();
//        controller_conn.connect(controller_host, controller_port, "admin", "123456");
//        for(int i = 0; i<ipports.length; i++){
//            String site = ipports[i];
//            controller_conn.run("try{startDataNode('" + site + "')}catch(ex){}");
//            System.out.println(site);
//        }
    }
    @Test
    public void test_SimpleDBConnectionPool_config_setInitialPoolSize(){
        config.setInitialPoolSize(10);
        SimpleDBConnectionPool pool=new SimpleDBConnectionPool(config);
        assertEquals(10,config.getMinimumPoolSize());
        assertEquals(10,config.getMaximumPoolSize());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_MinimumandPoolSize_and_MaximumPoolSize_null() {
        pool = new SimpleDBConnectionPool(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_maxpoolsize_minpoolsize_realtion() {
        config.setMaximumPoolSize(6);
        config.setMinimumPoolSize(-1);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(6, config.getMaximumPoolSize());

        config.setMaximumPoolSize(-4);
        config.setMinimumPoolSize(-6);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_maxpoolsize_minpoolsize_realtion_error() {
        config.setMaximumPoolSize(4);
        config.setMinimumPoolSize(6);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(6, config.getMaximumPoolSize());
        assertEquals(6, config.getMaximumPoolSize());

        config.setMaximumPoolSize(-5);
        config.setMinimumPoolSize(6);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(6, config.getMaximumPoolSize());
        assertEquals(6, config.getMaximumPoolSize());

        config.setMaximumPoolSize(-4);
        config.setMinimumPoolSize(-2);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(5, config.getMaximumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());

    }
    @Test
    public void test_SimpleDBConnectionPool_config_InvalididleTimeout_error() {
        config.setIdleTimeout(-50);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(600000, config.getIdleTimeout());
        config.setIdleTimeout(0);
        pool = new SimpleDBConnectionPool(config);
        assertEquals(600000, config.getIdleTimeout());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_InvalididleTimeout_null() {
        pool = new SimpleDBConnectionPool(config);
        assertEquals(600000, config.getIdleTimeout());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_hostName_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName("1sss");
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        config1.setEnableHighAvailability(false);
        String re = null;
        try{
            pool = new SimpleDBConnectionPool(config1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Invalid hostName: 1sss",re);
        config1.setHostName("111.111.111.111.111");
        String re1 = null;
        try{
            pool = new SimpleDBConnectionPool(config1);
        }catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals("Invalid hostName: 111.111.111.111.111",re1);
    }

    //@Test //default localhost
    public void test_SimpleDBConnectionPool_config_hostName_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setPort(8848);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        config1.setEnableHighAvailability(false);
        pool = new SimpleDBConnectionPool(config1);
    }
    @Test
    public void test_SimpleDBConnectionPool_config_port_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        String re = null;
        try{
            config1.setPort(-111);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The port should be positive.",re);
        String re1 = null;
        try{
            config1.setPort(0);
        }catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals("The port should be positive.",re1);

    }
    //@Test //default is 8848
    public void test_SimpleDBConnectionPool_config_port_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName("127.0.0.1");
        //config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(8848,config1.getPort());
    }
    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_config_userId_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin_error");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
    }
    @Test
    public void test_SimpleDBConnectionPool_config_userId_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        //config1.setUserId("admin");
        //config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity = pool.getConnection();
        String re = null;
        try{
            poolEntity.run("getGroupList();");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("Login is required for script execution with client authentication enabled")||re.contains("Only administrators execute function getGroupList"));
    }
    @Test
    public void test_SimpleDBConnectionPool_config_userId_not_admin() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};"+
                "rpc(getControllerAlias(),create_user);");
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("test1");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
        conn.close();
    }
    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_config_password_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456_error");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
    }
    @Test
    public void test_SimpleDBConnectionPool_config_password_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(controller_host);
        config1.setPort(controller_port);
        config1.setUserId("test");
        //config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity = pool.getConnection();
        String re = null;
        try{
            poolEntity.run("getGroupList();");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals(true,re.contains("getGroupList() => Only administrators execute function getGroupList"));
    }

    @Test
    public void test_SimpleDBConnectionPool_config_InitialPoolSize_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        String re = null;
        try{
            config1.setInitialPoolSize(-5);

        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The number of connection pools should be positive.",re);
        String re1 = null;
        try{
            config1.setInitialPoolSize(0);

        }catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals("The number of connection pools should be positive.",re1);

    }
    @Test
    public void test_SimpleDBConnectionPool_config_InitialPoolSize_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_InitialPoolSize_1() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(1);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity = pool.getConnection();
        assertEquals(1,pool.getTotalConnectionsCount());
        long startTime = System.currentTimeMillis();
        poolEntity.run("sleep(1000);");
        long completeTime1 = System.currentTimeMillis();
        long tcompleteTime = completeTime1 - startTime;
        assertEquals(true,tcompleteTime>=1000);
    }

    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_config_InitialScript_error() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialScript("pt.append!(t);");
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
    }
    @Test
    public void test_SimpleDBConnectionPool_config_InitialScript_null() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialScript("");
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals("",config1.getInitialScript());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_compless_true() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setCompress(true);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isCompress());
    }
    @Test
    public void test_SimpleDBConnectionPool_config_compless_false() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setCompress(false);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isCompress());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_UseSSL_true() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setUseSSL(true);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isUseSSL());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_UseSSL_false() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setUseSSL(false);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isUseSSL());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_UsePython_true() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setUsePython(true);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isUsePython());
    }

    @Test
    public void test_SimpleDBConnectionPool_config_UsePython_false() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setUsePython(false);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isUsePython());
    }

    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_config_LoadBalance_true_highAvailablity_false() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setLoadBalance(true);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(true,config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
    }
    @Test
    public void test_SimpleDBConnectionPool_config_LoadBalance_true_highAvailablity_true() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(100);
        config1.setLoadBalance(true);
        config1.setEnableHighAvailability(true);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(100, pool.getTotalConnectionsCount());
        assertEquals(true, config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
        poolEntity.run("sleep(2000)");
        BasicTable re = (BasicTable) poolEntity.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            assertEquals(true, Integer.valueOf(connectionNum) > 20);
            assertEquals(true, Integer.valueOf(connectionNum) < 50);
        }
    }

    @Test
    public void test_SimpleDBConnectionPool_config_LoadBalance_false_highAvailablity_false() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setLoadBalance(false);
        config1.setEnableHighAvailability(false);
        config1.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(100,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
        poolEntity.run("sleep(2000)");
        BasicTable re = (BasicTable) poolEntity.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==PORT) {
                assertEquals(true, Integer.valueOf(connectionNum) >= 100);
            }
        }
    }
    @Test
    public void test_SimpleDBConnectionPool_config_LoadBalance_false_highAvailablity_true() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setLoadBalance(false);
        config1.setEnableHighAvailability(true);
        config1.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config1);
        assertEquals(100,pool.getTotalConnectionsCount());
        assertEquals(false,config1.isLoadBalance());
        DBConnection poolEntity = pool.getConnection();
        poolEntity.run("sleep(1000)");
        BasicTable re = (BasicTable) poolEntity.run("select port ,connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0");
        for (int i = 0; i < re.rows(); i++) {
            System.out.println("port:" + re.getColumn(0).get(i) + " connectionNum:" + re.getColumn(1).get(i));
            String port = re.getColumn(0).get(i).toString();
            String connectionNum = re.getColumn(1).get(i).toString();
            if(Integer.valueOf(port)==PORT) {
                assertEquals(true, Integer.valueOf(connectionNum) >= 100);
            }
        }
    }

    @Test
    public void test_SimpleDBConnectionPool_config_set() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        config1.setInitialScript("1");
        config1.setCompress(true);
        config1.setUseSSL(true);
        config1.setUsePython(true);
        config1.setLoadBalance(false);
        config1.setEnableHighAvailability(false);
        config1.setHighAvailabilitySites(null);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(HOST,config1.getHostName());
        assertEquals(PORT,config1.getPort());
        assertEquals("admin",config1.getUserId());
        assertEquals("123456",config1.getPassword());
        assertEquals(5,config1.getInitialPoolSize());
        assertEquals("1",config1.getInitialScript());
        assertEquals(true,config1.isUseSSL());
        assertEquals(true,config1.isUsePython());
        assertEquals(true,config1.isCompress());
        assertEquals(false,config1.isLoadBalance());
        assertEquals(false,config1.isEnableHighAvailability());
        assertEquals(null,config1.getHighAvailabilitySites());
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_config_default() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        assertEquals(HOST,config1.getHostName());
        assertEquals(PORT,config1.getPort());
        assertEquals(null,config1.getUserId());
        assertEquals(null,config1.getPassword());
        assertEquals(-1,config1.getInitialPoolSize());
        assertEquals(null,config1.getInitialScript());
        assertEquals(false,config1.isUseSSL());
        assertEquals(false,config1.isUsePython());
        assertEquals(false,config1.isCompress());
        assertEquals(false,config1.isLoadBalance());
        assertEquals(false,config1.isEnableHighAvailability());
        assertEquals(null,config1.getHighAvailabilitySites());
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntry = pool.getConnection();
        assertEquals(5,pool.getTotalConnectionsCount());
        poolEntry.run("version",new ArrayList<>());
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection_new(){
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        pool=new SimpleDBConnectionPool(config);
        DBConnection conn = pool.getConnection();
        assertEquals(1, pool.getActiveConnectionsCount());
        assertEquals(5, pool.getIdleConnectionsCount());
        assertEquals(6, pool.getTotalConnectionsCount());
        for (int i = 1; i < 10; i++) {
            pool.getConnection();
        }
        assertEquals(10, pool.getActiveConnectionsCount());
        assertEquals(0, pool.getIdleConnectionsCount());
        assertEquals(10, pool.getTotalConnectionsCount());
    }
    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_getConnection_new_WhenMaxrReadched() {
        for (int i = 0; i < 5; i++) {
            pool.getConnection();
        }
        pool.getConnection();
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection_new_MultiThread() throws InterruptedException {
        int n=9;
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        pool=new SimpleDBConnectionPool(config);
        Thread[] threads = new Thread[10];
        for (int i = 0; i <10; i++) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry = pool.getConnection();
            });
        }
        for (int i = 0; i <9; i++) {
            threads[i].start();
        }
        for (int i = 0; i <9; i++) {
            threads[i].join();
        }
        assertEquals(9, pool.getActiveConnectionsCount());
        assertEquals(9, pool.getTotalConnectionsCount());
        assertEquals(0, pool.getIdleConnectionsCount());
        threads[9].start();
        threads[9].join();
        assertEquals(10, pool.getActiveConnectionsCount());
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(0, pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection_new_WhenMaxrReadched_MultiThread() throws InterruptedException {
        int n=11;
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        pool=new SimpleDBConnectionPool(config);
        Thread[] threads = new Thread[n];
        AtomicReference<Exception> exception = new AtomicReference<>();
        for (int i = 0; i < n; i++) {
            threads[i] = new Thread(() -> {
                try {
                    DBConnection poolEntry = pool.getConnection();
                } catch (Exception e) {
                    exception.set(e);
                }
            });
        }
        for (int i = 0; i <n; i++) {
            threads[i].start();
        }
        for (int i = 0; i < n; i++) {
            threads[i].join();
        }
        assertEquals("java.lang.RuntimeException: No available idle connections.",exception.get().toString());
    }
    @Test
    public void test_SimpleDBConnectionPool_close_new(){
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        pool=new SimpleDBConnectionPool(config);
        DBConnection[] a=new DBConnection[10];
        for(int i=0;i<10;i++){
            a[i]=pool.getConnection();
        }
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(10, pool.getActiveConnectionsCount());
        assertEquals(0, pool.getIdleConnectionsCount());
        a[0].close();
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(9, pool.getActiveConnectionsCount());
        assertEquals(1, pool.getIdleConnectionsCount());
        for(int i=1;i<10;i++){
            a[i].close();
        }
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(0, pool.getActiveConnectionsCount());
        assertEquals(10, pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_close_new_MultiThread() throws InterruptedException {
        int n=10;
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        pool=new SimpleDBConnectionPool(config);
        DBConnection[] a=new DBConnection[10];
        for(int i=0;i<10;i++){
            a[i]=pool.getConnection();
        }
        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                a[index].close();
            });
        }
        for (int i = 0; i <n-1; i++) {
            threads[i].start();
        }
        for (int i = 0; i < n-1; i++) {
            threads[i].join();
        }
        assertEquals(1, pool.getActiveConnectionsCount());
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(9, pool.getIdleConnectionsCount());
        threads[9].start();
        threads[9].join();
        assertEquals(0, pool.getActiveConnectionsCount());
        assertEquals(10, pool.getTotalConnectionsCount());
        assertEquals(10, pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnections_Auto() throws InterruptedException {
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        for(int i=0;i<10;i++){
            A[i].close();
        }
        Thread.sleep(9000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());
        Thread.sleep(1000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());
        Thread.sleep(972);//972
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
        Thread.sleep(1000);//972
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnections_Auto_1() throws InterruptedException {
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        for(int i=0;i<2;i++){
            A[i].close();
        }
        Thread.sleep(9000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(8,pool.getActiveConnectionsCount());
        assertEquals(2,pool.getIdleConnectionsCount());
        Thread.sleep(1000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(8,pool.getActiveConnectionsCount());
        assertEquals(2,pool.getIdleConnectionsCount());
        Thread.sleep(972);
        assertEquals(8,pool.getTotalConnectionsCount());
        assertEquals(8,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnections_Auto_2() throws InterruptedException {
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
//        for(int i=0;i<2;i++){
//            A[i].close();
//        }
        Thread.sleep(9000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        Thread.sleep(1000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        Thread.sleep(972);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnections_Auto_MultiThread() throws InterruptedException {
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        Thread[] threads = new Thread[10];
        for(int i=0;i<10;i++){
            int finalI = i;
            threads[i]=new Thread(()->{
                A[finalI].close();
                try {
                    Thread.sleep(10972);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });
        }
        for (int i=0;i<10;i++){
            threads[i].start();
        }
        for (int i=0;i<10;i++){
            threads[i].join();
        }
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
        Thread.sleep(972);//972
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnectionsManual() throws InterruptedException{
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        for(int i=0;i<10;i++){
            A[i].close();
        }
        Thread.sleep(7000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());

        pool.manualCleanupIdleConnections();
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
        Thread.sleep(1000);
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnectionsManual_1() throws InterruptedException{
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        for(int i=0;i<2;i++){
            A[i].close();
        }
        Thread.sleep(7000);
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(8,pool.getActiveConnectionsCount());
        assertEquals(2,pool.getIdleConnectionsCount());

        pool.manualCleanupIdleConnections();
        assertEquals(8,pool.getTotalConnectionsCount());
        assertEquals(8,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_closeIdleConnectionsManual_MultiThread() throws InterruptedException{
        config.setMaximumPoolSize(10);
        config.setMinimumPoolSize(6);
        config.setIdleTimeout(10000);
        pool=new SimpleDBConnectionPool(config);
        DBConnection [] A=new DBConnection[10];
        for(int i=0;i<10;i++){
            A[i]=pool.getConnection();
        }
        for(int i=0;i<10;i++){
            A[i].close();
        }
        Thread thread=new Thread(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            pool.manualCleanupIdleConnections();
        });
        thread.start();
        thread.join();
        assertEquals(6,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
    }
    @Test
    public void test_SimpleDBConnectionPool_connectCount_equal_threadCount() throws IOException, InterruptedException {
        config.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config);
        Thread [] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry = pool.getConnection();
                try {
                    poolEntry.run("table(1..10000 as id ,take(`qq`aa`ss,10000) as id1); sleep(6000)");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry.close();
            });
        }
        for (int i = 0; i < 5; i++) {
            threads[i].start();
        }
        Thread.sleep(1000);
        assertEquals(5,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        Thread.sleep(6000);
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        for (int i = 0; i < 5; i++) {
            threads[i].join();
        }
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_connectCount_less_than_threadCount() throws IOException, InterruptedException {
        config.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config);
        Thread [] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry = pool.getConnection();
                try {
                    poolEntry.run("table(1..10000 as id ,take(`qq`aa`ss,10000) as id1); sleep(5000)");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry.close();
            });
        }
        for (int i = 0; i < 10; i++) {
            threads[i].start();
        }
        Thread.sleep(1000);
        assertEquals(5,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        Thread.sleep(6000);
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getTotalConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        pool.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_connectCount_greater_than_threadCount() throws IOException, InterruptedException {
        config.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config);
        Thread [] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry = pool.getConnection();
                try {
                    poolEntry.run("table(1..10000 as id ,take(`qq`aa`ss,10000) as id1); sleep(5000)");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry.close();
            });
        }
        for (int i = 0; i < 5; i++) {
            threads[i].start();
        }
        Thread.sleep(1000);
        assertEquals(5,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        Thread.sleep(6000);
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());
        for (int i = 0; i < 5; i++) {
            threads[i].join();
        }
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());
        pool.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_getActiveConnectionsCount() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        int t = pool.getActiveConnectionsCount();
        System.out.println("count: "+ t);
        assertEquals(1,t);
        pool.close();
        String re = null;
        try{
            pool.getActiveConnectionsCount();
        }catch(Exception e){
            re = e.getMessage();

        }
        assertEquals("The connection pool has been closed.",re);
    }

    @Test
    public void test_SimpleDBConnectionPool_getIdleConnectionsCount() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        int t = pool.getIdleConnectionsCount();
        System.out.println("count: "+ t);
        assertEquals(4,t);
        pool.close();
        String re = null;
        try{
            pool.getIdleConnectionsCount();
        }catch(Exception e){
            re = e.getMessage();

        }
        assertEquals("The connection pool has been closed.",re);

    }

    @Test
    public void test_SimpleDBConnectionPool_getTotalConnectionsCount() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        int t = pool.getTotalConnectionsCount();
        System.out.println("count: "+ t);
        assertEquals(5,t);
        pool.close();
        String re = null;
        try{
            pool.getTotalConnectionsCount();
        }catch(Exception e){
            re = e.getMessage();

        }
        assertEquals("The connection pool has been closed.",re);
    }
    @Test
    public void test_SimpleDBConnectionPool_close() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(false,pool.isClosed());
        pool.close();
        assertEquals(true,pool.isClosed());
        pool.close();
        assertEquals(true,pool.isClosed());
        String re = null;
        try{
            pool.getConnection();
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The connection pool has been closed.",re);
    }

    @Test
    public void test_SimpleDBConnectionPool_close_isBusy() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(5);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntry = pool.getConnection();
        Thread [] threads = new Thread[2];
        threads[0] = new Thread(() -> {
                try {
                    poolEntry.run("sleep(5000);");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        threads[1] = new Thread(() -> {
            try {
                poolEntry.run("sleep(1000);");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            pool.close();
        });
        for (int i = 0; i < 2; i++) {
            threads[i].start();
        }
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config1);
        pool.getConnection();
        assertEquals(1,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(9,pool.getIdleConnectionsCount());
        pool.getConnection();
        pool.getConnection();
        DBConnection poolEntry = pool.getConnection();
        assertEquals(4,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(6,pool.getIdleConnectionsCount());
        poolEntry.close();
        assertEquals(3,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getTotalConnectionsCount());
        assertEquals(7,pool.getIdleConnectionsCount());
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection_connect() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity= pool.getConnection();
        String re = null;
        try{
            poolEntity.connect(HOST,PORT);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The connection in connection pool can only connect by pool.",re);
        pool.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_getConnection_login() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity= pool.getConnection();
        String re = null;
        try{
            poolEntity.login("admin","123456",false);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The connection in connection pool can only login by pool.",re);
        pool.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_getConnection_setLoadBalance() throws IOException, InterruptedException {
        SimpleDBConnectionPoolConfig config1 = new SimpleDBConnectionPoolConfig();
        config1.setHostName(HOST);
        config1.setPort(PORT);
        config1.setUserId("admin");
        config1.setPassword("123456");
        config1.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config1);
        DBConnection poolEntity= pool.getConnection();
        String re = null;
        try{
            poolEntity.setLoadBalance(true);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The loadBalance configuration of connection in connection pool can only be set in SimpleDBConnectionPoolConfig.",re);
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_getConnection_Failed_TryReconnectNums_enableHighAvailability_false_enableLoadBalance_false(){
        int port=7100;
        int trynums=3;
        config.setMinimumPoolSize(1);
        config.setTryReconnectNums(trynums);
        config.setPort(port);
        String s="";
        try
        {
            pool = new SimpleDBConnectionPool(config);
        }
        catch (Exception e)
        {
            s=e.toString();
        }
        assertEquals("java.lang.RuntimeException: Create connection pool failure, because Connect to "+HOST+":"+port+" failed after "+trynums+" reconnect attempts.",s);
    }
    @Test(expected =RuntimeException.class)
    public void test_SimpleDBConnectionPool_getConnection_Failed_TryReconnectNums_enableHighAvailability_true_enableLoadBalance_false(){
        class LogCapture {
            private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            private final PrintStream originalErr = System.err;
            public void start() {
                System.setErr(new PrintStream(baos));
            }
            public void stop() {
                System.setErr(originalErr);
            }
            public String getLogMessages() {
                return baos.toString();
            }
        }
        int port=7100;
        int trynums=3;
        config.setTryReconnectNums(trynums);
        config.setMinimumPoolSize(2);
        config.setPort(port);
        config.setEnableHighAvailability(true);
        config.setLoadBalance(false);
        String []highAvailabilitySites={"localhost:7200"};
        config.setHighAvailabilitySites(highAvailabilitySites);
        LogCapture logCapture = new LogCapture();
        logCapture.start();
        pool = new SimpleDBConnectionPool(config);
        logCapture.stop();
        String s=logCapture.getLogMessages();
        assertTrue(s.contains("Connect failed after "+trynums+" reconnect attemps for every node in high availability sites."));
    }
    @Test(expected = RuntimeException.class)
    public void test_SimpleDBConnectionPool_getConnection_Failed_TryReconnectNums_enableHighAvailability_true_enableLoadBalance_true(){
        class LogCapture {
            private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            private final PrintStream originalErr = System.err;
            public void start() {
                System.setErr(new PrintStream(baos));
            }
            public void stop() {
                System.setErr(originalErr);
            }
            public String getLogMessages() {
                return baos.toString();
            }
        }
        int port=7100;
        int trynums=3;
        config.setTryReconnectNums(trynums);
        config.setMinimumPoolSize(1);
        config.setPort(port);
        config.setEnableHighAvailability(true);
        config.setLoadBalance(true);
        String []highAvailabilitySites={"localhost:7200","localhost:7300"};
        config.setHighAvailabilitySites(highAvailabilitySites);
        LogCapture logCapture = new LogCapture();
        logCapture.start();
        pool = new SimpleDBConnectionPool(config);
        String s=logCapture.getLogMessages();
        assertTrue(s.contains("Connect failed after "+trynums+" reconnect attemps for every node in high availability sites."));
    }

    @Test
    public void test_SimpleDBConnectionPool_upload_all_dateType() throws Exception {
        config.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config);
        DBConnection poolEntry = pool.getConnection();
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        bbv.Append(new BasicBoolean(false));
        System.out.println(bbv.rows());
        cols.add(bbv);
        colNames.add("cbool");
        BasicByteVector byv = new BasicByteVector(1);
        byv.add((byte) 22);
        byv.add((byte) 57);
        byv.Append(new BasicByte((byte) 13));
        System.out.println(byv.rows());
        cols.add(byv);
        colNames.add("cchar");
        BasicShortVector bsv = new BasicShortVector(1);
        bsv.add((short) 12);
        bsv.Append(new BasicShort((short) 35));
        bsv.add((short) 73);
        System.out.println(bsv.rows());
        cols.add(bsv);
        colNames.add("cshort");
        BasicIntVector biv = new BasicIntVector(1);
        biv.Append(new BasicInt(5));
        biv.add(11);
        biv.Append(new BasicInt(76));
        System.out.println(biv.rows());
        cols.add(biv);
        colNames.add("cint");
        BasicLongVector blv = new BasicLongVector(1);
        blv.add(12);
        blv.Append(new BasicLong(367));
        blv.Append(new BasicLong(23));
        System.out.println(blv.rows());
        cols.add(blv);
        colNames.add("clong");
        BasicDateVector bdv = new BasicDateVector(1);
        bdv.add(1);
        bdv.Append(new BasicDate(LocalDate.MIN));
        bdv.Append(new BasicDate(LocalDate.now()));
        System.out.println(bdv.rows());
        System.out.println(bdv.rows());
        cols.add(bdv);
        colNames.add("cdate");
        BasicMonthVector bmv = new BasicMonthVector(1);
        bmv.add(346);
        bmv.Append(new BasicMonth(2010, Month.APRIL));
        bmv.Append(new BasicMonth(2006,Month.MARCH));
        System.out.println(bmv.rows());
        cols.add(bmv);
        colNames.add("cmonth");
        BasicTimeVector btv = new BasicTimeVector(1);
        btv.add(2345);
        btv.Append(new BasicTimeVector(new int[]{46284,5839}));
        System.out.println(btv.rows());
        cols.add(btv);
        colNames.add("ctime");
        BasicMinuteVector bmiv = new BasicMinuteVector(1);
        bmiv.Append(new BasicMinuteVector(new int[]{749,904}));
        bmiv.add(432);
        System.out.println(bmiv.rows());
        cols.add(bmiv);
        colNames.add("cminute");
        BasicSecondVector bsev = new BasicSecondVector(1);
        bsev.add(17);
        bsev.Append(new BasicSecondVector(new int[]{4890,494}));
        System.out.println(bsev.rows());
        cols.add(bsev);
        colNames.add("csecond");
        BasicDateTimeVector bdtv = new BasicDateTimeVector(1);
        bdtv.Append(new BasicDateTimeVector(new int[]{49,242}));
        bdtv.add(25);
        System.out.println(bdtv.rows());
        cols.add(bdtv);
        colNames.add("cdatetime");
        BasicTimestampVector btsv = new BasicTimestampVector(1);
        btsv.Append(new BasicTimestampVector(new long[]{2839,480}));
        btsv.add(341);
        System.out.println(btsv.rows());
        cols.add(btsv);
        colNames.add("ctimestamp");
        BasicNanoTimeVector bntv = new BasicNanoTimeVector(1);
        bntv.add(521);
        bntv.Append(new BasicNanoTime(LocalTime.now()));
        bntv.Append(new BasicNanoTimeVector(new long[]{353566}));
        System.out.println(bntv.rows());
        cols.add(bntv);
        colNames.add("cnanotime");
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(1);
        bntsv.Append(new BasicNanoTimestampVector(new long[]{38297658492L}));
        bntsv.add(78900482747L);
        bntsv.Append(new BasicNanoTimestamp(LocalDateTime.MAX));
        System.out.println(bntsv.rows());
        cols.add(bntsv);
        colNames.add("cnanotimestamp");
        BasicFloatVector bfv = new BasicFloatVector(1);
        bfv.Append(new BasicFloatVector(new float[]{(float) 4580.02, (float) 394.3}));
        bfv.add((float) 5.981);
        System.out.println(bfv.rows());
        cols.add(bfv);
        colNames.add("cfloat");
        BasicDoubleVector bdbv = new BasicDoubleVector(1);
        bdbv.add(15.32);
        bdbv.Append(new BasicDoubleVector(new double[]{748.55}));
        bdbv.Append(new BasicDouble(7.17));
        System.out.println(bdbv.rows());
        cols.add(bdbv);
        colNames.add("cdouble");
        BasicStringVector bstv = new BasicStringVector(1);
        bstv.Append(new BasicStringVector(new String[]{"hello","abandon"}));
        bstv.add("lambada");
        System.out.println(bstv.rows());
        cols.add(bstv);
        colNames.add("cstring");
        List<String> list = new ArrayList<>();
        list.add("KingBase");
        list.add("vastBase");
        list.add(null);
        list.add("OceanBase");
        BasicSymbolVector bsyv = new BasicSymbolVector(list);
        cols.add(bsyv);
        colNames.add("csymbol");
        String[] array = new String[]{"Dolphindb","MongoDB","GaussDB","GoldenDB"};
        BasicStringVector bblv = new BasicStringVector(array,true,true);
        cols.add(bblv);
        colNames.add("cblob");
        BasicUuidVector buv = new BasicUuidVector(1);
        buv.add(new Long2(4758,1231));
        buv.Append(new BasicUuid(5890,943));
        buv.Append(new BasicUuidVector(new Long2[]{new Long2(9000,659)}));
        cols.add(buv);
        colNames.add("cuuid");
        System.out.println(buv.rows());
        BasicDateHourVector bdhv = new BasicDateHourVector(1);
        bdhv.Append(new BasicDateHourVector(new int[]{225,37}));
        bdhv.add(28);
        System.out.println(bdhv.rows());
        cols.add(bdhv);
        colNames.add("cdatehour");
        BasicIPAddrVector biav = new BasicIPAddrVector(1);
        biav.add(new Long2(231,489));
        biav.Append(new BasicIPAddrVector(new Long2[]{new Long2(34837,2938),new Long2(4794,95838)}));
        System.out.println(biav.rows());
        cols.add(biav);
        colNames.add("cipaddr");
        BasicInt128Vector bi128v = new BasicInt128Vector(1);
        bi128v.add(new Long2(384,390));
        bi128v.Append(new BasicInt128Vector(new Long2[]{new Long2(2719,3829),new Long2(849,49320)}));
        System.out.println(bi128v.rows());
        cols.add(bi128v);
        colNames.add("cint128");
        BasicComplexVector bcv = new BasicComplexVector(1);
        bcv.Append(new BasicComplexVector(new Double2[]{new Double2(23.04,3718.52),new Double2(37.23,25.12)}));
        bcv.add(new Double2(16.71,35.778));
        System.out.println(bcv.rows());
        cols.add(bcv);
        colNames.add("ccomplex");
        BasicPointVector bpv = new BasicPointVector(1);
        bpv.Append(new BasicPointVector(new Double2[]{new Double2(0.83,4.51),new Double2(33.16,49.71)}));
        bpv.add(new Double2(52.10,45.43));
        System.out.println(bpv.rows());
        cols.add(bpv);
        colNames.add("cpoint");
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(1,2);
        bd32v.add(35);
        bd32v.Append(new BasicDecimal32(17,2));
        bd32v.Append(new BasicDecimal32(25,2));
        System.out.println(bd32v.rows());
        cols.add(bd32v);
        colNames.add("cdecimal32");
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(1,4);
        bd64v.add(349);
        bd64v.Append(new BasicDecimal64(5372,4));
        bd64v.Append(new BasicDecimal64(2336,4));
        System.out.println(bd64v.rows());
        cols.add(bd64v);
        colNames.add("cdecimal64");

        BasicDecimal128Vector bd128v = new BasicDecimal128Vector(1,4);
        bd128v.add(new BigDecimal(349));
        bd128v.Append(new BasicDecimal128("5372",4));
        bd128v.Append(new BasicDecimal128("2336",4));
        System.out.println(bd128v.rows());
        cols.add(bd128v);
        colNames.add("cdecimal128");

        BasicTable bt = new BasicTable(colNames,cols);
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",bt);
        poolEntry.upload(map);
        BasicTable ta = (BasicTable) poolEntry.run("testUpload;");
        assertEquals(4,ta.rows());
        assertEquals(28,ta.columns());
        assertEquals(bt.getString(),ta.getString());
        pool.close();
    }

    @Test
    public void test_SimpleDBConnectionPool_insert_into_memoryTable_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n" +
                "n=100;\n" +
                "t1 = table(n:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "share t1 as tt;\n" +
                "boolv = bool(rand([true, false, NULL], n));\n" +
                "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                "intv = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                "share table(boolv, charv, intv, shortv, longv, floatv, doublev, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, uuidv, datehourv, ippaddrv, int128v, blobv, decimal32v, decimal64v, decimal128v) as data;\n";
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        for (int i = 1; i <= 10; i++) {
            DBConnection poolEntry1 = pool.getConnection();
            poolEntry1.run("tt.append!(data)");
            BasicTable re1 = (BasicTable) poolEntry1.run("select * from tt");
            Assert.assertEquals(100 * i, re1.rows());
            poolEntry1.close();
        }
        Thread[] threads = new Thread[3];
        threads[0] = new Thread(() -> {
            BasicTable re1 = null;
            DBConnection poolEntry1 = pool.getConnection();
            try {
                re1 = (BasicTable) poolEntry1.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(1000, re1.rows());
            poolEntry1.close();
        });
        threads[1] = new Thread(() -> {
            BasicTable re2 = null;
            DBConnection poolEntry2 = pool.getConnection();
            try {
                re2 = (BasicTable) poolEntry2.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(1000, re2.rows());
            poolEntry2.close();
        });
        threads[2] = new Thread(() -> {
            BasicTable re3 = null;
            DBConnection poolEntry3 = pool.getConnection();
            try {
                re3 = (BasicTable) poolEntry3.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(1000, re3.rows());
            poolEntry3.close();
        });
    }
    @Test
    public void test_SimpleDBConnectionPool_insert_into_memoryTable_arrayVector_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(10);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n"+
                "colNames=\"col\"+string(1..26);\n" +
                "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                "share table(1:0,colNames,colTypes) as tt;\n" +
                "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], 1000), 100))\n" +
                "cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), 1000), 100))\n" +
                "cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000), 100))\n" +
                "cint = array(INT[]).append!(cut(take(-100..100 join NULL, 1000), 100))\n" +
                "clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000), 100))\n" +
                "cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254, 100))\n" +
                "cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254f, 100))\n" +
                "cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000), 100))\n" +
                "cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000), 100))\n" +
                "ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000), 100))\n" +
                "cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000), 100))\n" +
                "csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000), 100))\n" +
                "cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000), 100))\n" +
                "ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000), 100))\n" +
                "cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000), 100))\n" +
                "cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000), 100))\n" +
                "cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000), 100))\n" +
                "cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000), 100))\n" +
                "ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000) + 0.254, 3), 100))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, 1000) + 0.25467, 4), 100))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, 1000) + 0.25467, 5), 100))\n" +
                "data = table(cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex,cpoint,cdecimal32,cdecimal64,cdecimal128)\n" +
                "insert into tt(col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26) values(cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex, cpoint, cdecimal32, cdecimal64, cdecimal128);" ;
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        Thread [] threads = new Thread[3];
        threads[0] = new Thread(() -> {
            BasicTable re1 = null;
            DBConnection poolEntry1 = pool.getConnection();
            try {
                re1 = (BasicTable)poolEntry1.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(10,re1.rows());
            poolEntry1.close();
        });
        threads[1] = new Thread(() -> {
            BasicTable re2 = null;
            DBConnection poolEntry2 = pool.getConnection();
            try {
                re2 = (BasicTable)poolEntry2.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(10,re2.rows());
            poolEntry2.close();
        });
        threads[2] = new Thread(() -> {
            BasicTable re3 = null;
            DBConnection poolEntry3 = pool.getConnection();
            try {
                re3 = (BasicTable)poolEntry3.run("select * from tt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.assertEquals(10,re3.rows());
            poolEntry3.close();
        });

        for (int i = 0; i < 3; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 3; i++) {
            threads[i].join();
        }
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_insert_into_dfs_arrayVector_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..26);\n" +
                "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                "t1 =  table(1:0,colNames,colTypes) ;\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 0 5 11,,'TSDB') \n"+
                "db.createPartitionedTable(t1, `pt, `col1,,`col1)\n";
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        DBConnection poolEntry1 = pool.getConnection();
        String script1 = "col1 = 1..10;\n" +
                "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], 1000), 100))\n" +
                "cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), 1000), 100))\n" +
                "cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000), 100))\n" +
                "cint = array(INT[]).append!(cut(take(-100..100 join NULL, 1000), 100))\n" +
                "clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000), 100))\n" +
                "cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254, 100))\n" +
                "cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254f, 100))\n" +
                "cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000), 100))\n" +
                "cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000), 100))\n" +
                "ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000), 100))\n" +
                "cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000), 100))\n" +
                "csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000), 100))\n" +
                "cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000), 100))\n" +
                "ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000), 100))\n" +
                "cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000), 100))\n" +
                "cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000), 100))\n" +
                "cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000), 100))\n" +
                "cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000), 100))\n" +
                "ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000) + 0.254, 3), 100))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, 1000) + 0.25467, 4), 100))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, 1000) + 0.25467, 5), 100))\n" +
                "share table(col1, cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex,cpoint,cdecimal32,cdecimal64,cdecimal128) as data;\n" +
                "pt = loadTable(\"dfs://test_append_type_tsdb1\",`pt)\n"+
                "pt.append!(data)\n";
        poolEntry1.run(script1);
        poolEntry1.close();
        Thread [] threads = new Thread[100];
        for (int i = 0; i < 100 ; ++i) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry2 =  pool.getConnection();
                try {
                    BasicTable re1 = (BasicTable)poolEntry2.run("select count(*) from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    assertEquals("10",re1.getColumn(0).get(0).toString());
                    BasicTable re2 = (BasicTable)poolEntry2.run("select * from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    for(int j=0; j<10; j++){
                        assertEquals(String.valueOf(j+1),re2.getColumn(0).get(j).toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry2.close();
            });
        }
        for (int i = 0; i < 100; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 100; i++){
            threads[i].join();
        }
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_insert_into_DimensionTable_arrayVector_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..26);\n" +
                "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                "t1 =  table(1:0,colNames,colTypes) ;\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 0 5 11,,'TSDB') \n"+
                "db.createTable(t1, `pt,,`col1)\n";
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        DBConnection poolEntry1 = pool.getConnection();
        String script1 = "col1 = 1..10;\n" +
                "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], 1000), 100))\n" +
                "cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), 1000), 100))\n" +
                "cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000), 100))\n" +
                "cint = array(INT[]).append!(cut(take(-100..100 join NULL, 1000), 100))\n" +
                "clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000), 100))\n" +
                "cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254, 100))\n" +
                "cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254f, 100))\n" +
                "cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000), 100))\n" +
                "cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000), 100))\n" +
                "ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000), 100))\n" +
                "cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000), 100))\n" +
                "csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000), 100))\n" +
                "cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000), 100))\n" +
                "ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000), 100))\n" +
                "cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
                "cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000), 100))\n" +
                "cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000), 100))\n" +
                "cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000), 100))\n" +
                "cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000), 100))\n" +
                "ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000) + 0.254, 3), 100))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, 1000) + 0.25467, 4), 100))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, 1000) + 0.25467, 5), 100))\n" +
                "share table(col1, cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex,cpoint,cdecimal32,cdecimal64,cdecimal128) as data;\n" +
                "pt = loadTable(\"dfs://test_append_type_tsdb1\",`pt)\n"+
                "pt.append!(data)\n";
        poolEntry1.run(script1);
        poolEntry1.close();
        Thread [] threads = new Thread[100];
        for (int i = 0; i < 100 ; ++i) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry2 =  pool.getConnection();
                try {
                    BasicTable re1 = (BasicTable)poolEntry2.run("select count(*) from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    assertEquals("10",re1.getColumn(0).get(0).toString());
                    BasicTable re2 = (BasicTable)poolEntry2.run("select * from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    for(int j=0; j<10; j++){
                        assertEquals(String.valueOf(j+1),re2.getColumn(0).get(j).toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry2.close();
            });
        }
        for (int i = 0; i < 100; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 100; i++){
            threads[i].join();
        }
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_insert_into_dfs_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "n=1000;\n" +
                "t1 = table(n:0, `col1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [INT, BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]);\n" +
                "share t1 as tt;\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 0 100 200 300 400 500 600 700 800 900 1001,,'TSDB') \n"+
                "db.createPartitionedTable(t1, `pt, `col1,,`col1)\n";
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        DBConnection poolEntry1 = pool.getConnection();
        String script1 = "n = 1000;\n" +
                    "col1 = 1..1000;\n" +
                    "boolv = bool(rand([true, false, NULL], n));\n" +
                    "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                    "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                    "intv = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                    "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                    "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                    "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                    "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                    "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                    "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                    "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                    "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                    "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                    "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                    "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                    "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                    "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                    "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                    "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                    "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                    "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                    "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                    "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                    "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                    "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                    "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                    "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                    "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                    "share table(col1, boolv, charv, intv, shortv, longv, floatv, doublev, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, uuidv, datehourv, ippaddrv, int128v, blobv, decimal32v, decimal64v, decimal128v) as data;\n"+
                    "pt = loadTable(\"dfs://test_append_type_tsdb1\",`pt)\n"+
                    "pt.append!(data)\n";
        poolEntry1.run(script1);
        poolEntry1.close();
        Thread [] threads = new Thread[100];
        for (int i = 0; i < 100 ; ++i) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry2 =  pool.getConnection();
                try {
                    BasicTable re1 = (BasicTable)poolEntry2.run("select count(*) from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    assertEquals("1000",re1.getColumn(0).get(0).toString());
                    BasicTable re2 = (BasicTable)poolEntry2.run("select * from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    for(int j=0; j<1000; j++){
                        assertEquals(String.valueOf(j+1),re2.getColumn(0).get(j).toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry2.close();
            });
        }
        for (int i = 0; i < 100; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 100; i++){
            threads[i].join();
        }
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_insert_into_DimensionTable_all_dateType() throws IOException, InterruptedException {
        config.setInitialPoolSize(100);
        pool = new SimpleDBConnectionPool(config);
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "n=1000;\n" +
                "t1 = table(n:0, `col1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [INT, BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]);\n" +
                "share t1 as tt;\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 0 100 200 300 400 500 600 700 800 900 1001,,'TSDB') \n"+
                "db.createTable(t1, `pt,,`col1)\n";
        DBConnection poolEntry = pool.getConnection();
        poolEntry.run(script);
        poolEntry.close();
        DBConnection poolEntry1 = pool.getConnection();
        String script1 = "n = 1000;\n" +
                "col1 = 1..1000;\n" +
                "boolv = bool(rand([true, false, NULL], n));\n" +
                "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                "intv = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                "share table(col1, boolv, charv, intv, shortv, longv, floatv, doublev, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, uuidv, datehourv, ippaddrv, int128v, blobv, decimal32v, decimal64v, decimal128v) as data;\n"+
                "pt = loadTable(\"dfs://test_append_type_tsdb1\",`pt)\n"+
                "pt.append!(data)\n";
        poolEntry1.run(script1);
        poolEntry1.close();
        Thread [] threads = new Thread[100];
        for (int i = 0; i < 100 ; ++i) {
            threads[i] = new Thread(() -> {
                DBConnection poolEntry2 =  pool.getConnection();
                try {
                    BasicTable re1 = (BasicTable)poolEntry2.run("select count(*) from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    assertEquals("1000",re1.getColumn(0).get(0).toString());
                    BasicTable re2 = (BasicTable)poolEntry2.run("select * from loadTable(\"dfs://test_append_type_tsdb1\",`pt);");
                    for(int j=0; j<1000; j++){
                        assertEquals(String.valueOf(j+1),re2.getColumn(0).get(j).toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                poolEntry2.close();
            });
        }
        for (int i = 0; i < 100; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 100; i++){
            threads[i].join();
        }
        pool.close();
    }
    @Test
    public void test_SimpleDBConnectionPool_upload_WideTable() throws Exception {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        for(int i=0;i<1000;i++){
            colNames.add("id"+i);
            Vector v = null;
            if((i%6) == 0){
                v = new BasicIntVector(new int[]{i,i+5,i+10,i+15});
            }else if((i%6) == 1){
                v = new BasicDateVector(new int[]{i,i+105,i+110,i+115});
            }else if((i%6) == 2){
                v = new BasicComplexVector(new Double2[]{new Double2(i+0.1,i+0.2),new Double2(i+100.5,i-0.25),new Double2(i+1.35,i-0.75),new Double2(i+1.65,i-0.5)});
            }else if((i%6) == 3){
                v = new BasicDecimal32Vector(4,4);
                v.set(0,new BasicDecimal32(i+3,4));
                v.set(1,new BasicDecimal32(i+6,4));
                v.set(2,new BasicDecimal32(i+9,4));
                v.set(3,new BasicDecimal32(i+12,4));
            }else if((i%6) == 4){
                v = new BasicNanoTimeVector(new long[]{i+4,i+8,i+12,i+16});
            }else{
                v = new BasicByteVector(new byte[]{(byte) ('a'+i%6), (byte) ('e'+i%6), (byte) ('k'+i%6), (byte) ('q'+i%6)});
            }
            cols.add(v);
        }
        BasicTable bt = new BasicTable(colNames,cols);
        pool = new SimpleDBConnectionPool(config);
        DBConnection poolEntry = pool.getConnection();
        Map<String,Entity> map = new HashMap<>();
        map.put("wideTable",bt);
        poolEntry.upload(map);
        BasicTable ua = (BasicTable) poolEntry.run("wideTable;");
        assertEquals(1000,ua.columns());
        assertEquals(bt.getString(),ua.getString());
    }
}

