package com.xxdb;

import com.xxdb.DBConnectionPool;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.ExclusiveDBConnectionPoolConfig;
import com.xxdb.data.*;
import com.xxdb.route.PartitionedTableAppender;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExclusiveDBConnectionPoolConfigTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    private void invokeValidate(ExclusiveDBConnectionPoolConfig config) throws Exception {
        Method validate = ExclusiveDBConnectionPoolConfig.class.getDeclaredMethod("validate");
        validate.setAccessible(true);
        validate.invoke(config);
    }

    private ExclusiveDBConnectionPoolConfig baseConfig() {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        return config;
    }

    private String createPoolAndGetMessage(ExclusiveDBConnectionPoolConfig config) {
        String re = null;
        try {
            new ExclusiveDBConnectionPool(config);
        } catch (Exception e) {
            re = e.getMessage();
        }
        System.out.println(re);
        return re;

    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_host_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(null);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals("localhost", config.getHostName());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_host_empty() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName("");
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals("localhost", config.getHostName());
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_host_notSet() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
//        config.setHostName(null);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals("localhost", config.getHostName());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_host_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName("1sss");
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
//        invokeValidate(config);
        String re = null;
        try{
            ExclusiveDBConnectionPool pool1 = new ExclusiveDBConnectionPool(config);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Invalid hostName: 1sss",re);
        config.setHostName("111.111.111.111.111");
        String re1 = null;
        try{
            ExclusiveDBConnectionPool pool1 = new ExclusiveDBConnectionPool(config);
        }catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals("Invalid hostName: 111.111.111.111.111",re1);

    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_port_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        String re = null;
        try {
            config.setPort(-111);
        } catch (Exception e) {
            re = e.getMessage();
        }
        assertEquals("The port should be positive.", re);
        String re1 = null;
        try {
            config.setPort(0);
        } catch (Exception e) {
            re1 = e.getMessage();
        }
        assertEquals("The port should be positive.", re1);
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_port_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setUserId("admin");
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals(8848, config.getPort());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_userId_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setUserId("admin_error");
        String re = createPoolAndGetMessage(config);
        assertTrue(re != null && re.contains("The user name or password is incorrect."));
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_userId_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals("", config.getUserId());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_userId_not_admin() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        try {
            conn.run("def create_user(){try{deleteUser(`test1)}catch(ex){};createUser(`test1, '123456');};" +
                    "rpc(getControllerAlias(),create_user);");
            ExclusiveDBConnectionPoolConfig config = baseConfig();
            config.setUserId("test1");
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);
            assertEquals(5, pool.getCurrentConnectionCount());
            pool.shutdown();
        } finally {
            conn.close();
        }
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_password_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setPassword("123456_error");
        String re = createPoolAndGetMessage(config);
        assertTrue(re != null && re.contains("The user name or password is incorrect. "));
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_password_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        invokeValidate(config);
        assertEquals("", config.getPassword());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_InitialScript_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setInitialScript("pt.append!(t);");
        String re = createPoolAndGetMessage(config);
        assertTrue(re != null && re.contains("Cannot recognize the token pt script:"));
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_compless_true() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setCompress(true);
        invokeValidate(config);
        assertTrue(config.isCompress());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_compless_false() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setCompress(false);
        invokeValidate(config);
        assertEquals(false, config.isCompress());
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_UseSSL_true() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setUseSSL(true);
        invokeValidate(config);
        assertTrue(config.isUseSSL());
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_UseSSL_false() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setUseSSL(false);
        invokeValidate(config);
        assertEquals(false, config.isUseSSL());
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_UsePython_true() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setUsePython(true);
        invokeValidate(config);
        assertTrue(config.isUsePython());
    }
    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_UsePython_false() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setUsePython(false);
        invokeValidate(config);
        assertEquals(false, config.isUsePython());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_LoadBalance_true_highAvailablity_false() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setLoadBalance(true);
        config.setEnableHighAvailability(false);
        invokeValidate(config);
        assertTrue(config.isLoadBalance());
        assertEquals(false, config.isEnableHighAvailability());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_LoadBalance_true_highAvailablity_true() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setLoadBalance(true);
        config.setEnableHighAvailability(true);
        config.setHighAvailabilitySites(new String[]{HOST + ":" + PORT});
        invokeValidate(config);
        assertTrue(config.isLoadBalance());
        assertTrue(config.isEnableHighAvailability());
        assertEquals(HOST + ":" + PORT, config.getHighAvailabilitySites()[0]);
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_LoadBalance_false_highAvailablity_false() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setLoadBalance(false);
        config.setEnableHighAvailability(false);
        invokeValidate(config);
        assertEquals(false, config.isLoadBalance());
        assertEquals(false, config.isEnableHighAvailability());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_LoadBalance_false_highAvailablity_true() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setLoadBalance(false);
        config.setEnableHighAvailability(true);
        config.setHighAvailabilitySites(new String[]{HOST + ":" + PORT});
        invokeValidate(config);
        assertEquals(false, config.isLoadBalance());
        assertTrue(config.isEnableHighAvailability());
        assertEquals(HOST + ":" + PORT, config.getHighAvailabilitySites()[0]);
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_MinimumandPoolSize_and_MaximumPoolSize_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        invokeValidate(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_maxpoolsize_minpoolsize_realtion() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMaximumPoolSize(6);
        config.setMinimumPoolSize(-1);
        invokeValidate(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(6, config.getMaximumPoolSize());

        config = baseConfig();
        config.setMaximumPoolSize(-4);
        config.setMinimumPoolSize(-6);
        invokeValidate(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_maxpoolsize_minpoolsize_realtion_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMaximumPoolSize(4);
        config.setMinimumPoolSize(6);
        invokeValidate(config);
        assertEquals(6, config.getMaximumPoolSize());
        assertEquals(6, config.getMinimumPoolSize());

        config = baseConfig();
        config.setMaximumPoolSize(-5);
        config.setMinimumPoolSize(6);
        invokeValidate(config);
        assertEquals(6, config.getMaximumPoolSize());
        assertEquals(6, config.getMinimumPoolSize());

        config = baseConfig();
        config.setMaximumPoolSize(-4);
        config.setMinimumPoolSize(-2);
        invokeValidate(config);
        assertEquals(5, config.getMaximumPoolSize());
        assertEquals(5, config.getMinimumPoolSize());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_InvalididleTimeout_error() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setIdleTimeout(-50);
        invokeValidate(config);
        assertEquals(600000, config.getIdleTimeout());
        config.setIdleTimeout(0);
        invokeValidate(config);
        assertEquals(600000, config.getIdleTimeout());
    }

    @Test
    public void test_ExclusiveDBConnectionPoolConfig_InvalididleTimeout_null() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        invokeValidate(config);
        assertEquals(600000, config.getIdleTimeout());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_minimumPoolSize_zero_default() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(0);
        config.setMaximumPoolSize(2);
        config.setIdleTimeout(10000);
        invokeValidate(config);
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
    }


    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_set_all() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(2);
        config.setMaximumPoolSize(4);
        config.setIdleTimeout(30000);
        config.setInitialScript("1+1");
        config.setCompress(true);
        config.setUseSSL(true);
        config.setUsePython(false);
        config.setLoadBalance(true);
        config.setEnableHighAvailability(true);
        config.setHighAvailabilitySites(new String[]{HOST + ":" + PORT});
        invokeValidate(config);
        assertEquals(HOST, config.getHostName());
        assertEquals(PORT, config.getPort());
        assertEquals("admin", config.getUserId());
        assertEquals("123456", config.getPassword());
        assertEquals(2, config.getMinimumPoolSize());
        assertEquals(4, config.getMaximumPoolSize());
        assertEquals(30000, config.getIdleTimeout());
        assertEquals("1+1", config.getInitialScript());
        assertTrue(config.isCompress());
        assertTrue(config.isUseSSL());
        assertEquals(false, config.isUsePython());
        assertTrue(config.isLoadBalance());
        assertTrue(config.isEnableHighAvailability());
        assertEquals(HOST + ":" + PORT, config.getHighAvailabilitySites()[0]);
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_default() throws Exception {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        invokeValidate(config);
        assertEquals("localhost", config.getHostName());
        assertEquals(8848, config.getPort());
        assertEquals("", config.getUserId());
        assertEquals("", config.getPassword());
        assertEquals(5, config.getMinimumPoolSize());
        assertEquals(5, config.getMaximumPoolSize());
        assertEquals(600000, config.getIdleTimeout());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_maximumPoolSize_lessThanMinimum_adjust() throws IOException {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(6);
        config.setMaximumPoolSize(4);
        config.setIdleTimeout(10000);
        try {
            invokeValidate(config);
        } catch (Exception e) {
            throw new IOException(e);
        }
        assertEquals(6, config.getMinimumPoolSize());
        assertEquals(6, config.getMaximumPoolSize());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_idleTimeout_small_default() throws IOException {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(1);
        config.setMaximumPoolSize(3);
        config.setIdleTimeout(1);
        try {
            invokeValidate(config);
        } catch (Exception e) {
            throw new IOException(e);
        }
        assertEquals(600000, config.getIdleTimeout());
    }

    @Test
    public void Test_ExclusiveDBConnectionPoolConfig_normalValues() throws IOException {
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(1);
        config.setMaximumPoolSize(3);
        config.setIdleTimeout(10000);
        try {
            invokeValidate(config);
        } catch (Exception e) {
            throw new IOException(e);
        }
        assertEquals(HOST, config.getHostName());
        assertEquals(PORT, config.getPort());
        assertEquals(1, config.getMinimumPoolSize());
        assertEquals(3, config.getMaximumPoolSize());
        assertEquals(10000, config.getIdleTimeout());
    }

    @Test(timeout = 30000)
    public void test_ExclusiveDBConnectionPoolConfig_MinimumPoolSize_equal_MaximumPoolSize() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMinimumPoolSize(5);
        config.setMaximumPoolSize(5);
        config.setIdleTimeout(10000);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);
        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 5; i++){
            BasicDBTask task = new BasicDBTask("sleep(1000);");
            tasks.add(task);
        }
        Thread threads1 = new Thread(() -> {
            pool.execute(tasks);
        });
        threads1.start();
        Thread.sleep(10);

        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(5,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        threads1.join();

        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        Thread.sleep(10100);
        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        pool.shutdown();
    }

    @Test(timeout = 30000)
    public void test_ExclusiveDBConnectionPoolConfig_MinimumPoolSize_less_MaximumPoolSize() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMinimumPoolSize(5);
        config.setMaximumPoolSize(10);
        config.setIdleTimeout(10000);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);
        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++){
            BasicDBTask task = new BasicDBTask("sleep(7000);");
            tasks.add(task);
        }
        Thread threads1 = new Thread(() -> {
            pool.execute(tasks);
        });
        threads1.start();
        Thread.sleep(6000);
        assertEquals(10,pool.getCurrentConnectionCount());
        assertEquals(10,pool.getActiveConnectionsCount());
        assertEquals(0,pool.getIdleConnectionsCount());
        threads1.join();
        assertEquals(10,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(10,pool.getIdleConnectionsCount());
        Thread.sleep(10100);
        //closeIdleConnections_Auto
        assertEquals(5,pool.getCurrentConnectionCount());
        assertEquals(0,pool.getActiveConnectionsCount());
        assertEquals(5,pool.getIdleConnectionsCount());
        pool.shutdown();
    }

    @Test(timeout = 30000)
    public void test_ExclusiveDBConnectionPoolConfig_close() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMinimumPoolSize(5);
        config.setMaximumPoolSize(5);
        config.setIdleTimeout(10000);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);
        assertEquals(5, pool.getCurrentConnectionCount());
        pool.shutdown();
        Thread.sleep(1000);
        assertEquals(0, pool.getCurrentConnectionCount());
        pool.shutdown();
        assertEquals(0, pool.getCurrentConnectionCount());
    }

    @Test(timeout = 30000)
    public void test_ExclusiveDBConnectionPoolConfig_close_new() throws Exception {
        ExclusiveDBConnectionPoolConfig config = baseConfig();
        config.setMinimumPoolSize(6);
        config.setMaximumPoolSize(10);
        config.setIdleTimeout(10000);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);

        List<DBTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(new BasicDBTask("sleep(1000);"));
        }
        pool.execute(tasks);
        assertEquals(10, pool.getCurrentConnectionCount());

        pool.shutdown();
        Thread.sleep(1000);
        assertEquals(0, pool.getCurrentConnectionCount());
    }

    @Test
    public void test_ExclusiveDBConnectionPoolConfig_PartitionedTableAppender() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT,"admin","123456");
        conn.run("if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db=database(\"dfs://testArrayVector\",RANGE,int(1..10),,\"TSDB\")\n" +
                "t = table(1000000:0,`sym`tradeDate`volume`valueTrade,[INT,DATETIME,INT[],DOUBLE])\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym,,`tradeDate)");
        ExclusiveDBConnectionPoolConfig config = new ExclusiveDBConnectionPoolConfig();
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setMinimumPoolSize(3);
        config.setMaximumPoolSize(4);
        config.setIdleTimeout(30000);
        config.setInitialScript("1+1");
        config.setCompress(true);
        config.setUseSSL(false);
        config.setUsePython(false);
        config.setLoadBalance(false);
        config.setEnableHighAvailability(false);
        config.setHighAvailabilitySites(null);
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(config);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector","pt","sym",pool);
        List<String> colNames = new ArrayList<>();
        colNames.add("sym");
        colNames.add("tradesDate");
        colNames.add("volume");
        colNames.add("valueTrade");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector biv = new BasicIntVector(new int[]{1,2,3});
        cols.add(biv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{10,20,30});
        cols.add(bdtv);
        List<Vector> value = new ArrayList<>();
        value.add(new BasicIntVector(new int[]{1,2,3}));
        value.add(new BasicIntVector(new int[]{4,5,6,7,8}));
        value.add(new BasicIntVector(new int[]{9,10,11,13,17,21}));
        BasicArrayVector bav = new BasicArrayVector(value);
        cols.add(bav);
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{1.1,3.6,7.9});
        cols.add(bdv);
        BasicTable bt = new BasicTable(colNames,cols);
        int x = appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_INT_ARRAY,res.getColumn(2).getDataType());
        pool.shutdown();
    }
}
