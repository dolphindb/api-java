package com.xxdb.replicator;

import com.xxdb.DBConnection;
import com.xxdb.comm.ConnectionState;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

import static com.xxdb.Prepare.*;
import static com.xxdb.Prepare.Preparedata_streamTable_array1;
import static java.lang.Thread.sleep;

public class ReplicatorConfigTest {
    private static DBConnection conn= new DBConnection();
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));

    static String[] ipports = bundle.getString("SITES").split(",");

    @BeforeClass
    public static void setUp() throws IOException {
        try {clear_env_1();}catch (Exception e){}
    }
    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void after() throws IOException, InterruptedException {
        try {clear_env();}catch (Exception e){}
        conn.close();
    }

    @Test
    public void test_ReplicatorConfig_default() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        Assert.assertEquals(null,replicator.getCompression());
        Assert.assertEquals(0,replicator.getBatchInterval());
        Assert.assertEquals(1,replicator.getBatchSize());
        Assert.assertEquals(3,replicator.getMaxRetry());
        Assert.assertEquals(1000,replicator.getRetryInterval());
    }

    @Test
    public void test_ReplicatorConfig_setBatching_batchSize_0() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setBatching(0,1,TimeUnit.SECONDS);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'batchSize' must be greater than 0.",re);
    }
    @Test
    public void test_ReplicatorConfig_setBatching_batchSize_negative() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setBatching(-1,1,TimeUnit.SECONDS);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'batchSize' must be greater than 0.",re);
    }

    @Test
    public void test_ReplicatorConfig_setBatching_batchInterval_negative() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setBatching(1,-1, TimeUnit.SECONDS);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'batchInterval' must be greater than or equal to 0.",re);
    }

    @Test
    public void test_ReplicatorConfig_setBatching_unit_null() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setBatching(1,1, null);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'unit' cannot be null.",re);
    }

    @Test
    public void test_ReplicatorConfig_setRetry_maxRetry_less_then_negative_1() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setRetry(-2,-1, TimeUnit.SECONDS);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'maxRetry' must be -1 (infinite), 0 (no retry), or positive.",re);
    }

    @Test
    public void test_ReplicatorConfig_setRetry_retryInterval_less_then_0() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        String re = null;
        try{
            replicator.setRetry(0,-1, TimeUnit.SECONDS);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'retryInterval' cannot be negative.",re);
    }

    @Test
    public void test_ReplicatorConfig_setRetry_retryInterval_5s() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        replicator.setRetry(0,5, TimeUnit.SECONDS);
        Assert.assertEquals(5000,replicator.getRetryInterval());
    }

    @Test
    public void test_ReplicatorConfig_setRetry_retryInterval_2000ms() throws IOException {
        ReplicatorConfig replicator = new ReplicatorConfig();
        replicator.setRetry(1,2000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(2000,replicator.getRetryInterval());
    }

    @Test
    public void test_ReplicatorConfig_setCompression_compressMethod_less_then_col() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1) as table1;");
        conn2.run("share table(array(INT) as col1) as table1;");
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setCompression(new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        String re = null;
        try{
            StreamReplicator replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Compression array length (2) does not match column count (1).",re);
    }


    @Test
    public void test_ReplicatorConfig_setCompression_compressMethod_more_then_col() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setCompression(new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        String re = null;
        try{
            StreamReplicator replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Compression array length (2) does not match column count (3).",re);
    }

    @Test
    public void test_ReplicatorConfig_setCompression_compressMethod_col_not_support() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, Integer.parseInt(port1), "admin", "123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST, Integer.parseInt(port2), "admin", "123456");
        conn1.run("share table(array(INT[]) as col1,array(CHAR) as col2,array(SYMBOL) as col3) as table1;");
        conn2.run("share table(array(INT[]) as col1,array(CHAR) as col2,array(SYMBOL) as col3) as table1;");
        HostInfo host1 = new HostInfo(HOST, Integer.parseInt(port1), "admin", "123456", "label1");
        HostInfo host2 = new HostInfo(HOST, Integer.parseInt(port2), "admin", "123456", "label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setCompression(new int[]{Vector.COMPRESS_DELTA, Vector.COMPRESS_DELTA, Vector.COMPRESS_DELTA});
        String re = null;
        try {
            StreamReplicator replicator = new StreamReplicator(hostInfoList, "table1", replicatorConfig);
        } catch (Exception ex) {
            re = ex.getMessage();
        }
        Assert.assertEquals("Compression method 'COMPRESS_DELTA' is not supported for column 'col1' of type DT_INT_ARRAY.", re);
    }

    @Test
    public void test_ReplicatorConfig_setCompression_compressMethod_delta() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "colNames = `shortv`intv`longv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv ;\n" +
                "colTypes=[SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" ;
        conn1.run(script);
        conn2.run(script);
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setCompression(new int[]{Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA,Vector.COMPRESS_DELTA});
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"data",replicatorConfig);
        Assert.assertEquals("[2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]",Arrays.toString(replicatorConfig.getCompression()));
        Assert.assertEquals("ReplicatorConfig{batchSize=1, batchInterval=0, maxRetry=3, retryInterval=1000, compression=[2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]}", replicatorConfig.toString());
    }

    @Test
    public void test_ReplicatorConfig_setCompression_compressMethod_lz4() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "colNames = `shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv ;\n" +
                "colTypes=[SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" ;
        conn1.run(script);
        conn2.run(script);
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setCompression(new int[]{Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4,Vector.COMPRESS_LZ4});
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"data",replicatorConfig);
        System.out.println(Arrays.toString(replicatorConfig.getCompression()));
        Assert.assertEquals("[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",Arrays.toString(replicatorConfig.getCompression()));
    }

    @Test
    public void test_ReplicatorConfig_onConnectionStateChange() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig config = new ReplicatorConfig();
        // 设置一个自定义的连接状态变化回调
        config.onConnectionStateChange((state, hostLabel) -> {
            System.out.println("[onConnectionStateChange 触发]");
            System.out.println("主机: " + hostLabel);
            System.out.println("新状态: " + state);

            // 根据状态决定是否继续
            if (state == ConnectionState.Terminated || state == ConnectionState.Initializing) {
                System.out.println("连接异常，可以考虑停止同步");
                return false; // 停止
            } else {
                System.out.println("连接正常或正在重连，继续同步");
                return true; // 继续
            }
        });
        Assert.assertEquals(true,config.getOnConnectionStateChange().onStateChange(ConnectionState.Connected,"label1"));
        Assert.assertEquals(true,config.getOnConnectionStateChange().onStateChange(ConnectionState.Connected,"label2"));
        Assert.assertEquals(false,config.getOnConnectionStateChange().onStateChange(ConnectionState.Terminated,"label1"));
    }

    @Test
    public void test_ReplicatorConfig_onDataDump() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "colNames = `shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv ;\n" +
                "colTypes=[SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" ;
        conn1.run(script);
        conn2.run(script);
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        // 设置一个自定义的连接状态变化回调
        replicatorConfig.onDataDump((hostLabel, table) -> {
            System.out.println("onDataDump called for host: " + hostLabel);
            // 返回 true 表示继续
            return true;
        });
        BasicTable data = (BasicTable)conn1.run("data");
        Assert.assertEquals(true,replicatorConfig.getOnDataDump().onDump("label1",data));
    }
}
