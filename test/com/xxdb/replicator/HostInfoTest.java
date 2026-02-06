package com.xxdb.replicator;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.BasicTable;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static com.xxdb.Prepare.*;
import static com.xxdb.Prepare.checkData;

public class HostInfoTest {
    private static DBConnection conn= new DBConnection();
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");
    static String port1 = ipports[0].split(":")[1];
    static String port2 = ipports[1].split(":")[1];

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
    public void test_HostInfo_HOST_null() throws IOException {
        String re = null;
        try{
            HostInfo host = new HostInfo(null,PORT,"admin","123456","label1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'host' cannot be null or empty.",re);
    }

    @Test
    public void test_HostInfo_HOST_null_1() throws IOException {
        String re = null;
        try{
            HostInfo host = new HostInfo("",PORT,"admin","123456","label1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'host' cannot be null or empty.",re);
    }

    @Test
    public void test_HostInfo_HOST_not_exist() throws IOException {
        String re = null;
        try{
            List<HostInfo> hostInfoList = new ArrayList<>();
            HostInfo host1 = new HostInfo("111",Integer.parseInt(port1),"admin","123456","label1");
            HostInfo host2 = new HostInfo("111",Integer.parseInt(port2),"admin","123456","label2");
            hostInfoList.add(host1);
            hostInfoList.add(host2);
            ReplicatorConfig replicatorConfig = new ReplicatorConfig();
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
            StreamReplicator replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Failed to connect to host 'label1'.",re);
    }

    @Test
    public void test_HostInfo_port_negative() throws IOException {
        String re = null;
        try{
            HostInfo host = new HostInfo(HOST,-1,"admin","123456","label1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'port' must be between 1 and 65535.",re);
    }

    @Test
    public void test_HostInfo_port_65536() throws IOException {
        String re = null;
        try{
            HostInfo host = new HostInfo(HOST,65536,"admin","123456","label1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'port' must be between 1 and 65535.",re);
    }

    @Test
    public void test_HostInfo_port_not_exist() throws IOException {
        String re = null;
        try{
            List<HostInfo> hostInfoList = new ArrayList<>();
            HostInfo host1 = new HostInfo(HOST,111,"admin","123456","label1");
            HostInfo host2 = new HostInfo(HOST,112,"admin","123456","label2");
            hostInfoList.add(host1);
            hostInfoList.add(host2);
            ReplicatorConfig replicatorConfig = new ReplicatorConfig();
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
            StreamReplicator replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Failed to connect to host 'label1'.",re);
    }

    @Test//用户密码为空时候 非登录态连接成功
    public void test_HostInfo_userId_password_null() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"","13456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(1,1,1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re2.rows());
        checkData(re1, re2);
        Assert.assertEquals("col1 col2 col3\n" +
                "---- ---- ----\n" +
                "1    1    1   \n",re1.getString());
    }

    @Test
    public void test_HostInfo_userId_password_not_match() throws IOException {
        String re = null;
        try{
            List<HostInfo> hostInfoList = new ArrayList<>();
            String port1 = ipports[0].split(":")[1];
            String port2 = ipports[1].split(":")[1];
            HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"aa","13456","label1");
            HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"aa","123456","label2");
            hostInfoList.add(host1);
            hostInfoList.add(host2);
            ReplicatorConfig replicatorConfig = new ReplicatorConfig();
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
            Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
            StreamReplicator replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("The user name or password is incorrect. function: login"));
    }

    @Test
    public void test_HostInfo_label_null() throws IOException {
        String re = null;
        try{
            HostInfo host1 = new HostInfo(HOST,PORT,"admin","123456","");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'label' cannot be null or empty.",re);
    }

    @Test
    public void test_HostInfo_label_null_1() throws IOException {
        String re = null;
        try{
            HostInfo host1 = new HostInfo(HOST,PORT,"admin","123456",null);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'label' cannot be null or empty.",re);
    }

    @Test
    public void test_HostInfo_label_all_characters() throws IOException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 1qqafdfdfdfd 2,dsffffffffffffffffffffffffffff 3, 4,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 6,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 999999999999999");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 1qqafdfdfdfd 2,dsffffffffffffffffffffffffffff 3, 4,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 6,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testchahahhahahahahaaaaaaaaa超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 超长长 9999999999999991");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
        BasicTable re = (BasicTable)conn.run("select * from Trades");
        for(int i=0;i<34;i++){
            ErrorCodeInfo ret = replicator.insert(
                    re.getColumn(0).get(i),
                    re.getColumn(1).get(i),
                    re.getColumn(2).get(i),
                    re.getColumn(3).get(i),
                    re.getColumn(4).get(i),
                    re.getColumn(5).get(i),
                    re.getColumn(6).get(i),
                    re.getColumn(7).get(i),
                    re.getColumn(8).get(i),
                    re.getColumn(9).get(i),
                    re.getColumn(10).get(i),
                    re.getColumn(11).get(i),
                    re.getColumn(12).get(i),
                    re.getColumn(13).get(i),
                    re.getColumn(14).get(i),
                    re.getColumn(15).get(i),
                    re.getColumn(16).get(i),
                    re.getColumn(17).get(i),
                    re.getColumn(18).get(i),
                    re.getColumn(19).get(i),
                    re.getColumn(20).get(i),
                    re.getColumn(21).get(i),
                    re.getColumn(22).get(i),
                    re.getColumn(23).get(i),
                    re.getColumn(24).get(i),
                    re.getColumn(25).get(i));
        }
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from Receive");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from Receive");
        System.out.println("re2："+re2.rows());
        checkData(re,re1);
        checkData(re,re2);
    }

    @Test
    public void test_HostInfo_basic_function() throws IOException {
        HostInfo host1 = new HostInfo(HOST,PORT,"admin","123456","label1");
        Assert.assertEquals(HOST,host1.getHost());
        Assert.assertEquals(PORT,host1.getPort());
        Assert.assertEquals("admin",host1.getUserId());
        Assert.assertEquals("123456",host1.getPassword());
        Assert.assertEquals("label1",host1.getLabel());
        Assert.assertEquals("HostInfo{label='label1', host='"+HOST+"', port="+PORT+", userId='admin'}",host1.toString());
//        Assert.assertEquals(-303577811,host1.hashCode());
        Assert.assertEquals(true,host1.equals(host1));
    }

    @Test
    public void test_HostInfo_equals() throws IOException {
        HostInfo host1 = new HostInfo(HOST,PORT,"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,PORT,"admin","123456","label2");
        HostInfo host3 = new HostInfo(HOST,PORT,"admin1","123456","label1");
        Assert.assertEquals(true,host1.equals(host1));
        Assert.assertEquals(true,host2.equals(host2));
        Assert.assertEquals(false,host1.equals(host2));
        Assert.assertEquals(false,host1.equals(null));
        Assert.assertEquals(false,host1.equals(""));
        Assert.assertEquals(true,host1.equals(host3));
    }
}
