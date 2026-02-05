package com.xxdb.replicator;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.io.Double2;
import org.junit.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.xxdb.Prepare.*;
import static com.xxdb.Prepare.clear_env;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class StreamReplicatorTest {
    private static DBConnection conn = new DBConnection();
    private static List<HostInfo> hostInfoList = null;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");
    static String port1 = ipports[0].split(":")[1];
    static String port2 = ipports[1].split(":")[1];
    StreamReplicator replicator = null;
    @BeforeClass
    public static void setUp() throws IOException {
        try {clear_env_1();}catch (Exception e){}
    }
    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList = new ArrayList<>();
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

//    @After
    public void after() throws IOException, InterruptedException {
        try {clear_env();}catch (Exception e){}
        replicator.close();
        conn.close();
    }

    @Test
    public void test_StreamReplicator_hostInfoList_null() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        String re = null;
        try{
            replicator = new StreamReplicator(null,"all_dataType",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'hosts' cannot be null or empty.", re);
    }

    @Test
    public void test_StreamReplicator_hostInfoList_null_1() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        List<HostInfo> hostInfoList = new ArrayList<>();
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'hosts' cannot be null or empty.", re);
    }

    @Test
    public void test_StreamReplicator_hostInfoList_null_2() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        List<HostInfo> hostInfoList = new ArrayList<>();
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        hostInfoList.add(null);
        hostInfoList.add(host1);
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("'HostInfo' at index 0 is null.", re);
    }

    @Test
    public void test_StreamReplicator_hostInfoList_host_1() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        List<HostInfo> hostInfoList = new ArrayList<>();
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        hostInfoList.add(host1);
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        Preparedata1(2000);
        BasicTable re = (BasicTable)conn.run("select * from data");
        for(int i=0;i<2000;i++){
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
                    re.getColumn(25).get(i),
                    re.getColumn(26).get(i),
                    re.getColumn(27).get(i));
        }
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.rows());
        checkData(re,re1);
    }

    @Test
    public void test_StreamReplicator_tableName_null() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataTyp1");
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList,null,replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'tableName' cannot be null or empty.", re);
    }

    @Test
    public void test_StreamReplicator_tableName_null_1() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataTyp1");
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList,"",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The param 'tableName' cannot be null or empty.", re);
    }

    @Test
    public void test_StreamReplicator_tableName_not_exist() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataTyp1");
        String re = null;

        try{
            replicator = new StreamReplicator(hostInfoList,"null1",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("Cannot recognize the token null1 "));
    }

    @Test
    public void test_StreamReplicator_tableName_not_match() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType11111");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataTyp1");
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList,"all_dataTyp1",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("Cannot recognize the token all_dataTyp1"));
    }

    @Test
    public void test_StreamReplicator_replicatorConfig_null() throws IOException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        conn2.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1");
        ErrorCodeInfo ret = replicator.insert((int)1, (int)1, (int)-1, (int)0, (int)-1);
        ret = replicator.insert(null, (int)1, (int)1, (int)0, (int)1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2,re1.rows());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_Column_count_mismatch() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "share table(1:0,`int`arrayv,[INT,INT[]]) as dataType_int;";
        conn1.run(script);
        conn2.run(script);
        replicator = new StreamReplicator(hostInfoList,"dataType_int",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(1);
        Assert.assertEquals("code=A2 info=Column count mismatch: expected 2 columns, but get 1.",ret.toString());
        Assert.assertEquals("A2",ret.getErrorCode());
        Assert.assertEquals("Column count mismatch: expected 2 columns, but get 1.",ret.getErrorInfo());
    }

    @Test
    public void test_StreamReplicator_insert_Column_type_not_match() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "share table(1:0,`int`arrayv,[INT,INT[]]) as dataType_int;";
        conn1.run(script);
        conn2.run(script);
        replicator = new StreamReplicator(hostInfoList,"dataType_int",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert("1","1");
        Assert.assertEquals("code=A1 info=Invalid object error when create scalar for column 0: Failed to insert data. Cannot convert String to DT_INT.",ret.toString());
        Assert.assertEquals("A1",ret.getErrorCode());
        Assert.assertEquals("Invalid object error when create scalar for column 0: Failed to insert data. Cannot convert String to DT_INT.",ret.getErrorInfo());
    }

    @Test
    public void test_StreamReplicator_insert_isClosed() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "share table(1:0,`int`arrayv,[INT,INT[]]) as dataType_int;";
        conn1.run(script);
        conn2.run(script);
        replicator = new StreamReplicator(hostInfoList,"dataType_int",replicatorConfig);
        replicator.close();
        ErrorCodeInfo ret = replicator.insert("1","1");
        Assert.assertEquals("code=A7 info=StreamReplicator has been closed.",ret.toString());
        Assert.assertEquals("A7",ret.getErrorCode());
        Assert.assertEquals("StreamReplicator has been closed.",ret.getErrorInfo());
    }

    @Test
    public void test_StreamReplicator_insert_allDateType() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        Preparedata1(2000);
        BasicTable re = (BasicTable)conn.run("select * from data");
        for(int i=0;i<2000;i++){
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
                        re.getColumn(25).get(i),
                        re.getColumn(26).get(i),
                        re.getColumn(27).get(i)
                );
        }
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from all_dataType");
        System.out.println("re2："+re2.rows());
        checkData(re,re1);
        checkData(re,re2);
    }

    @Test//基础类型
    public void test_StreamReplicator_insert_allDateType_java_type() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);

        ErrorCodeInfo ret = replicator.insert(false, (byte)1, (short)1, (int)1, (long)9223372036854775807l, (double)1, (float)1, 1, 1, 1, 1, 1, 1, (long)1, (long)1, (long)1, "symbol", "string", 1, "5d212a78-cc48-e3b1-4235-b4d91473ee87", "192.168.1.13", "e1671797c52e15f763380b45e841ec32", "11111", new BasicPoint(1,1), new BasicComplex(1,1), "2.2", "2.2", "2.2");
        ErrorCodeInfo ret1 = replicator.insert(true, new Byte((byte)-1), new Short((short)-1), new Integer((int)-1), new Long((long)-9223372036854775807l),new Double((double)-1), new Float((float)-1), LocalDate.of(1,1,1), LocalDate.of(1,1,1), LocalTime.of(1,1,1,342), LocalTime.of(1,1,1,342), LocalTime.of(1,1,1,342), LocalDateTime.of(2022,2,1,1,1,2,45364654),LocalDateTime.of(2022,2,1,1,1,2,45364654), LocalTime.of(1,1,1,45364654),LocalDateTime.of(2022,2,1,1,1,2,45364654), "最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testch", "string最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testch", -1, "5d212a78-cc48-e3b1-4235-b4d91473ee87", "192.168.1.13", "e1671797c52e15f763380b45e841ec32", "-11111", new BasicPoint(-1,-1), new BasicComplex(-1,-1), -2.2, -2.2, "-2.2");
        System.out.println(ret1.toString());
        System.out.println(ret.toString());
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.getString());
        for(int i=0;i<re1.columns();i++){
            System.out.println(re1.getColumn(i).getString());
        }
        BasicTable re2 = (BasicTable)conn2.run("select * from all_dataType");
        System.out.println("re2："+re2.rows());
        checkData(re1,re2);
        Assert.assertEquals("[false,true]",re1.getColumn(0).getString());
        Assert.assertEquals("[1,-1]",re1.getColumn(1).getString());
        Assert.assertEquals("[1,-1]",re1.getColumn(2).getString());
        Assert.assertEquals("[1,-1]",re1.getColumn(3).getString());
        Assert.assertEquals("[9223372036854775807,-9223372036854775807]",re1.getColumn(4).getString());
        Assert.assertEquals("[1,-1]",re1.getColumn(5).getString());
        Assert.assertEquals("[1,-1]",re1.getColumn(6).getString());
        Assert.assertEquals("[1970.01.02,0001.01.01]",re1.getColumn(7).getString());
        Assert.assertEquals("[0000.02M,0001.01M]",re1.getColumn(8).getString());
        Assert.assertEquals("[00:00:00.001,01:01:01.000]",re1.getColumn(9).getString());
        Assert.assertEquals("[00:01m,01:01m]",re1.getColumn(10).getString());
        Assert.assertEquals("[00:00:01,01:01:01]",re1.getColumn(11).getString());
        Assert.assertEquals("[1970.01.01T00:00:01,2022.02.01T01:01:02]",re1.getColumn(12).getString());
        Assert.assertEquals("[1970.01.01T00:00:00.001,2022.02.01T01:01:02.045]",re1.getColumn(13).getString());
        Assert.assertEquals("[00:00:00.000000001,01:01:01.045364654]",re1.getColumn(14).getString());
        Assert.assertEquals("[1970.01.01T00:00:00.000000001,2022.02.01T01:01:02.045364654]",re1.getColumn(15).getString());
        Assert.assertEquals("[symbol,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testch]",re1.getColumn(16).getString());
        Assert.assertEquals("[string,string最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWSSSSHHHHHHH这个点做工&&，。、testch]",re1.getColumn(17).getString());
        Assert.assertEquals("[1970.01.01T01,1969.12.31T23]",re1.getColumn(18).getString());
        Assert.assertEquals("[5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87]",re1.getColumn(19).getString());
        Assert.assertEquals("[192.168.1.13,192.168.1.13]",re1.getColumn(20).getString());
        Assert.assertEquals("[e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32]",re1.getColumn(21).getString());
        Assert.assertEquals("[11111,-11111]",re1.getColumn(22).getString());
        Assert.assertEquals("[(1.0, 1.0),(-1.0, -1.0)]",re1.getColumn(23).getString());
        Assert.assertEquals("[1.0+1.0i,-1.0-1.0i]",re1.getColumn(24).getString());
        Assert.assertEquals("[2.20,-2.20]",re1.getColumn(25).getString());
        Assert.assertEquals("[2.2000000,-2.2000000]",re1.getColumn(26).getString());
        Assert.assertEquals("[2.200000000000000000,-2.200000000000000000]",re1.getColumn(27).getString());
    }

    @Test
    public void test_StreamReplicator_insert_allDateType_null() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
        Assert.assertEquals("code= info=",ret.toString());
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.rows());
        System.out.println("re1："+re1.getString());
        BasicTable re2 = (BasicTable)conn2.run("select * from all_dataType");
        System.out.println("re2："+re2.rows());
        System.out.println("re1："+re2.getString());
        checkData(re2,re1);
        for(int i=0;i<28;i++){
            System.out.println(re1.getColumn(i).get(0).getString());
            if (i == 20) {
                Assert.assertEquals("0.0.0.0",re1.getColumn(i).get(0).getString());
            }else if(i==23){
                Assert.assertEquals("(,)",re1.getColumn(i).get(0).getString());
            }else{
                Assert.assertEquals("",re1.getColumn(i).get(0).getString());
            }
        }
    }

    @Test
    public void test_StreamReplicator_insert_allDateType_null_1() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_streamTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_streamTable(HOST,Integer.parseInt(port2),"all_dataType");
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        BasicBoolean boolv = new BasicBoolean(true);
        boolv.setNull();
        BasicByte charv = new BasicByte((byte)1);
        charv.setNull();
        BasicShort shortv = new BasicShort((short)1);
        shortv.setNull();
        BasicInt intv = new BasicInt(1);
        intv.setNull();
        BasicLong longv = new BasicLong(1);
        longv.setNull();
        BasicDouble doublev = new BasicDouble(1);
        doublev.setNull();
        BasicFloat floatv = new BasicFloat(1);
        floatv.setNull();
        BasicDate datev = new BasicDate(1);
        datev.setNull();
        BasicMonth monthv = new BasicMonth(1);
        monthv.setNull();
        BasicTime timev = new BasicTime(1);
        timev.setNull();
        BasicMinute minutev = new BasicMinute(1);
        minutev.setNull();
        BasicSecond secondv = new BasicSecond(1);
        secondv.setNull();
        BasicDateTime datetimev = new BasicDateTime(1);
        datetimev.setNull();
        BasicTimestamp timestampv = new BasicTimestamp(1);
        timestampv.setNull();
        BasicNanoTime nanotimev = new BasicNanoTime(1);
        nanotimev.setNull();
        BasicNanoTimestamp nanotimestampv = new BasicNanoTimestamp((byte)1);
        nanotimestampv.setNull();
        BasicString symbolv = new BasicString("2");
        symbolv.setNull();
        BasicString stringv = new BasicString("1");
        stringv.setNull();
        BasicUuid uuidv = new BasicUuid(1,1);
        uuidv.setNull();
        BasicDateHour datehourv = new BasicDateHour(1);
        datehourv.setNull();
        BasicIPAddr ippaddrv = new BasicIPAddr(1,1);
        ippaddrv.setNull();
        BasicInt128 int128v = new BasicInt128(1,1);
        int128v.setNull();
        BasicString blobv = new BasicString("(byte)1",true);
        blobv.setNull();
        BasicComplex complexv = new BasicComplex(1,1);
        complexv.setNull();
        BasicPoint pointv = new BasicPoint(1,1);
        pointv.setNull();
        BasicDecimal32 decimal32v = new BasicDecimal32("1.1",2);
        decimal32v.setNull();
        BasicDecimal64 decimal64v = new BasicDecimal64("1.1",2);
        decimal64v.setNull();
        BasicDecimal128 decimal128v = new BasicDecimal128("1.1",2);
        decimal128v.setNull();
        ErrorCodeInfo ret = replicator.insert(boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,symbolv,stringv,datehourv,uuidv,ippaddrv,int128v,blobv,pointv,complexv,decimal32v,decimal64v,decimal128v);
        System.out.println("ret:"+ret);
        Assert.assertEquals("code= info=",ret.toString());
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.rows());
        System.out.println("re1："+re1.getString());
        BasicTable re2 = (BasicTable)conn2.run("select * from all_dataType");
        System.out.println("re2："+re2.rows());
        System.out.println("re1："+re2.getString());
        checkData(re2,re1);
        for(int i=0;i<28;i++){
            System.out.println(re1.getColumn(i).get(0).getString());
            if (i == 20) {
                Assert.assertEquals("0.0.0.0",re1.getColumn(i).get(0).getString());
            }else if(i==23){
                Assert.assertEquals("(,)",re1.getColumn(i).get(0).getString());
            }else{
                Assert.assertEquals("",re1.getColumn(i).get(0).getString());
            }
        }
    }

    @Test
    public void test_StreamReplicator_insert_array() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
        replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
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
    public void test_StreamReplicator_insert_array_java_type() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
        replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
//        BasicTable re = (BasicTable)conn.run("select * from Trades");
        ErrorCodeInfo ret = replicator.insert(1,
                new Boolean[]{true,null,false},
                new Byte[]{(byte)1,(byte)128,(byte)-128},
                new Short[]{1,Short.MIN_VALUE,Short.MAX_VALUE},
                new Integer[]{1,Integer.MIN_VALUE,Integer.MAX_VALUE},
                new Long[]{1l,9223372036854775807l,-9223372036854775807l},
                new Double[]{Double.valueOf(0)},
                new Float[]{Float.valueOf(0)},
                new LocalDate[]{LocalDate.of(1,1,1)},
                new LocalDate[]{LocalDate.of(2021,1,1)},
                new LocalTime[]{LocalTime.of(1,1,1,342)},
                new LocalTime[]{LocalTime.of(1,1,1,342)},
                new LocalTime[]{LocalTime.of(1,1,1,1)},
                new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654)},
                new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654)},
                new LocalTime[]{LocalTime.of(1,1,1,45364654)},
                new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654),
                LocalDateTime.of(2022,2,1,1,1,2,45364654)},
//                new String[]{"SYMBOL",null,"SYMBOL"},
//                new String[]{"SYMBOL",null,"SYMBOL"},
                new LocalDateTime[]{LocalDateTime.of(2022,2,1,1,1,2,45364654)},
                new String[]{"5d212a78-cc48-e3b1-4235-b4d91473ee87",null,"5d212a78-cc48-e3b1-4235-b4d91473ee87"},
                new String[]{"192.168.1.13",null,"192.168.1.13"},
                new String[]{"e1671797c52e15f763380b45e841ec32",null,"e1671797c52e15f763380b45e841ec32"},
//                new String[]{"BLOB",null,"BLOB"},
                new BasicPointVector(new Double2[]{new Double2(1.0,9.2),new Double2(3.8,7.4),null,new Double2(5.6,6.5)}) ,
                new BasicComplexVector(new Double2[]{new Double2(1.1,3.9)}),
                new BasicDecimal32Vector(new String[] {"0.0","-123.00432","132.204234","100.0"},4),
                new BasicDecimal64Vector(new String[] {"0.0","-123.00432","132.204234","100.0"},4),
                new BasicDecimal128Vector(new String[] {"0.0","-123.00432","132.204234","100.0"},4)
                );

        ErrorCodeInfo ret1 = replicator.insert(2,
                new Boolean[]{},
                new Byte[]{},
                new Short[]{},
                new Integer[]{},
                new Long[]{},
                new Double[]{},
                new Float[]{},
                new LocalDate[]{LocalDate.of(1965,1,1)},
                new LocalDate[]{LocalDate.of(1965,1,1)},
                new LocalTime[]{LocalTime.of(23,1,1,342)},
                new LocalTime[]{LocalTime.of(23,1,1,342)},
                new LocalTime[]{LocalTime.of(23,1,1,1)},
                new LocalDateTime[]{LocalDateTime.of(1965,2,1,1,1,2,45364654)},
                new LocalDateTime[]{LocalDateTime.of(1965,2,1,1,1,2,45364654)},
                new LocalTime[]{LocalTime.of(23,59,59,45364654)},
                new LocalDateTime[]{LocalDateTime.of(1965,2,1,1,1,2,45364654),
                LocalDateTime.of(1965,2,1,1,1,2,45364654)},
                new LocalDateTime[]{LocalDateTime.of(1965,2,1,1,1,2,45364654)},
                new String[]{},
                new String[]{},
                new String[]{},
                new BasicPointVector(1) ,
                new BasicComplexVector(1),
                new BasicDecimal32Vector(1),
                new BasicDecimal64Vector(1),
                new BasicDecimal128Vector(1)
        );
        System.out.println(ret.toString());
        System.out.println(ret1.toString());
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from Receive");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from Receive");
        System.out.println("re2："+re2.rows());
        checkData(re1,re2);
        Assert.assertEquals("[[true,,false],[]]",re1.getColumn(1).getString());
        Assert.assertEquals("[[1,,],[]]",re1.getColumn(2).getString());
        Assert.assertEquals("[[1,,32767],[]]",re1.getColumn(3).getString());
        Assert.assertEquals("[[1,,2147483647],[]]",re1.getColumn(4).getString());
        Assert.assertEquals("[[1,9223372036854775807,-9223372036854775807],[]]",re1.getColumn(5).getString());
        Assert.assertEquals("[[0],[]]",re1.getColumn(6).getString());
        Assert.assertEquals("[[0],[]]",re1.getColumn(7).getString());
        Assert.assertEquals("[[0001.01.01],[1965.01.01]]",re1.getColumn(8).getString());
        Assert.assertEquals("[[2021.01M],[1965.01M]]",re1.getColumn(9).getString());
        Assert.assertEquals("[[01:01:01.000],[23:01:01.000]]",re1.getColumn(10).getString());
        Assert.assertEquals("[[01:01m],[23:01m]]",re1.getColumn(11).getString());
        Assert.assertEquals("[[01:01:01],[23:01:01]]",re1.getColumn(12).getString());
        Assert.assertEquals("[[2022.02.01T01:01:02],[1965.02.01T01:01:02]]",re1.getColumn(13).getString());
        Assert.assertEquals("[[2022.02.01T01:01:02.045],[1965.02.01T01:01:02.045]]",re1.getColumn(14).getString());
        Assert.assertEquals("[[01:01:01.045364654],[23:59:59.045364654]]",re1.getColumn(15).getString());
        Assert.assertEquals("[[2022.02.01T01:01:02.045364654,2022.02.01T01:01:02.045364654],[1965.02.01T01:01:02.045364654,1965.02.01T01:01:02.045364654]]",re1.getColumn(16).getString());
        Assert.assertEquals("[[2022.02.01T01],[1965.02.01T01]]",re1.getColumn(17).getString());
        Assert.assertEquals("[[5d212a78-cc48-e3b1-4235-b4d91473ee87,,5d212a78-cc48-e3b1-4235-b4d91473ee87],[]]",re1.getColumn(18).getString());
        Assert.assertEquals("[[192.168.1.13,0.0.0.0,192.168.1.13],[0.0.0.0]]",re1.getColumn(19).getString());
        Assert.assertEquals("[[e1671797c52e15f763380b45e841ec32,,e1671797c52e15f763380b45e841ec32],[]]",re1.getColumn(20).getString());
        Assert.assertEquals("[[(1.0, 9.2),(3.8, 7.4),(,),(5.6, 6.5)],[(0.0, 0.0)]]",re1.getColumn(21).getString());
        Assert.assertEquals("[[1.1+3.9i],[0.0+0.0i]]",re1.getColumn(22).getString());
        Assert.assertEquals("[[0.00,-123.00,132.20,100.00],[0.00]]",re1.getColumn(23).getString());
        Assert.assertEquals("[[0.0000000,-123.0043000,132.2042000,100.0000000],[0.0000000]]",re1.getColumn(24).getString());
        Assert.assertEquals("[[0.0000000000000000000,-123.0043000000000000000,132.2042000000000000000,100.0000000000000000000],[0.0000000000000000000]]",re1.getColumn(25).getString());
    }

    @Test
    public void test_StreamReplicator_insert_array_null() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port1),100,3);
        Preparedata_streamTable_array1(HOST,Integer.parseInt(port2),100,3);
        replicator = new StreamReplicator(hostInfoList,"Receive",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from Receive");
        System.out.println("re1："+re1.rows());
        System.out.println("re1："+re1.getString());
        BasicTable re2 = (BasicTable)conn2.run("select * from Receive");
        System.out.println("re2："+re2.rows());
        checkData(re2,re1);
        for(int i=1;i<26;i++){
            System.out.println(re1.getColumn(i).get(0).getString());
            if (i == 19) {
                Assert.assertEquals("[0.0.0.0]",re1.getColumn(i).get(0).getString());
            }else if(i==21){
                Assert.assertEquals("[(,)]",re1.getColumn(i).get(0).getString());
            }else{
                Assert.assertEquals("[]",re1.getColumn(i).get(0).getString());
            }
        }
    }

    @Test
    public void test_StreamReplicator_insert_any() throws IOException {
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1:0,`any1`any2`any3,[ANY,ANY,ANY]) as dataType_any;");
        conn2.run("share table(1:0,`any1`any2`any3,[ANY,ANY,ANY]) as dataType_any;");
        String script = "cany=array(ANY,0).append!(1000).append!(`www`qqq).append!(matrix([1 2 3, 4 5 6])).append!(set(1 2)).append!(100:11).append!(table(`qa`ws`ed as id)).append!(dict(`aaa11`bbb22, [dict(`p1`p2, `1`2, true), dict(`p11`p22, `100`200, true)])).append!((100, `11)).append!( [[`1a,`a1]].setColumnarTuple!());\n" +
                "share  table(cany as any1, cany as any2, cany as any3) as data;";
        conn.run(script);
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicator = new StreamReplicator(hostInfoList,"dataType_any",replicatorConfig);
        BasicTable re = (BasicTable)conn.run("select * from data");
        for(int i=0;i<re.rows();i++){
            ErrorCodeInfo ret = replicator.insert(
                    re.getColumn(0).get(i),
                    re.getColumn(1).get(i),
                    re.getColumn(2).get(i));
        }
        replicator.waitForThreadCompletion();
        BasicTable re1 = (BasicTable)conn1.run("select * from dataType_any");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from dataType_any");
        System.out.println("re2："+re2.rows());
        System.out.println("re2："+re2.getString());
        checkData(re,re1);
        checkData(re,re2);
    }

    //数据类型转换
    @Test
    public void test_StreamReplicator_insert_data_byte() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        conn2.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert((byte)1, (byte)1, (byte)1, (byte)1, (byte)1);
        ret = replicator.insert(null, (byte)1, (byte)1, (byte)1, (byte)1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2,re1.rows());
        assertEquals("1", re1.getColumn("char").get(0).getString());
        assertEquals("", re1.getColumn("char").get(1).getString());
        assertEquals("1", re1.getColumn("int").get(0).getString());
        assertEquals("1", re1.getColumn("long").get(0).getString());
        assertEquals("1", re1.getColumn("short").get(0).getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_short() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        conn2.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert((short)1, (short)1, (short)-1, (short)0, (short)-1);
        ret = replicator.insert(null, (short)1, (short)1, (short)0, (short)1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2,re1.rows());
        assertEquals("[1,]", re1.getColumn("char").getString());
        assertEquals("[1,1]", re1.getColumn("int").getString());
        assertEquals("[-1,1]", re1.getColumn("long").getString());
        assertEquals("[0,0]", re1.getColumn("short").getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_int() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        conn2.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert((int)1, (int)1, (int)-1, (int)0, (int)-1);
        ret = replicator.insert(null, (int)1, (int)1, (int)0, (int)1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2,re1.rows());
        assertEquals("[1,]", re1.getColumn("char").getString());
        assertEquals("[1,1]", re1.getColumn("int").getString());
        assertEquals("[-1,1]", re1.getColumn("long").getString());
        assertEquals("[0,0]", re1.getColumn("short").getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_long() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        conn2.run("share table(1000:0, `char`int`long`short`id,[CHAR,INT,LONG,SHORT,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert((long)1, (long)1, (long)-1, (long)0, (long)-1);
        ret = replicator.insert((long)1, (long)1, (long)-1, (long)0, (long)-1);
        ret = replicator.insert(null, (long)1, -9223372036854775807L, (long)1, (long)1);
        ret = replicator.insert(null, (long)1, 9223372036854775807L, (long)-1, (long)1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(4,re1.rows());
        assertEquals("[1,1,,]", re1.getColumn("char").getString());
        assertEquals("[1,1,1,1]", re1.getColumn("int").getString());
        assertEquals("[-1,-1,-9223372036854775807,9223372036854775807]", re1.getColumn("long").getString());
        assertEquals("[0,0,1,-1]", re1.getColumn("short").getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_float() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `float`double`id,[FLOAT,DOUBLE,INT]) as table1;");
        conn2.run("share table(1000:0, `float`double`id,[FLOAT,DOUBLE,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(1.9f,0.2f,2);
        ret = replicator.insert(-1.90f,-0.2f,2);
        ret = replicator.insert(null,null,2);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(3,re1.rows());
        assertEquals("[1.89999998,-1.89999998,]", re1.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", re1.getColumn("double").getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_double() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `float`double`id, [FLOAT,DOUBLE,INT]) as table1;");
        conn2.run("share table(1000:0, `float`double`id, [FLOAT,DOUBLE,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(1.9,0.2,2);
        ret = replicator.insert(-1.90,-0.2,2);
        ret = replicator.insert(null,null,2);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(3,re1.rows());
        assertEquals("[1.89999998,-1.89999998,]", re1.getColumn("float").getString());
        assertEquals("[0.2,-0.2,]", re1.getColumn("double").getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_blob() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(1000:0, `blob`string`symbol`id, [BLOB,STRING,SYMBOL,INT]) as table1;");
        conn2.run("share table(1000:0, `blob`string`symbol`id, [BLOB,STRING,SYMBOL,INT]) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        String blob=conn1.run("n=10;t = table(1..n as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,n) as name1);"+
                "t.toStdJson()").getString();
        BasicString blob1 = new BasicString(blob,true);
        ErrorCodeInfo ret = replicator.insert(blob1,blob1,blob1,1);
        replicator.waitForThreadCompletion();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(1,re1.rows());
        assertEquals(blob, re1.getColumn("blob").get(0).getString());
        assertEquals(blob, re1.getColumn("string").get(0).getString());
        assertEquals(blob, re1.getColumn("symbol").get(0).getString());
        checkData(re1,re2);
    }

    @Test
    public void test_StreamReplicator_insert_keyTable() throws IOException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        Prepare_keyTable(HOST,Integer.parseInt(port1),"all_dataType");
        Prepare_keyTable(HOST,Integer.parseInt(port2),"all_dataType");
        replicator = new StreamReplicator(hostInfoList,"all_dataType",replicatorConfig);
        Preparedata_keyTable(10000);
        BasicTable re = (BasicTable)conn.run("select * from t1");
        System.out.println("re.columns()"+re.columns());
        for(int i=0;i<10000;i++){
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
                    re.getColumn(25).get(i),
                    re.getColumn(26).get(i),
                    re.getColumn(27).get(i),
                    re.getColumn(28).get(i)
                   );
        }
        replicator.waitForThreadCompletion();
        replicator.waitForThreadCompletion();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        BasicTable re1 = (BasicTable)conn1.run("select * from all_dataType");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from all_dataType");
        System.out.println("re2："+re2.rows());
        checkData(re,re1);
        checkData(re,re2);
    }

    @Test
    public void test_StreamReplicator_insert_data_less_then_batchsize_time_more_then_batchInterval() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setBatching(10000,1, TimeUnit.MILLISECONDS);

        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
//        replicator.waitForThreadCompletion();
        System.out.println(replicator.getStatus().toString());
        sleep(2000);
        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re2.rows());
        Assert.assertEquals(2000,re1.rows());
        Assert.assertEquals(2000,re2.rows());
    }
    @Test
    public void test_StreamReplicator_insert_data_more_then_batchsize_time_less_then_batchInterval() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setBatching(1000,10, TimeUnit.SECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
//        replicator.waitForThreadCompletion();
        System.out.println(replicator.getStatus().toString());
        sleep(2000);
        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re2.rows());
        Assert.assertEquals(2000,re1.rows());
        Assert.assertEquals(2000,re2.rows());
    }
    @Test
    public void test_StreamReplicator_insert_data_more_then_batchsize_time_more_then_batchInterval() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setBatching(1000,1, TimeUnit.MILLISECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
//        replicator.waitForThreadCompletion();
        sleep(2000);
        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re2.rows());
        Assert.assertEquals(2000,re1.rows());
        Assert.assertEquals(2000,re2.rows());
    }

    @Test
    public void test_StreamReplicator_insert_data_less_then_batchsize_time_less_then_batchInterval() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setBatching(10000,5, TimeUnit.SECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
//        replicator.waitForThreadCompletion();
        sleep(2000);
        System.out.println(replicator.getStatus().toString());

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re1.rows());
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re2.rows());
        Assert.assertEquals(0,re1.rows());
        Assert.assertEquals(0,re2.rows());
    }

    @Test
    public void test_StreamReplicator_host_first_not_connect_retry() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRetry(5,1000,TimeUnit.MILLISECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        List<HostInfo> hostInfoList1 = new ArrayList<>();
        HostInfo host1 = new HostInfo(HOST,8888,"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,8889,"admin","123456","label2");
        hostInfoList1.add(host1);
        hostInfoList1.add(host2);
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        String re = null;
        try{
            replicator = new StreamReplicator(hostInfoList1,"table1",replicatorConfig);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Failed to connect to host 'label1'.",re);
    }

    @Test
    public void test_StreamReplicator_succeed() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRetry(2,10000,TimeUnit.MILLISECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        ErrorCodeInfo ret = replicator.insert(1,1,1);
        System.out.println();
        Assert.assertEquals(true, ret.succeed());
    }

    @Test
    public void test_StreamReplicator_close() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        System.out.println(replicator.getStatus().toString());
        replicator.close();
        replicator.close();

        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2000, re1.rows());
        Assert.assertEquals(2000, re2.rows());

        //关闭之后，重写写入数据无法写入成功
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        replicator.waitForThreadCompletion();
        BasicTable re11 = (BasicTable)conn1.run("select * from table1");
        BasicTable re22 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2000, re11.rows());
        Assert.assertEquals(2000, re22.rows());
    }

    @Test
    public void test_StreamReplicator_waitForThreadCompletion() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        replicator.waitForThreadCompletion();
        BasicTable re1 = (BasicTable)conn1.run("select * from table1");
        BasicTable re2 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2000, re1.rows());
        Assert.assertEquals(2000, re2.rows());

        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        replicator.waitForThreadCompletion();
        BasicTable re11 = (BasicTable)conn1.run("select * from table1");
        BasicTable re22 = (BasicTable)conn2.run("select * from table1");
        Assert.assertEquals(2000, re11.rows());
        Assert.assertEquals(2000, re22.rows());
    }

    @Test
    public void test_StreamReplicator_getStatus() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        Assert.assertEquals(0, replicator.getStatus().getTotalRows());
        Assert.assertEquals(0, replicator.getStatus().getTotalDumpedRows());
        Assert.assertEquals(0, replicator.getStatus().getTotalPendingRows());
        Assert.assertEquals("ReplicatorStreamStatus{inserted=0, dumped=0, pending=0, errorCode='', errorInfo=''}", replicator.getStatus().getHostStatus("label1").toString());
        Assert.assertEquals(0, replicator.getStatus().getHostStatus("label1").getInsertedRows());
        Assert.assertEquals(0, replicator.getStatus().getHostStatus("label1").getDumpedRows());
        Assert.assertEquals(0, replicator.getStatus().getHostStatus("label1").getPendingRows());
        Assert.assertEquals("", replicator.getStatus().getHostStatus("label1").getErrorInfo());
        Assert.assertEquals("", replicator.getStatus().getHostStatus("label1").getErrorCode());
        Assert.assertEquals(false, replicator.getStatus().getHostStatus("label1").hasError());

        for(int i=0;i<2000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        System.out.println(replicator.getStatus().toString());
        Assert.assertEquals(4000, replicator.getStatus().getTotalRows());
        Assert.assertEquals(4000, replicator.getStatus().getTotalDumpedRows()+replicator.getStatus().getTotalPendingRows());
        Assert.assertEquals(2000, replicator.getStatus().getHostStatus("label1").getDumpedRows()+replicator.getStatus().getHostStatus("label1").getPendingRows());
        Assert.assertEquals(2000, replicator.getStatus().getHostStatus("label1").getInsertedRows());
        Assert.assertEquals("", replicator.getStatus().getHostStatus("label1").getErrorInfo());
        Assert.assertEquals("", replicator.getStatus().getHostStatus("label1").getErrorCode());
        Assert.assertEquals(false, replicator.getStatus().getHostStatus("label1").hasError());

        replicator.waitForThreadCompletion();
        Assert.assertEquals(2000, replicator.getStatus().getHostStatus("label1").getDumpedRows());
        Assert.assertEquals(0, replicator.getStatus().getHostStatus("label1").getPendingRows());
    }

    @Test
    public void test_StreamReplicator_insert_200000() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setBatching(10000,1,TimeUnit.SECONDS);
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        long startTime = System.currentTimeMillis();
        for(int i=0;i<200000;i++){
            ErrorCodeInfo ret = replicator.insert(1,1,1);
        }
        replicator.waitForThreadCompletion();
        long completeTime1 = System.currentTimeMillis();
        long tcompleteTime = completeTime1 - startTime;
        System.out.println(tcompleteTime);
        BasicTable re11 = (BasicTable)conn1.run("select * from table1");
        System.out.println("re1："+re11.rows());
        BasicTable re22 = (BasicTable)conn2.run("select * from table1");
        System.out.println("re2："+re22.rows());
    }

    //@Test //多线程并发 会影响其他案例
    public void test_StreamReplicator_mul_thread_10() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        // 3. 打印初始状态（可选）
        System.out.println("初始状态：" + replicator.getStatus().toString());

        // 4. 定义并发写入的总行数
        int totalInserts = 200000;

        // 5. 使用线程池并发执行插入操作
        int threadCount = 10; // 可调整为 5、10、20 等，模拟多线程
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        CountDownLatch latch = new CountDownLatch(totalInserts); // 用于等待所有插入完成

        for (int i = 0; i < totalInserts; i++) {
            executor.execute(() -> {
                try {
                    ErrorCodeInfo ret = replicator.insert(1, 1, 1);
                    // 可以打印部分日志，但注意不要太多，避免影响性能测试
                    System.out.println("[线程 " + Thread.currentThread().getName() + "] Insert OK");
                } catch (Exception e) {
                    System.err.println("Insert failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown(); // 每完成一次插入，计数器减一
                }
            });
        }

        // 6. 等待所有插入任务完成
        latch.await(); // 阻塞，直到所有 insert 操作完成（ 次）
        System.out.println("所有线程已完成插入操作");
        System.out.println(replicator.getStatus().toString());
        // 7. 等待 replicator 内部队列数据全部写入目标节点
        replicator.waitForThreadCompletion();
        System.out.println("所有数据已写入目标节点，等待完成");
        // 8. 查询两个节点的 table1，验证数据是否写入成功
        BasicTable re1 = (BasicTable) conn1.run("select * from table1");
        System.out.println("节点1（port1）table1 行数: " + re1.rows());

        BasicTable re2 = (BasicTable) conn2.run("select * from table1");
        System.out.println("节点2（port2）table1 行数: " + re2.rows());
        Assert.assertEquals(200000,re1.rows());
        Assert.assertEquals(200000,re2.rows());
    }

    //@Test 多线程并发 会影响其他案例
    public void test_StreamReplicator_mul_thread_100() throws IOException, InterruptedException {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        conn1.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        conn2.run("share table(array(INT) as col1,array(INT) as col2,array(INT) as col3) as table1;");
        replicator = new StreamReplicator(hostInfoList,"table1",replicatorConfig);
        System.out.println(replicator.getStatus().toString());
        // 3. 打印初始状态（可选）
        System.out.println("初始状态：" + replicator.getStatus().toString());

        // 4. 定义并发写入的总次数
        int totalInserts = 200000;

        // 5. 使用线程池并发执行插入操作
        int threadCount = 100; // 可调整为 5、10、20 等，模拟多线程
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        CountDownLatch latch = new CountDownLatch(totalInserts); // 用于等待所有插入完成
        for (int i = 0; i < totalInserts; i++) {
            executor.execute(() -> {
                try {
                    ErrorCodeInfo ret = replicator.insert(1, 1, 1);
                    // 可以打印部分日志，但注意不要太多，避免影响性能测试
                    System.out.println("[线程 " + Thread.currentThread().getName() + "] Insert OK");
                } catch (Exception e) {
                    System.err.println("Insert failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown(); // 每完成一次插入，计数器减一
                }
            });
        }
        // 6. 等待所有插入任务完成
        latch.await(); // 阻塞，直到所有 insert 操作完成（2000 次）
        System.out.println("所有线程已完成插入操作");
        System.out.println(replicator.getStatus().toString());
        // 7. 等待 replicator 内部队列数据全部写入目标节点
        replicator.waitForThreadCompletion();

        System.out.println("所有数据已写入目标节点，等待完成");

        // 8. 查询两个节点的 table1，验证数据是否写入成功
        BasicTable re1 = (BasicTable) conn1.run("select * from table1");
        System.out.println("节点1（port1）table1 行数: " + re1.rows());

        BasicTable re2 = (BasicTable) conn2.run("select * from table1");
        System.out.println("节点2（port2）table1 行数: " + re2.rows());
        Assert.assertEquals(200000,re1.rows());
        Assert.assertEquals(200000,re2.rows());
    }

    @Test//写入中途其中一个节点断连
    public void test_StreamReplicator_one_node_stop() throws IOException, InterruptedException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection contro = new DBConnection();
        contro.connect(HOST,CONTROLLER_PORT,"admin","123456");
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "colNames = `shortv`intv`longv;\n" +
                "colTypes=[SHORT,INT,LONG]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" ;
        conn1.run(script);
        conn2.run(script);
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        BasicTable data1 = (BasicTable)conn1.run("data");
        BasicTable data2 = (BasicTable)conn1.run("data");
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRetry(3,1,TimeUnit.SECONDS);
        // 设置一个自定义的连接状态变化回调
        replicatorConfig.onDataDump((hostLabel, table) -> {
            BasicShortVector shortv = (BasicShortVector) table.getColumn(0);
            BasicIntVector intv = (BasicIntVector) table.getColumn(1);
            BasicLongVector longv = (BasicLongVector) table.getColumn(2);
            if(hostLabel=="label1"){
                try {
                    data1.getColumn(0).Append(shortv);
                    data1.getColumn(1).Append(intv);
                    data1.getColumn(2).Append(longv);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println("onDataDump called for host: " + hostLabel);
                System.out.println(hostLabel+":"+table.getString());
                // 返回 true 表示继续
            }else{
                try {
                    data2.getColumn(0).Append(shortv);
                    data2.getColumn(1).Append(intv);
                    data2.getColumn(2).Append(longv);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println("onDataDump called for host: " + hostLabel);
                System.out.println(hostLabel+":"+table.getString());
                // 返回 true 表示继续
            }
            return true;
        });
        BasicTable data = (BasicTable)conn1.run("data");

        Assert.assertEquals(true,replicatorConfig.getOnDataDump().onDump("label1",data));
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"data",replicatorConfig);

        for (int i = 1; i <= 100; i++){
            ErrorCodeInfo ret = replicator.insert(i,i,i);
            sleep(100);

            //数据写入一半 其中一个节点停止
            if(i==50){
                try{
                    contro.run("stopDataNode([\""+HOST+":"+Integer.parseInt(port1)+"\"])");
                    sleep(5000);
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }
        }
        //节点重启
        try{
            contro.run("startDataNode([\""+HOST+":"+Integer.parseInt(port1)+"\"])");
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        sleep(3000);
        System.out.println("data1.rows()"+data1.rows());
        System.out.println(data1.getString());
        Assert.assertEquals(50, data1.rows());
        Assert.assertEquals(0, data2.rows());
        BasicTable ex = (BasicTable)contro.run("tmp=table(51..100 as shortv,51..100 as intv,51..100 as longv);tmp;");
        checkData(ex,data1);

        //节点重新启动，再次写入数据
        conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        conn1.run(script);

        for (int i = 101; i <= 110; i++){
            ErrorCodeInfo ret = replicator.insert(i,i,i);
            sleep(100);
        }
//        System.out.println(replicator.getStatus());
        Assert.assertEquals("A5",replicator.getStatus().getHostStatus("label1").getErrorCode());
        Assert.assertEquals(true,replicator.getStatus().getHostStatus("label1").getErrorInfo().contains("Failed to write batch to host 'label1' (attempt 3/3):"));
        Assert.assertEquals("",replicator.getStatus().getHostStatus("label2").getErrorCode());
        Assert.assertEquals("",replicator.getStatus().getHostStatus("label2").getErrorInfo());

        sleep(3000);
        System.out.println("data1.rows()"+data1.rows());
        System.out.println(data1.getString());
        Assert.assertEquals(60, data1.rows());
        Assert.assertEquals(0, data2.rows());
        BasicTable ex1 = (BasicTable)contro.run("tmp=table(51..110 as shortv,51..110 as intv,51..110 as longv);tmp;");
        checkData(ex1,data1);
    }

    @Test//写入中途两个节点断连
    public void test_StreamReplicator_two_nodes_stop() throws IOException, InterruptedException {
        List<HostInfo> hostInfoList = new ArrayList<>();
        String port1 = ipports[0].split(":")[1];
        String port2 = ipports[1].split(":")[1];
        DBConnection contro = new DBConnection();
        contro.connect(HOST,CONTROLLER_PORT,"admin","123456");
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        DBConnection conn2 = new DBConnection();
        conn2.connect(HOST,Integer.parseInt(port2),"admin","123456");
        String script = "colNames = `shortv`intv`longv;\n" +
                "colTypes=[SHORT,INT,LONG]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" ;
        conn1.run(script);
        conn2.run(script);
        HostInfo host1 = new HostInfo(HOST,Integer.parseInt(port1),"admin","123456","label1");
        HostInfo host2 = new HostInfo(HOST,Integer.parseInt(port2),"admin","123456","label2");
        hostInfoList.add(host1);
        hostInfoList.add(host2);
        BasicTable data1 = (BasicTable)conn1.run("data");
        BasicTable data2 = (BasicTable)conn1.run("data");
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRetry(3,1,TimeUnit.SECONDS);
        // 设置一个自定义的连接状态变化回调
        replicatorConfig.onDataDump((hostLabel, table) -> {
            BasicShortVector shortv = (BasicShortVector) table.getColumn(0);
            BasicIntVector intv = (BasicIntVector) table.getColumn(1);
            BasicLongVector longv = (BasicLongVector) table.getColumn(2);
            if(hostLabel=="label1"){
                try {
                    data1.getColumn(0).Append(shortv);
                    data1.getColumn(1).Append(intv);
                    data1.getColumn(2).Append(longv);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println("onDataDump called for host: " + hostLabel);
                System.out.println(hostLabel+":"+table.getString());
                // 返回 true 表示继续
            }else{
                try {
                    data2.getColumn(0).Append(shortv);
                    data2.getColumn(1).Append(intv);
                    data2.getColumn(2).Append(longv);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println("onDataDump called for host: " + hostLabel);
                System.out.println(hostLabel+":"+table.getString());
                // 返回 true 表示继续
            }
            return true;
        });
        BasicTable data = (BasicTable)conn1.run("data");

        Assert.assertEquals(true,replicatorConfig.getOnDataDump().onDump("label1",data));
        StreamReplicator replicator = new StreamReplicator(hostInfoList,"data",replicatorConfig);

        for (int i = 1; i <= 100; i++){
            ErrorCodeInfo ret = replicator.insert(i,i,i);
            sleep(100);

            //数据写入一半 其中一个节点停止
            if(i==50){
                try{
                    contro.run("stopDataNode([\""+HOST+":"+Integer.parseInt(port1)+"\"])");
                    contro.run("stopDataNode([\""+HOST+":"+Integer.parseInt(port2)+"\"])");
                    sleep(5000);
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }
        }
        //节点重启
        try{
            contro.run("startDataNode([\""+HOST+":"+Integer.parseInt(port1)+"\"])");
            contro.run("startDataNode([\""+HOST+":"+Integer.parseInt(port2)+"\"])");
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        sleep(3000);
        System.out.println("data1.rows()"+data1.rows());
        System.out.println(data1.getString());
        Assert.assertEquals(50, data1.rows());
        Assert.assertEquals(50, data2.rows());
        BasicTable ex = (BasicTable)contro.run("tmp=table(51..100 as shortv,51..100 as intv,51..100 as longv);tmp;");
        checkData(ex,data1);
        checkData(ex,data2);
        //节点重新启动，再次写入数据
        conn1 = new DBConnection();
        conn1.connect(HOST,Integer.parseInt(port1),"admin","123456");
        conn1.run(script);

        for (int i = 101; i <= 110; i++){
            ErrorCodeInfo ret = replicator.insert(i,i,i);
            sleep(100);
        }
//        System.out.println(replicator.getStatus());
        Assert.assertEquals("A5",replicator.getStatus().getHostStatus("label1").getErrorCode());
        Assert.assertEquals(true,replicator.getStatus().getHostStatus("label1").getErrorInfo().contains("Failed to write batch to host 'label1' (attempt 3/3):"));
        Assert.assertEquals("A5",replicator.getStatus().getHostStatus("label2").getErrorCode());
        Assert.assertEquals(true,replicator.getStatus().getHostStatus("label2").getErrorInfo().contains("Failed to write batch to host 'label2' (attempt 3/3):"));

        sleep(3000);
        System.out.println("data1.rows()"+data1.rows());
        System.out.println(data1.getString());
        Assert.assertEquals(60, data1.rows());
        Assert.assertEquals(60, data2.rows());
        BasicTable ex1 = (BasicTable)contro.run("tmp=table(51..110 as shortv,51..110 as intv,51..110 as longv);tmp;");
        checkData(ex1,data1);
        checkData(ex1,data2);
    }
}
