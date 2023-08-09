package com.xxdb.data;

import com.xxdb.*;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.route.PartitionedTableAppender;
import com.xxdb.route.AutoFitTableAppender;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.PollingClient;
import com.xxdb.streaming.client.TopicPoller;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;

public class BasicDecimalTest {
    private Logger logger_ = Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    public static Integer insertTime = 5000;
    public static ErrorCodeInfo pErrorInfo =new ErrorCodeInfo();;

    //private final int id;
    private static MultithreadedTableWriter mutithreadTableWriter_ = null;

    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
                "}");
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
    }
    @Test
    public void test_BasicDecimal_readMemoryTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t = table(1:0,`a`b`c,[INT,DECIMAL32(2),DECIMAL64(4)]);\n"+
                "t.append!(table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c));\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t;");
        BasicDecimal32 bd32 = new BasicDecimal32(0,4);
        //BasicDecimal64 bd64 = new BasicDecimal64(0,7L);
        System.out.println(bd32.getDataType());
        //assertEquals(bd64,bt.getColumn(2).get(0));
        //assertEquals(bd32,bt.getColumn(1).get(0));
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn(1).get(0).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn(2).get(0).getDataType());
    }
    @Test
    public void test_BasicDecimal_readPartitionTable() throws IOException{
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        BasicTable bt = (BasicTable) conn.run("select top 100 * from pt;");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        assertEquals(100,bt.rows());
    }

    @Test
    public void test_BasicDecimal_readDimensionTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "dt = db.createTable(t,`dt);\n" +
                "dt.append!(t);" +
                "dt = loadTable(\"dfs://testDecimal\",\"dt\");";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select top 50 * from dt;");
        assertEquals(50,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
    }

    @Test
    public void test_BasicDecimal_readStreamingTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(1000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL32(2),DECIMAL64(4)]);" +
                "rt.append!(table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci))" +
                "share rt as read";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from read;");
        assertEquals(1000,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("ci").getDataType());
        conn.run("undef(`read,SHARED)");
    }

    @Test
    public void test_BasicDecimal_normalWriteMemoryTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("it=table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c)");
        BasicTable bt = (BasicTable) conn.run("it");
        conn.run("t = table(6:0,`a`b`c,[INT,DECIMAL32(2),DECIMAL64(4)]);" +
                "share t as st");
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{st}",args);
        BasicTable res = (BasicTable) conn.run("t;");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,res.getColumn("b").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,res.getColumn("c").getDataType());
        conn.run("insert into t values(10,20,50);");
        BasicTable resn = (BasicTable) conn.run("t;");
        assertNotNull(resn.getRowJson(3));
        assertEquals("{\"a\":10,\"b\":20.00,\"c\":50.0000}",resn.getRowJson(3));
        assertEquals(4,resn.rows());
    }

    @Test
    public void test_BasicDecimal_normalWriteStreamingTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(2000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL32(2),DECIMAL64(4)]);" +
                "share rt as read;";
        conn.run(script);
        conn.run("it=table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci)");
        BasicTable bt = (BasicTable) conn.run("it");
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{read}",args);
        BasicTable res = (BasicTable) conn.run("read;");
        assertEquals(1000,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,res.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,res.getColumn("ci").getDataType());
        conn.run("insert into read values(`XM,73,274,366);");
        BasicTable resn = (BasicTable) conn.run("read;");
        assertNotNull(resn.getRowJson(1000));
        //assertEquals("{a:10,b:20.00,c:50.0000}",resn.getRowJson(3));
        assertEquals(1001,resn.rows());
        conn.run("undef(`read,SHARED);");
    }

    @Test
    public void test_BasicDecimal_normalWritePartitionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        conn.run("it=table(76 as id,102 as a,503 as b);");
        BasicTable bt = (BasicTable) conn.run("it;");
        List<Entity> args = new ArrayList<>(1);
        args.add(bt);
        String dpPath = "dfs://testDecimal";
        conn.run(String.format("tableInsert{loadTable('%s','pt')}",dpPath),args);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testDecimal\",\"pt\") where a=102.00");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,res.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,res.getColumn("b").getDataType());
        assertEquals("102.00",res.getColumn("a").get(0).getString());
        assertEquals("[503.0000]",res.getColumn("b").getString());
    }

    @Test
    public void test_BasicDecimal_normalWriteDimensionTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createTable(t,`pt);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        conn.run("it=table(76 as id,102 as a,503 as b);");
        BasicTable bt = (BasicTable) conn.run("it;");
        List<Entity> args = new ArrayList<>(1);
        args.add(bt);
        String dpPath = "dfs://testDecimal";
        conn.run(String.format("tableInsert{loadTable('%s','pt')}",dpPath),args);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testDecimal\",\"pt\") where a=102.00");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,res.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,res.getColumn("b").getDataType());
        assertEquals("102.00",res.getColumn("a").get(0).getString());
        assertEquals("[503.0000]",res.getColumn("b").getString());
    }

    @Test
    public void test_BasicDecimal_PoolReadPartitionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,true,true);
        BasicDBTask task = new BasicDBTask("select * from loadTable(\"dfs://testDecimal\",\"pt\");");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
    }

    @Test
    public void test_BasicDecimal_PoolReadDimensionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "dt = db.createTable(t,`dt);\n" +
                "dt.append!(t);" +
                "dt = loadTable(\"dfs://testDecimal\",\"dt\");";
        conn.run(script);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,true,true);
        BasicDBTask task = new BasicDBTask("select * from loadTable(\"dfs://testDecimal\",\"dt\");");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
    }

    @Test
    public void test_BasicDecimal_PoolReadStreamingTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(1000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL32(2),DECIMAL64(4)]);" +
                "rt.append!(table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci))" +
                "share rt as read";
        conn.run(script);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        BasicDBTask task = new BasicDBTask("select * from read;");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("ci").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
        conn.run("undef(`read,SHARED)");
    }

    @Test
    public void test_BasicDecimal_ConnectionPool_writePartitionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,int(1..10) as sym,decimal32(1..10,2) as str)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",RANGE,int(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://demohash\",\"pt\")");
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash","pt", "sym",pool);
        List<String> colNames = new ArrayList<String>(3);
        colNames.add("date");
        colNames.add("sym");
        colNames.add("str");
        List<Vector> cols = new ArrayList<Vector>(3);
        BasicTimestampVector date = new BasicTimestampVector(12);
        for (int i =0 ;i<12;i++)
            date.setTimestamp(i, LocalDateTime.now());
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(12);
        for (int i =0 ;i<12;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i + 1, 2);
            sym.setInt(i + 2, 3);
            sym.setInt(i + 3, 4);
        }
        cols.add(sym);
        BasicDecimal32Vector bdv32 = new BasicDecimal32Vector(12);
        for (int i =0 ;i<12;i++) {
            bdv32.set(i,new BasicDecimal32(i,0));
        }
        cols.add(bdv32);
        Map<String,Entity> map = new HashMap<>();
        map.put("TTable",new BasicTable(colNames,cols));
        conn.upload(map);
        BasicTable bt2 = (BasicTable) conn.run("TTable;");
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(bt2);
            assertEquals(12,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(12000,re.getLong());
        pool.shutdown();
    }
    public void checkData(BasicTable exception, BasicTable resTable) {
        assertEquals(exception.rows(), resTable.rows());
        for (int i = 0; i < exception.columns(); i++) {
            System.out.println("col" + resTable.getColumnName(i));
            assertEquals(exception.getColumn(i).getString(), resTable.getColumn(i).getString());
        }

    }
    @Test
    public void test_BasicDecimal_MultiThreadWriteMemoryTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`intX`date,[DECIMAL32(2),DECIMAL64(4),DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", true, false, null, 100000, 1000,
                1, "date");
        for (int i = 0; i < 900; i++) {
            mutithreadTableWriter_.insert(new BasicDecimal32(i,2),new BasicDecimal64((long) i,4), new Date());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(900,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("int").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("intX").getDataType());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_BasicDecimal_MultiThreadWriteStreamTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`vol`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, DECIMAL32(2), DECIMAL64(4), DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`vol`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, DECIMAL32(2), DECIMAL64(4), DOUBLE])\n;share tt as trades;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades", false, false, null, 1000, 1,
                20, "volume");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDecimal32(i % 10,2));
            row.add(new BasicDecimal64((long) (i%100),4));
            row.add(new BasicDouble(i + 0.1));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1,
                    new BasicDecimal32(i % 10,2), new BasicDecimal64((long) (i%100),4), i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("volume").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("vol").getDataType());
        conn.run("undef(`t1,SHARED);");
        conn.run("undef(`trades,SHARED);");
    }

    @Test(timeout = 600000)
    public void test_BasicDecimal_MultiThreadWritePartitionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1048576;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "share t as t1;\n"+
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://testDecimal", "pt", false, false, null, 1000, 1,
                20, "id");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(i%100));
            row.add(new BasicDecimal32(i,2));
            row.add(new BasicDecimal64((long)i,4));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicInt(i%100),new BasicDecimal32(i,2),new BasicDecimal64((long)i,4));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt order by id");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by id;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        conn.run("undef(`t1,SHARED);");
    }

    @Test(timeout = 600000)
    public void test_BasicDecimal_MultiThreadedWriteDimensionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1048576;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "share t as t1;\n"+
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createTable(t,`pt);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://testDecimal", "pt", false, false, null, 1000, 1,
                1, "id");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(i%100));
            row.add(new BasicDecimal32(i,2));
            row.add(new BasicDecimal64((long)i,4));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicInt(i%100),new BasicDecimal32(i,2),new BasicDecimal64((long)i,4));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt order by id");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by id;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        conn.run("undef(`t1,SHARED);");
    }

    @Test
    public void test_BasicDecimal32_compareTo(){
        BasicDecimal32 bd1 = new BasicDecimal32(15,2);
        BasicDecimal32 bd2 = new BasicDecimal32(15,2);
        assertEquals(0,bd1.compareTo(bd2));
        assertEquals(0,bd2.compareTo(bd1));
        BasicDecimal32 bd3 = new BasicDecimal32(15,3);
        assertEquals(0,bd1.compareTo(bd3));
        BasicDecimal32 bd4 = new BasicDecimal32(17,3);
        assertEquals(-1,bd1.compareTo(bd4));
        assertEquals(1,bd4.compareTo(bd1));
    }

    @Test
    public void test_BasicDecimal64_compareTo(){
        BasicDecimal64 bd1 = new BasicDecimal64(15L,2);
        BasicDecimal64 bd2 = new BasicDecimal64(15L,2);
        assertEquals(0,bd1.compareTo(bd2));
        assertEquals(0,bd2.compareTo(bd1));
        BasicDecimal64 bd3 = new BasicDecimal64(15L,3);
        assertEquals(0,bd1.compareTo(bd3));
        BasicDecimal64 bd4 = new BasicDecimal64(17L,3);
        assertEquals(-1,bd1.compareTo(bd4));
        assertEquals(1,bd4.compareTo(bd1));
    }

    @Test
    public void test_BasicDecimal32_method() throws Exception{
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        BasicDecimal32Vector b32v1 = (BasicDecimal32Vector) connection.run("decimal32(1..10, 4)");
        BasicDecimal32Vector b32v2 = (BasicDecimal32Vector) connection.run("decimal32(1..5, 4)");
        b32v1.combine(b32v2);
        Vector b1 = b32v1.getSubVector(new int[]{1,3,4});
        assertFalse(b32v1.isNull(1));
        b32v1.setNull(1);
        assertTrue(b32v1.isNull(1));
        BasicDecimal32 b32 = new BasicDecimal32(11, 4);
        b32.setNull();
        b32v1.set(2, b32);
        assertTrue(b32v1.isNull(2));
        BasicDecimal32 bd = new BasicDecimal32(1, 2);
        try {
            b32v1.set(0, b32);
        }catch (Exception e){
            assertEquals("Value's scale is not the same as the vector's!", e.getMessage());
        }
    }

    @Test
    public void test_BasicDecimal64_method() throws Exception{
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        BasicDecimal64Vector b64v1 = (BasicDecimal64Vector) connection.run("decimal64(1..10, 4)");
        BasicDecimal64Vector b64v2 = (BasicDecimal64Vector) connection.run("decimal64(1..5, 4)");
        b64v1.combine(b64v2);
        Vector b1 = b64v1.getSubVector(new int[]{1,3,4});
        assertFalse(b64v1.isNull(1));
        b64v1.setNull(1);
        assertTrue(b64v1.isNull(1));
        BasicDecimal64 b64 = new BasicDecimal64(11L, 4);
        b64.setNull();
        b64v1.set(2, b64);
        assertTrue(b64v1.isNull(2));
        BasicDecimal64 bd = new BasicDecimal64(1L, 2);
        try {
            b64v1.set(0, b64);
        }catch (Exception e){
            assertEquals("Value's scale is not the same as the vector's!", e.getMessage());
        }
    }

    @Test
    public void test_BasicDecimal_readMemoryTable_decimal128() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t = table(1:0,`a`b`c,[INT,DECIMAL128(2),DECIMAL128(4)]);\n"+
                "t.append!(table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c));\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t;");
        BasicDecimal128 bd32 = new BasicDecimal128("4",2);
        BasicDecimal128 bd64 = new BasicDecimal128("7",4);
        System.out.println(bd32.getDataType());
        assertEquals(bd64.getString(),bt.getColumn(2).get(0).getString());
        assertEquals(bd32.getString(),bt.getColumn(1).get(0).getString());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn(1).get(0).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn(2).get(0).getDataType());
    }
    @Test
    public void test_BasicDecimal_readPartitionTable_decimal128() throws IOException{
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        BasicTable bt = (BasicTable) conn.run("select top 100 * from pt;");
        assertEquals(100,bt.rows());
    }
    @Test
    public void test_BasicDecimal_readDimensionTable_decimal128() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "dt = db.createTable(t,`dt);\n" +
                "dt.append!(t);" +
                "dt = loadTable(\"dfs://testDecimal\",\"dt\");";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select top 50 * from dt;");
        assertEquals(50,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
    }

    @Test
    public void test_BasicDecimal_readStreamingTable_decimal128() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(1000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL128(2),DECIMAL128(4)]);" +
                "rt.append!(table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci))" +
                "share rt as read";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from read;");
        assertEquals(1000,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("ci").getDataType());
        conn.run("undef(`read,SHARED)");
    }

    @Test
    public void test_BasicDecimal_normalWriteMemoryTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("it=table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c)");
        BasicTable bt = (BasicTable) conn.run("it");
        conn.run("t = table(6:0,`a`b`c,[INT,DECIMAL128(2),DECIMAL128(4)]);" +
                "share t as st");
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{st}",args);
        BasicTable res = (BasicTable) conn.run("t;");
        assertEquals(3,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("b").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("c").getDataType());
        conn.run("insert into t values(10,20,50);");
        BasicTable resn = (BasicTable) conn.run("t;");
        assertNotNull(resn.getRowJson(3));
        assertEquals("{\"a\":10,\"b\":20.00,\"c\":50.0000}",resn.getRowJson(3));
        assertEquals(4,resn.rows());
    }

    @Test
    public void test_BasicDecimal_normalWriteStreamingTable_decimal128() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(2000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL128(2),DECIMAL128(4)]);" +
                "share rt as read;";
        conn.run(script);
        conn.run("it=table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci)");
        BasicTable bt = (BasicTable) conn.run("it");
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{read}",args);
        BasicTable res = (BasicTable) conn.run("read;");
        assertEquals(1000,res.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("ci").getDataType());
        conn.run("insert into read values(`XM,73,274,366);");
        BasicTable resn = (BasicTable) conn.run("read;");
        assertNotNull(resn.getRowJson(1000));
        //assertEquals("{a:10,b:20.00,c:50.0000}",resn.getRowJson(3));
        assertEquals(1001,resn.rows());
        conn.run("undef(`read,SHARED);");
    }

    @Test
    public void test_BasicDecimal_normalWritePartitionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        conn.run("it=table(76 as id,102 as a,503 as b);");
        BasicTable bt = (BasicTable) conn.run("it;");
        List<Entity> args = new ArrayList<>(1);
        args.add(bt);
        String dpPath = "dfs://testDecimal";
        conn.run(String.format("tableInsert{loadTable('%s','pt')}",dpPath),args);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testDecimal\",\"pt\") where a=102.00");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("b").getDataType());
        assertEquals("102.00",res.getColumn("a").get(0).getString());
        assertEquals("[503.0000]",res.getColumn("b").getString());
    }

    @Test
    public void test_BasicDecimal_normalWriteDimensionTable_decimal128() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createTable(t,`pt);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        conn.run("it=table(76 as id,102 as a,503 as b);");
        BasicTable bt = (BasicTable) conn.run("it;");
        List<Entity> args = new ArrayList<>(1);
        args.add(bt);
        String dpPath = "dfs://testDecimal";
        conn.run(String.format("tableInsert{loadTable('%s','pt')}",dpPath),args);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testDecimal\",\"pt\") where a=102.00");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,res.getColumn("b").getDataType());
        assertEquals("102.00",res.getColumn("a").get(0).getString());
        assertEquals("[503.0000]",res.getColumn("b").getString());
    }

    @Test
    public void test_BasicDecimal_PoolReadPartitionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,true,true);
        BasicDBTask task = new BasicDBTask("select * from loadTable(\"dfs://testDecimal\",\"pt\");");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
    }

    @Test
    public void test_BasicDecimal_PoolReadDimensionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "dt = db.createTable(t,`dt);\n" +
                "dt.append!(t);" +
                "dt = loadTable(\"dfs://testDecimal\",\"dt\");";
        conn.run(script);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,true,true);
        BasicDBTask task = new BasicDBTask("select * from loadTable(\"dfs://testDecimal\",\"dt\");");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
    }

    @Test
    public void test_BasicDecimal_PoolReadStreamingTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "rt = streamTable(1000:0,`sym`x`de`ci,[SYMBOL,INT,DECIMAL128(2),DECIMAL128(4)]);" +
                "rt.append!(table(take(`XOM`GS`AAPL,1000) as sym,rand(100,1000) as x,take(101..300,1000) as de,take(301..500,1000) as ci))" +
                "share rt as read";
        conn.run(script);
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST,PORT,"admin","123456",3,false,false);
        BasicDBTask task = new BasicDBTask("select * from read;");
        pool.execute(task);
        BasicTable bt = null;
        if(task.isSuccessful()){
            bt = (BasicTable) task.getResult();
        }else{
            throw new Exception(task.getErrorMsg());
        }
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("de").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("ci").getDataType());
        System.out.println(bt.getString());
        pool.shutdown();
        conn.run("undef(`read,SHARED)");
    }

    @Test
    public void test_BasicDecimal_ConnectionPool_writePartitionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "t = table(timestamp(1..10)  as date,int(1..10) as sym,decimal128(1..10,2) as str)\n" +
                "db1=database(\"\",VALUE,date(now())+0..100)\n" +
                "db2=database(\"\",RANGE,int(1..10))\n" +
                "if(existsDatabase(\"dfs://demohash\")){\n" +
                "\tdropDatabase(\"dfs://demohash\")\n" +
                "}\n" +
                "db =database(\"dfs://demohash\",COMPO,[db1,db2])\n" +
                "pt = db.createPartitionedTable(t,`pt,`date`sym)\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://demohash\",\"pt\")");
        DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash","pt", "sym",pool);
        List<String> colNames = new ArrayList<String>(3);
        colNames.add("date");
        colNames.add("sym");
        colNames.add("str");
        List<Vector> cols = new ArrayList<Vector>(3);
        BasicTimestampVector date = new BasicTimestampVector(12);
        for (int i =0 ;i<12;i++)
            date.setTimestamp(i, LocalDateTime.now());
        cols.add(date);
        BasicIntVector sym = new BasicIntVector(12);
        for (int i =0 ;i<12;i+=4) {
            sym.setInt(i, 1);
            sym.setInt(i + 1, 2);
            sym.setInt(i + 2, 3);
            sym.setInt(i + 3, 4);
        }
        cols.add(sym);
        BasicDecimal128Vector bdv128 = new BasicDecimal128Vector(12,12);
        for (int i =0 ;i<12;i++) {
            bdv128.set(i,new BasicDecimal128(String.valueOf(i),12));
        }
        cols.add(bdv128);
        Map<String,Entity> map = new HashMap<>();
        map.put("TTable",new BasicTable(colNames,cols));
        conn.upload(map);
        BasicTable bt2 = (BasicTable) conn.run("TTable;");
        for (int i =0 ;i<1000;i++) {
            int m = appender.append(bt2);
            assertEquals(12,m);
        }
        BasicLong re = (BasicLong) conn.run("pt= loadTable(\"dfs://demohash\",`pt)\n" +
                "exec count(*) from pt");
        assertEquals(12000,re.getLong());
        pool.shutdown();
    }
    @Test
    public void test_BasicDecimal_MultiThreadWriteMemoryTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `int`intX`date,[DECIMAL128(2),DECIMAL128(4),DATE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "t1", true, false, null, 100000, 1000,
                1, "date");
        for (int i = 0; i < 900; i++) {
            mutithreadTableWriter_.insert(new BasicDecimal128(String.valueOf(i),2),new BasicDecimal128(String.valueOf(i),4), new Date());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from t1;");
        assertEquals(900,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("int").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("intX").getDataType());
        conn.run("undef(`t1,SHARED)");
    }

    @Test
    public void test_BasicDecimal_MultiThreadWriteStreamTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script ="t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`vol`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, DECIMAL128(2), DECIMAL128(4), DOUBLE])\n;share t as t1;" +
                "tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`vol`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, DECIMAL128(2), DECIMAL128(4), DOUBLE])\n;share tt as trades1;";
        conn.run(script);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "", "trades1", false, false, null, 1000, 1,
                20, "volume");
        for (int i = 0; i < 100; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDecimal128(String.valueOf(i % 10),2));
//            row.add(new BasicDecimal128((long) (i%100),4));
            row.add(new BasicDouble(i%100));

            row.add(new BasicDouble(i + 0.1));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert( "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1,
                    new BasicDecimal128(String.valueOf(i % 10),2), new BasicDecimal128(String.valueOf(i%100),4), i + 0.1);
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from trades1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("volume").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("vol").getDataType());
        conn.run("undef(`t1,SHARED);");
        conn.run("undef(`trades1,SHARED);");
    }

    @Test(timeout = 600000)
    public void test_BasicDecimal_MultiThreadWritePartitionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1048576;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "share t as t1;\n"+
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://testDecimal", "pt", false, false, null, 1000, 1,
                20, "id");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(i%100));
            row.add(new BasicDecimal128(String.valueOf(i),2));
            row.add(new BasicDecimal128(String.valueOf(i),4));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicInt(i%100),new BasicDecimal128(String.valueOf(i),2),new BasicDecimal128(String.valueOf(i),4));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt order by id");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by id;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
        conn.run("undef(`t1,SHARED);");
    }

    @Test(timeout = 600000)
    public void test_BasicDecimal_MultiThreadedWriteDimensionTable_decimal128() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1048576;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "share t as t1;\n"+
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createTable(t,`pt);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://testDecimal", "pt", false, false, null, 1000, 1,
                1, "id");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(i%100));
            row.add(new BasicDecimal128(String.valueOf(i),2));
            row.add(new BasicDecimal128(String.valueOf(i),4));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicInt(i%100),new BasicDecimal128(String.valueOf(i),2),new BasicDecimal128(String.valueOf(i),4));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt order by id");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by id;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
        conn.run("undef(`t1,SHARED);");
    }
    @Test(timeout = 600000)
    public void test_BasicDecimal128_dfs_tableInsert() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1048576;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL128(2),DECIMAL128(4)]);\n" +
                "share t as t1;\n"+
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createTable(t,`pt);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        mutithreadTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://testDecimal", "pt", false, false, null, 1000, 1,
                1, "id");
        for (int i = 0; i < 1048576; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(i%100));
            row.add(new BasicDecimal128(String.valueOf(i),2));
            row.add(new BasicDecimal128(String.valueOf(i),4));
            conn.run("tableInsert{t1}", row);
            ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(new BasicInt(i%100),new BasicDecimal128(String.valueOf(i),2),new BasicDecimal128(String.valueOf(i),4));
            assertEquals("code= info=",pErrorInfo.toString());
        }
        mutithreadTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from pt order by id");
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by id;");
        checkData(ex,bt);
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128,bt.getColumn("b").getDataType());
        conn.run("undef(`t1,SHARED);");
    }
}
