package com.xxdb.performance;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.read.QueryThread;
import com.xxdb.performance.read.Utils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.xxdb.performance.read.QpsQuery.*;

public class PerformanceReadTest {

    private static String dayDBName = "dfs://dayDB";
    private static String dayTableName = "day";
    private static String symDBName = "dfs://symDB";
    private static String symTableName = "sym";
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    public static String ip = bundle.getString("HOST");
    public static int port = Integer.parseInt(bundle.getString("PORT"));
    private static String user = "admin";
    private static String password = "123456";
    public static String clientIp = "172.17.0.1";
    public static int clientPort = 31010;
    public static String[] nodeList = bundle.getString("SITES").split(",");
    public static int queryNum;
    public static int threadNum;
    public static DecimalFormat df = new DecimalFormat("#.00");
    public static String dbName;
    public static String tableName;
    public static String queryTableName;
    public static String queryDBName;
    public static String type;
    public static MultithreadedTableWriter mtw;
    public static int subPort = 31008;
    //dataPath
    public static String entrustPath = bundle.getString("P_DATA_DIR");
    public static String entrustName = bundle.getString("ENTRUST_NAME");
    public static String tickPath = bundle.getString("P_DATA_DIR");
    public static String tickName = bundle.getString("TICK_NAME");
    public static String snapshotPath = bundle.getString("P_DATA_DIR");
    public static String snapshotName = bundle.getString("SNAPSHOT_NAME");
    public static String performancePersistence = bundle.getString("PERFORMANCE_PERSISTENCE");
    public static List<Thread> qts = new ArrayList<>();

    public static void readStart(String type,int threadNum,int queryNum,String dbName,String tableName) throws Exception {
        PerformanceReadTest.dbName = dbName;
        PerformanceReadTest.type = type;
        PerformanceReadTest.queryNum = queryNum;
        PerformanceReadTest.threadNum = threadNum;
        PerformanceReadTest.tableName = tableName;
        PerformanceReadTest.queryTableName = tableName;
        PerformanceReadTest.queryDBName = dbName;

        System.out.println("\ttype = " + type);
        System.out.println("\tnodeList = " + Arrays.toString(nodeList));
        System.out.println("\tuser = " + user);
        System.out.println("\tpassword = " + password);
        System.out.println("\tqueryDBName = " + queryDBName);
        System.out.println("\tqueryTableName = " + queryTableName);
        System.out.println("\tdayDBName = " + dayDBName);
        System.out.println("\tdayTableName = " + dayTableName);
        System.out.println("\tthreadNum = " + threadNum);
        System.out.println("\tqueryNum = " + queryNum);
        System.out.println("\tclientIp = " + clientIp);
        System.out.println("\tclientPort = " + clientPort);

        DBConnection conn = new DBConnection(false,false,true);
        if (!type.equals("2")) {
            try {
                conn.connect(ip, port, user, password);
                QueryThread.dayList = (BasicDateVector) conn.run(String.format("exec DateTime from loadTable('%s', '%s')", dayDBName, dayTableName));
                QueryThread.idList = (BasicStringVector) conn.run(String.format("exec SecurityID from loadTable('%s', '%s')", symDBName, symTableName));
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        DBConnection clientConn = new DBConnection();
        clientConn.connect(clientIp,clientPort,"admin","123456");
        Entity run = clientConn.run("c = exec count(*) from objs(true) where name=`queryResult and form=`TABLE\n" +
                "if(c == 0){\n" +
                "\tcolnames = `data`type`threadNum`queryNum`threadName`times`cost`rows`sqlMsg`begin`end\n" +
                "\tcoltype = [SYMBOL,SYMBOL,INT,INT,SYMBOL,INT,LONG,LONG,STRING,TIMESTAMP,TIMESTAMP]\n" +
                "\tshare table(1:0,colnames,coltype) as `queryResult\n" +
                "}");
        run = clientConn.run("c = exec count(*) from objs(true) where name=`queryResult2 and form=`TABLE\n" +
                "if(c == 0){\n" +
                "\tcolnames = `data`type`threadNum`cost`QPS`RPS`start_time`end_time\n" +
                "\tcoltype = [SYMBOL,SYMBOL,INT,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,TIMESTAMP]\n" +
                "\tshare table(1:0,colnames,coltype) as `queryResult2\n" +
                "}");
        clientConn.close();

        //mtw = new MultithreadedTableWriter(clientIp, clientPort, "admin", "123456", "", "queryResult",
               // false, false, null, 100, 0.001f, 1, "threadName");

        query(threadNum);

        long st = System.currentTimeMillis();

        while (true) {
            Thread.sleep(1);
            if (QueryThread.cdl.get() == threadNum) break;
        }

        long ed = System.currentTimeMillis();

        long count = QueryThread.totalCount.get();
        double cost = (QueryThread.maxEd.get() - QueryThread.minSt.get())/1000;
        if (cost == 0) cost =1.0;
        double qps = queryNum * threadNum / cost;
        double rps = count / cost;
        System.out.printf("Total Count : %s, Cost : %s s,QPS : %s, Per Thread QPS : %s, RPS : %s, StartTime : %s, EndTime : %s", count, df.format(cost), df.format(qps), df.format(qps / threadNum), df.format(rps), Utils.timeStamp2Date(QueryThread.minSt.get()), Utils.timeStamp2Date(QueryThread.maxEd.get()));
        System.out.println();
        //mtw.waitForThreadCompletion();
        //MultithreadedTableWriter result = new MultithreadedTableWriter(clientIp, clientPort, "admin", "123456", "", "queryResult2",
                //false, false, null, 100, 0.001f, 1, "threadNum");
        //result.insert(tableName,type,threadNum,cost,qps,rps,st + Utils.timeDelta,ed + Utils.timeDelta);
        //result.waitForThreadCompletion();
        try{
            clientConn.connect(clientIp,clientPort,"admin","123456");
            String sql = String.format("insert into queryResult2 values(\"%s\",\"%s\",%d,%f,%f,%f,%s,%s)",tableName,type,threadNum,cost,qps,rps,st + Utils.timeDelta,ed + Utils.timeDelta);
            clientConn.run(sql);
        }catch (Exception e){
            e.printStackTrace();
        }

        QueryThread.cdl.set(0);

        TimeUnit.MINUTES.sleep(1);
    }

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(ip,port,"admin","123456");
        //1.create database
        conn.run("dbName = \"dfs://SH_TSDB_tick\"\n" +
                "tbName = \"tick\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 20])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "name = `SecurityID`TradeTime`TradePrice`TradeQty`TradeAmount`BuyNo`SellNo`TradeIndex`ChannelNo`TradeBSFlag`BizIndex\n" +
                "type = `SYMBOL`TIMESTAMP`DOUBLE`INT`DOUBLE`INT`INT`INT`INT`SYMBOL`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`TradeTime`SecurityID, compressMethods={TradeTime:\"delta\"}, sortColumns=`SecurityID`TradeTime, keepDuplicates=ALL)");
        conn.run("dbName = \"dfs://SH_TSDB_snapshot_ArrayVector\"\n" +
                "tbName = \"snapshot\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 30])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "schemaTable = table(\n" +
                "\tarray(SYMBOL, 0) as SecurityID,\n" +
                "\tarray(TIMESTAMP, 0) as DateTime,\n" +
                "\tarray(DOUBLE, 0) as PreClosePx,\n" +
                "\tarray(DOUBLE, 0) as OpenPx,\n" +
                "\tarray(DOUBLE, 0) as HighPx,\n" +
                "\tarray(DOUBLE, 0) as LowPx,\n" +
                "\tarray(DOUBLE, 0) as LastPx,\n" +
                "\tarray(INT, 0) as TotalVolumeTrade,\n" +
                "\tarray(DOUBLE, 0) as TotalValueTrade,\n" +
                "\tarray(SYMBOL, 0) as InstrumentStatus,\n" +
                "\tarray(DOUBLE[], 0) as BidPrice,\n" +
                "\tarray(INT[], 0) as BidOrderQty,\n" +
                "\tarray(INT[], 0) as BidNumOrders,\n" +
                "\tarray(INT[], 0) as BidOrders,\n" +
                "\tarray(DOUBLE[], 0) as OfferPrice,\n" +
                "\tarray(INT[], 0) as OfferOrderQty,\n" +
                "\tarray(INT[], 0) as OfferNumOrders,\n" +
                "\tarray(INT[], 0) as OfferOrders,\n" +
                "\tarray(INT, 0) as NumTrades,\n" +
                "\tarray(DOUBLE, 0) as IOPV,\n" +
                "\tarray(INT, 0) as TotalBidQty,\n" +
                "\tarray(INT, 0) as TotalOfferQty,\n" +
                "\tarray(DOUBLE, 0) as WeightedAvgBidPx,\n" +
                "\tarray(DOUBLE, 0) as WeightedAvgOfferPx,\n" +
                "\tarray(INT, 0) as TotalBidNumber,\n" +
                "\tarray(INT, 0) as TotalOfferNumber,\n" +
                "\tarray(INT, 0) as BidTradeMaxDuration,\n" +
                "\tarray(INT, 0) as OfferTradeMaxDuration,\n" +
                "\tarray(INT, 0) as NumBidOrders,\n" +
                "\tarray(INT, 0) as NumOfferOrders,\n" +
                "\tarray(INT, 0) as WithdrawBuyNumber,\n" +
                "\tarray(INT, 0) as WithdrawBuyAmount,\n" +
                "\tarray(DOUBLE, 0) as WithdrawBuyMoney,\n" +
                "\tarray(INT, 0) as WithdrawSellNumber,\n" +
                "\tarray(INT, 0) as WithdrawSellAmount,\n" +
                "\tarray(DOUBLE, 0) as WithdrawSellMoney,\n" +
                "\tarray(INT, 0) as ETFBuyNumber,\n" +
                "\tarray(INT, 0) as ETFBuyAmount,\n" +
                "\tarray(DOUBLE, 0) as ETFBuyMoney,\n" +
                "\tarray(INT, 0) as ETFSellNumber,\n" +
                "\tarray(INT, 0) as ETFSellAmount,\n" +
                "\tarray(DOUBLE, 0) as ETFSellMoney\n" +
                ")\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`DateTime`SecurityID, compressMethods={DateTime:\"delta\"}, sortColumns=`SecurityID`DateTime, keepDuplicates=ALL)");
        conn.run("login(\"admin\", \"123456\")\n" +
                "dbName = \"dfs://SH_TSDB_entrust\"\n" +
                "tbName = \"entrust\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 20])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "name = `SecurityID`TransactTime`OrderNo`Price`Balance`OrderBSFlag`OrdType`OrderIndex`ChannelNo`BizIndex\n" +
                "type = `SYMBOL`TIMESTAMP`INT`DOUBLE`INT`SYMBOL`SYMBOL`INT`INT`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`TransactTime`SecurityID, compressMethods={TransactTime:\"delta\"}, sortColumns=`SecurityID`TransactTime, keepDuplicates=ALL)");
        TimeUnit.SECONDS.sleep(3);
        //2 import data
        conn.run("def writeData(files, dirPath, dbName, tbName){\n" +
                "\tlogin(\"admin\", \"123456\")\n" +
                "\tpt = loadTable(dbName, tbName)\n" +
                "\tfor(file in files ){\n" +
                "\t\tfilePath = dirPath+\"/\"+file \n" +
                "\t\tprint(filePath)\n" +
                "\t\tschemaTB = table(pt.schema().colDefs.name as name, pt.schema().colDefs.typeString as type)\n" +
                "\t\ttry{ds = textChunkDS(filePath,512,\",\",schemaTB)\n" +
                "\t\t      mr(ds, append!{pt},,,false)\n" +
                "\t\t      }\n" +
                "\t\t catch(ex){print(ex)}\n" +
                "\n" +
                "\t}\n" +
                "}\n" +
                "def functionPOCTestCase3(files, dirPath, dbName, tbName,parallel){\n" +
                "//\tcutFile = cut(files, (size(files)/parallel+1))\n" +
                "\tcutFile = files\n" +
                "\tfor (i in 0..(cutFile.size()-1)){\n" +
                "\t\tprint(cutFile)\n" +
                "\t\tsubmitJob(tbName, tbName + string(parallel) , writeData{cutFile[i], dirPath, dbName, tbName})\n" +
                "\t}\n" +
                "}\n" +
                "\n" +
                "dirPath = \""+ entrustPath + "\""+"\n" +
                "files = exec filename from files(dirPath) where filename in [\""+entrustName+"\"] order by filename\n" +
                "parallel = 3\n" +
                "dbName = \"dfs://SH_TSDB_entrust\"\n" +
                "tbName = \"entrust\"\n" +
                "functionPOCTestCase3(files, dirPath, dbName, tbName, parallel)\n" +
                "\n" +
                "dirPath = \""+ tickPath + "\""+"\n" +
                "files = exec filename from files(dirPath) where filename in [\""+tickName+"\"] order by filename\n" +
                "parallel = 3\n" +
                "dbName = \"dfs://SH_TSDB_tick\"\n" +
                "tbName = \"tick\"\n" +
                "functionPOCTestCase3(files, dirPath, dbName, tbName, parallel)" +
                "\n" +
                "dirPath = \""+ snapshotPath + "\"" +"\n" +
                "files = exec filename from files(dirPath) where filename in [\""+snapshotName+"\"] order by filename\n" +
                "parallel = 3\n" +
                "dbName = \"dfs://SH_TSDB_snapshot_ArrayVector\"\n" +
                "tbName = \"snapshot\"\n" +
                "functionPOCTestCase3(files, dirPath, dbName, tbName, parallel)");
        TimeUnit.SECONDS.sleep(20);
        //3 dimension table
        conn.run("dbName = \"dfs://dayDB\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db=database(\"dfs://dayDB\",VALUE,1 2 3)\n" +
                "t = select distinct(date(DateTime)) as DateTime from loadTable(\"dfs://SH_TSDB_snapshot_ArrayVector\",\"snapshot\")\n" +
                "dt=db.createTable(t,`day).append!(t)\n" +
                "select * from dt;\n" +
                "\n" +
                "dbName = \"dfs://symDB\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db=database(\"dfs://symDB\",VALUE,1 2 3)\n" +
                "t = select distinct(SecurityID) as SecurityID from loadTable(\"dfs://SH_TSDB_snapshot_ArrayVector\",\"snapshot\") where date(DateTime) = 2021.01.04\n" +
                "dt=db.createTable(t,`sym).append!(t)\n" +
                "select * from dt;");
        TimeUnit.SECONDS.sleep(10);
        //4 write table
        conn.run("dbName = \"dfs://tick_mtw\"\n" +
                "tbName = \"tick\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 20])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "name = `SecurityID`TradeTime`TradePrice`TradeQty`TradeAmount`BuyNo`SellNo`TradeIndex`ChannelNo`TradeBSFlag`BizIndex\n" +
                "type = `SYMBOL`TIMESTAMP`DOUBLE`INT`DOUBLE`INT`INT`INT`INT`SYMBOL`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`TradeTime`SecurityID, compressMethods={TradeTime:\"delta\"}, sortColumns=`SecurityID`TradeTime, keepDuplicates=ALL)");
        conn.run("dbName = \"dfs://snapshot_mtw\"\n" +
                "tbName = \"snapshot\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 30])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "schemaTable = table(\n" +
                "\tarray(SYMBOL, 0) as SecurityID,\n" +
                "\tarray(TIMESTAMP, 0) as DateTime,\n" +
                "\tarray(DOUBLE, 0) as PreClosePx,\n" +
                "\tarray(DOUBLE, 0) as OpenPx,\n" +
                "\tarray(DOUBLE, 0) as HighPx,\n" +
                "\tarray(DOUBLE, 0) as LowPx,\n" +
                "\tarray(DOUBLE, 0) as LastPx,\n" +
                "\tarray(INT, 0) as TotalVolumeTrade,\n" +
                "\tarray(DOUBLE, 0) as TotalValueTrade,\n" +
                "\tarray(SYMBOL, 0) as InstrumentStatus,\n" +
                "\tarray(DOUBLE[], 0) as BidPrice,\n" +
                "\tarray(INT[], 0) as BidOrderQty,\n" +
                "\tarray(INT[], 0) as BidNumOrders,\n" +
                "\tarray(INT[], 0) as BidOrders,\n" +
                "\tarray(DOUBLE[], 0) as OfferPrice,\n" +
                "\tarray(INT[], 0) as OfferOrderQty,\n" +
                "\tarray(INT[], 0) as OfferNumOrders,\n" +
                "\tarray(INT[], 0) as OfferOrders,\n" +
                "\tarray(INT, 0) as NumTrades,\n" +
                "\tarray(DOUBLE, 0) as IOPV,\n" +
                "\tarray(INT, 0) as TotalBidQty,\n" +
                "\tarray(INT, 0) as TotalOfferQty,\n" +
                "\tarray(DOUBLE, 0) as WeightedAvgBidPx,\n" +
                "\tarray(DOUBLE, 0) as WeightedAvgOfferPx,\n" +
                "\tarray(INT, 0) as TotalBidNumber,\n" +
                "\tarray(INT, 0) as TotalOfferNumber,\n" +
                "\tarray(INT, 0) as BidTradeMaxDuration,\n" +
                "\tarray(INT, 0) as OfferTradeMaxDuration,\n" +
                "\tarray(INT, 0) as NumBidOrders,\n" +
                "\tarray(INT, 0) as NumOfferOrders,\n" +
                "\tarray(INT, 0) as WithdrawBuyNumber,\n" +
                "\tarray(INT, 0) as WithdrawBuyAmount,\n" +
                "\tarray(DOUBLE, 0) as WithdrawBuyMoney,\n" +
                "\tarray(INT, 0) as WithdrawSellNumber,\n" +
                "\tarray(INT, 0) as WithdrawSellAmount,\n" +
                "\tarray(DOUBLE, 0) as WithdrawSellMoney,\n" +
                "\tarray(INT, 0) as ETFBuyNumber,\n" +
                "\tarray(INT, 0) as ETFBuyAmount,\n" +
                "\tarray(DOUBLE, 0) as ETFBuyMoney,\n" +
                "\tarray(INT, 0) as ETFSellNumber,\n" +
                "\tarray(INT, 0) as ETFSellAmount,\n" +
                "\tarray(DOUBLE, 0) as ETFSellMoney\n" +
                ")\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`DateTime`SecurityID, compressMethods={DateTime:\"delta\"}, sortColumns=`SecurityID`DateTime, keepDuplicates=ALL)");
        conn.run("dbName = \"dfs://entrust_mtw\"\n" +
                "tbName = \"entrust\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 20])\n" +
                "db = database(dbName, COMPO, [db1, db2], , \"TSDB\")\n" +
                "name = `SecurityID`TransactTime`OrderNo`Price`Balance`OrderBSFlag`OrdType`OrderIndex`ChannelNo`BizIndex\n" +
                "type = `SYMBOL`TIMESTAMP`INT`DOUBLE`INT`SYMBOL`SYMBOL`INT`INT`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`TransactTime`SecurityID, compressMethods={TransactTime:\"delta\"}, sortColumns=`SecurityID`TransactTime, keepDuplicates=ALL)");
        TimeUnit.SECONDS.sleep(2);
    }

    @AfterClass
    public static void tearDowm() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(clientIp,clientPort,"admin","123456");
        String day;
        SimpleDateFormat parser = new SimpleDateFormat("yyyy_MM_dd");
        day = parser.format(new Date());
        String sql1 = String.format("saveText(queryResult, \"%s\",,1)",performancePersistence + File.separator + day + "_queryResult.csv");
        conn.run(sql1);
    }

    //Single thread and multi client concurrent random query of small pieces of data
    @Test
    public void readTick01() throws Exception {
        readStart("0",1,1000,"dfs://SH_TSDB_tick","tick");
    }
    @Test
    public void readTick010() throws Exception {
        readStart("0",10,1000,"dfs://SH_TSDB_tick","tick");
    }
    @Test
    public void readTick020() throws Exception {
        readStart("0",20,1000,"dfs://SH_TSDB_tick","tick");
    }
    @Test
    public void readTick050() throws Exception {
        readStart("0",50,1000,"dfs://SH_TSDB_tick","tick");
    }
    @Test
    public void readTick0100() throws Exception {
        readStart("0",100,1000,"dfs://SH_TSDB_tick","tick");
    }
    @Test
    public void readSnapshot01() throws Exception {
        readStart("0",1,1000,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    @Test
    public void readSnapshot010() throws Exception {
        readStart("0",10,1000,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    @Test
    public void readSnapshot020() throws Exception {
        readStart("0",20,1000,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    @Test
    public void readSnapshot050() throws Exception {
        readStart("0",50,1000,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    @Test
    public void readSnapshot0100() throws Exception {
        readStart("0",100,1000,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    //Single thread and multi client concurrent random query of big pieces of data
    //@Test
    public void readTick11() throws Exception {
        readStart("1",1,10,"dfs://SH_TSDB_tick","tick");
    }
    //@Test
    public void readTick110() throws Exception {
        readStart("1",10,10,"dfs://SH_TSDB_tick","tick");
    }

    //@Test
    public void readTick120() throws Exception {
        readStart("1",20,10,"dfs://SH_TSDB_tick","tick");
    }
    //@Test
    public void readTick150() throws Exception {
        readStart("1",50,10,"dfs://SH_TSDB_tick","tick");
    }
    //@Test
    public void readTick1100() throws Exception {
        readStart("1",100,1,"dfs://SH_TSDB_tick","tick");
    }
    //@Test
    public void readSnapshot11() throws Exception {
        readStart("1",1,10,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    //@Test
    public void readSnapshot110() throws Exception {
        readStart("1",10,10,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    //@Test//oom
    public void readSnapshot120() throws Exception {
        readStart("1",20,10,"dfs://SH_TSDB_snapshot_ArrayVector","snapshot");
    }
    //----
}
