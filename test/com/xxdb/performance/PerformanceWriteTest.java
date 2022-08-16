package com.xxdb.performance;

import com.xxdb.DBConnection;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.write.ResWriter;
import com.xxdb.performance.write.large.Entrust;
import com.xxdb.performance.write.large.Snapshot;
import com.xxdb.performance.write.large.Tick;
import com.xxdb.performance.write.tiny.EntrustWriter;
import com.xxdb.performance.write.tiny.SnapshotWriter;
import com.xxdb.performance.write.tiny.TickWriter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceWriteTest {

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
    public static DecimalFormat df = new DecimalFormat("#.00");
    public static String dbName;
    public static String tableName;
    public static String type;
    public static MultithreadedTableWriter mtw;
    public static int subPort = 31008;
    public static AtomicInteger writeFlag;
    private static int batchSize = 100000;

    public static String entrustPath = bundle.getString("P_DATA_DIR");
    public static String entrustName = bundle.getString("ENTRUST_NAME");
    public static String tickPath = bundle.getString("P_DATA_DIR");
    public static String tickName = bundle.getString("TICK_NAME");
    public static String snapshotPath = bundle.getString("P_DATA_DIR");
    public static String snapshotName = bundle.getString("SNAPSHOT_NAME");
    public static String performancePersistence = bundle.getString("PERFORMANCE_PERSISTENCE");
    public static List<Thread> qts = new ArrayList<>();
    ////@BeforeClass
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
        conn.run("dbName = \"dfs://tick_table\"\n" +
                "tbName = \"tick\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 5])\n" +
                "db3 = database(, VALUE, 1..20)\n" +
                "db = database(dbName, COMPO, [db3,db1, db2 ], , \"TSDB\")\n" +
                "name = `SecurityID`TradeTime`TradePrice`TradeQty`TradeAmount`BuyNo`SellNo`TradeIndex`ChannelNo`TradeBSFlag`BizIndex`Channel\n" +
                "type = `SYMBOL`TIMESTAMP`DOUBLE`INT`DOUBLE`INT`INT`INT`INT`SYMBOL`INT`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`Channel`TradeTime`SecurityID, compressMethods={TradeTime:\"delta\"}, sortColumns=`SecurityID`TradeTime, keepDuplicates=ALL)");
        conn.run("dbName = \"dfs://snapshot_table\"\n" +
                "tbName = \"snapshot\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 5])\n" +
                "db3 = database(, VALUE, 1..20)\n" +
                "db = database(dbName, COMPO, [db3, db1, db2 ], , \"TSDB\")\n" +
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
                "\tarray(DOUBLE, 0) as ETFSellMoney,\n" +
                "\tarray(INT,0) as Channel\n" +
                ")\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`Channel`DateTime`SecurityID, compressMethods={DateTime:\"delta\"}, sortColumns=`SecurityID`DateTime, keepDuplicates=ALL)");
        conn.run("dbName = \"dfs://entrust_table\"\n" +
                "tbName = \"entrust\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDatabase(dbName)\n" +
                "}\n" +
                "db1 = database(, VALUE, 2020.01.01..2021.01.01)\n" +
                "db2 = database(, HASH, [SYMBOL, 5])\n" +
                "db3 = database(, VALUE, 1..20)\n" +
                "db = database(dbName, COMPO, [db3, db1, db2], , \"TSDB\")\n" +
                "name = `SecurityID`TransactTime`OrderNo`Price`Balance`OrderBSFlag`OrdType`OrderIndex`ChannelNo`BizIndex`Channel\n" +
                "type = `SYMBOL`TIMESTAMP`INT`DOUBLE`INT`SYMBOL`SYMBOL`INT`INT`INT`INT\n" +
                "schemaTable = table(1:0, name, type)\n" +
                "db.createPartitionedTable(table=schemaTable, tableName=tbName, partitionColumns=`Channel`TransactTime`SecurityID, compressMethods={TransactTime:\"delta\"}, sortColumns=`SecurityID`TransactTime, keepDuplicates=ALL)");
        TimeUnit.SECONDS.sleep(2);
    }

    ////@AfterClass
    public static void tearDowm() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(clientIp,clientPort,"admin","123456");
        String day;
        SimpleDateFormat parser = new SimpleDateFormat("yyyy_MM_dd");
        day = parser.format(new Date());
        String sql1 = String.format("saveText(writeResult, \"%s\",,1)",performancePersistence + File.separator + day + "_writeResult.csv");
        conn.run(sql1);
    }

    //settings/write-entrust-0-1.setting
    ////@Test
    public void writeEntrust01() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                EntrustWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        1,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-0-5.setting
    ////@Test
    public void writeEntrust05() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                EntrustWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        5,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-0-10.setting
    ////@Test
    public void writeEntrust010() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                EntrustWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        10,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-0-15.setting
    ////@Test
    public void writeEntrust015() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                EntrustWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        15,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-0-20.setting
    ////@Test
    public void writeEntrust020() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                EntrustWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        20,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-0-1.setting
    ////@Test
    public void writeTick01() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                TickWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        1,20,10000000,"2021.12.08");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-0-5.setting
    ////@Test
    public void writeTick05() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                TickWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        5,20,10000000,"2021.12.08");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-0-10.setting
    ////@Test
    public void writeTick010() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                TickWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        10,20,10000000,"2021.12.08");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-0-20.setting
    ////@Test
    public void writeTick020() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                TickWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        20,20,10000000,"2021.12.08");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-0-1.setting
    ////@Test
    public void writeSnapshot01() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                SnapshotWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        1,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-0-5.setting
    //@Test
    public void writeSnapshot05() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                SnapshotWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        5,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-0-10.setting
    //@Ignore
    public void writeSnapshot010() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                SnapshotWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        10,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-0-15.setting
    //@Ignore
    public void writeSnapshot015() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                SnapshotWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        15,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-0-20.setting
    //@Ignore
    public void writeSnapshot020() throws Exception {
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);
        new Thread(() -> {
            try {
                SnapshotWriter.start2("pro","write","0",nodeList,user,password,batchSize,
                        20,20,10000000,"2021.01.04");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-1-1.setting
    //@Test
    public void writeEntrust11() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Entrust.start2("pro",nodeList,1,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-1-5.setting
    //@Test
    public void writeEntrust15() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Entrust.start2("pro",nodeList,5,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-1-10.setting
    //@Test
    public void writeEntrust110() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Entrust.start2("pro",nodeList,10,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-1-15.setting
    //@Test
    public void writeEntrust115() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Entrust.start2("pro",nodeList,15,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-entrust-1-20.setting
    //@Test
    public void writeEntrust120() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Entrust.start2("pro",nodeList,20,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-1-1.setting
    //@Test
    public void writeTick11() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Tick.start2("pro",nodeList,1,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-1-5.setting
    //@Test
    public void writeTick15() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Tick.start2("pro",nodeList,5,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-1-10.setting
    //@Test
    public void writeTick110() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Tick.start2("pro",nodeList,10,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-1-15.setting
    //@Test
    public void writeTick115() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Tick.start2("pro",nodeList,15,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-tick-1-20.setting
    //@Test
    public void writeTick120() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Tick.start2("pro",nodeList,20,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-1-1.setting
    //@Test
    public void writeSnapshot11() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Snapshot.start2("pro",nodeList,1,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //-----
    //settings/write-snapshot-1-5.setting
    //@Test//oom
    public void writeSnapshot15() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Snapshot.start2("pro",nodeList,5,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }

    //settings/write-snapshot-1-10.setting
    //@Test//oom
    public void writeSnapshot110() throws Exception{
        ResWriter.start2(clientIp,clientPort);
        writeFlag = new AtomicInteger(1);

        new Thread(() -> {
            try {
                Snapshot.start2("pro",nodeList,10,5000000,2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        while (true) {
            MultithreadedTableWriter.Status status = ResWriter.mtw.getStatus();
            Thread.sleep(1);
            if (status.sentRows == 1)
                break;
        }
        ResWriter.mtw.waitForThreadCompletion();
    }
}
