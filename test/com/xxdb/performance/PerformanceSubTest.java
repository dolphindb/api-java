package com.xxdb.performance;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Scalar;
import com.xxdb.performance.read.Utils;
import com.xxdb.performance.stream.Sub;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

public class PerformanceSubTest {

    private static String user = "admin";
    private static String password = "123456";
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    public static String ip = bundle.getString("HOST");
    public static int port = Integer.parseInt(bundle.getString("PORT"));
    public static int clientPort = 31010;
    public static String clientIp = "172.17.0.1";
    public static String[] nodeList = bundle.getString("SITES").split(",");
    public static int subPort = 31999;
    public static String entrustPath = bundle.getString("P_DATA_DIR");
    public static String entrustName = bundle.getString("ENTRUST_NAME");
    public static String tickPath = bundle.getString("P_DATA_DIR");
    public static String tickName = bundle.getString("TICK_NAME");
    public static String snapshotPath = bundle.getString("P_DATA_DIR");
    public static String snapshotName = bundle.getString("SNAPSHOT_NAME");

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
        TimeUnit.SECONDS.sleep(2);
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
        TimeUnit.SECONDS.sleep(10);
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
        TimeUnit.SECONDS.sleep(2);
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
        conn.run("clearAllCache()");
        conn.run("undef(all)");
        conn.run("def cleanEnvironment(){\n" +
                "try{ dropStreamTable(`sharedTick) } catch(ex){ print(ex) }\n" +
                "undef all\n" +
                "}\n" +
                "cleanEnvironment()\n" +
                "go");
    }

    @Test
    public void SubTest() throws Exception {
        long st = System.currentTimeMillis();
        Sub.start2("pro",nodeList,subPort,clientIp,clientPort,ip,port,33613835);
        TimeUnit.SECONDS.sleep(1);
        DBConnection dbConnection = new DBConnection();
        dbConnection.connect(ip,port);
        TimeUnit.SECONDS.sleep(10);
        int count = 0;
        while (true){
            TimeUnit.MILLISECONDS.sleep(1);

            BasicTable data = (BasicTable) dbConnection.run("t = getStreamingStat().pubTables;t;");
            if (data.rows() == 0) break;
            Scalar scalar = data.getColumn(2).get(0);
            String msgOffset = scalar.getString();
            BasicTable data2 = (BasicTable) dbConnection.run("t = select count(*) from sharedTick; t;");
            String ho = data2.getColumn(0).get(0).toString();
            count = Integer.parseInt(ho);
            if (msgOffset.equals(ho)) break;
        }
        long ed = System.currentTimeMillis();
        dbConnection.connect(clientIp,clientPort);
        String sql = String.format("insert into streamResult  values (%d,%d,%f,%d,%d)",st + Utils.timeDelta,ed + Utils.timeDelta,(ed - st) / 1000.0,count,1);
        dbConnection.run(sql);
    }
}
