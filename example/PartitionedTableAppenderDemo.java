package com.dolphindb.examples;

import com.xxdb.DBConnection;
import com.xxdb.DBConnectionPool;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.data.BasicTable;
import com.xxdb.route.PartitionedTableAppender;



/**
 * PTA (PartitionedTableAppender) is used to write batch data to dfs table efficiently. The data will be sent to multiThreads
 *  and void chunk concurrent write collision().
 * @see <a href="https://docs.dolphindb.cn/zh/help/ErrorCodeList/S00002/index.html">S00002</a>
 */
public class PartitionedTableAppenderDemo {

     static String host = "192.168.1.206";
     static int port = 8911;
     static String username = "admin";
     static String password = "DolphinDB@123";

    public static void main(String[] args) throws Exception {

        String prepareData = "drop database if exists \"dfs://test\"\n"
            + "n=1000000\n" +
            "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, rand(10.0, n) as value)\n" +
            "db = database(\"dfs://test\", RANGE, `A`F`M`S`ZZZZ)\n" +
            "db.createPartitionedTable(t, \"pt\", \"symbol\")";
        DBConnection conn = new DBConnection();
        conn.connect(host, port, username, password);
        conn.run(prepareData);

        String dbPath = "dfs://test";
        String tableName = "pt";
        int threadCount = 10;
        DBConnectionPool connPool = new ExclusiveDBConnectionPool(host, port, username, password, threadCount, false, false);
        PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "symbol", connPool);
        BasicTable data = (BasicTable) conn.run("select * from t limit 10000");
        int rows = appender.append(data);
        System.out.println(rows + " rows inserted.");

        connPool.waitForThreadCompletion();
        connPool.shutdown();
    }


}
