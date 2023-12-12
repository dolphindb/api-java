package examples;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.time.LocalDate;
import java.util.*;


/**
 *  MTW (MultihreadedTableWriter) is used to write streaming data into dfs table or streamTable.
 *  when write dfs table, we recommend that the thread number equals to the partition number of the dfs table.
 */
public class MultithreadedTableWriterDemo {

    private static String host = "localhost";
    private static int port = 8848;
    private static String username = "admin";
    private static String password = "123456";

    public static void main(String[] args) throws Exception {
        DBConnection conn= new DBConnection();
        conn.connect(host, port, "admin", "123456");
        Random random = new Random();
        String script =
                "dbName = 'dfs://valuedb3'" +
                        "if (exists(dbName))" +
                        "{" +
                        "dropDatabase(dbName);" +
                        "}" +
                        "datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);" +
                        "db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);" +
                        "pt = db.createPartitionedTable(datetest,'pdatetest','id');";
        conn.run(script);
        MultithreadedTableWriter multithreadedTableWriter_ = new MultithreadedTableWriter(host, port, "admin", "123456", "dfs://valuedb3", "pdatetest",
                false, false, null, 10000, 1,
                5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
        ErrorCodeInfo ret;
        try
        {
            for (int i = 0; i < 10000; ++i)
            {
                ret = multithreadedTableWriter_.insert(LocalDate.of(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
                if(ret.hasError()){
                    System.out.println("insert failed: " + ret.getErrorInfo());
                }
            }
        }
        catch (Exception e) // mtw may throw exception
        {
            System.out.println("MTW exit with exception {0}" + e.getMessage());
        }

        //wait for the Completion by join the thread.
        multithreadedTableWriter_.waitForThreadCompletion();
        MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
        writeStatus = multithreadedTableWriter_.getStatus();

        // check if mtw has errors.
        if (writeStatus.hasError())
        {
            System.out.println("error in writing !");
        }
        System.out.println("writeStatus: {0}\n" + writeStatus.toString());
        System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
    }
}
