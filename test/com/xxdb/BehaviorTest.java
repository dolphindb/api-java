package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class BehaviorTest {
    private static DBConnection conn= new DBConnection();
    public static String HOST = "192.168.1.116";
    public static Integer PORT = 8999;
    private static MultithreadedTableWriter multithreadedTableWriter_ = null;
    private static MultithreadedTableWriter.Status status_ = new MultithreadedTableWriter.Status();


    public static void testMul() throws Exception{
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        conn.connect(HOST, PORT, "admin", "123456");
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
        conn.run("grant('test', TABLE_WRITE, 'dfs://valuedb3/pdatetest')");
        conn.run("grant('test', TABLE_READ, 'dfs://valuedb3/pdatetest')");
//        conn.run("mtwCreateTime=gmtime(now())");
//        System.out.println(((BasicTimestamp)conn.run("mtwCreateTime")).getString());
        System.out.println("-------------------------------------------------------------------------------------");
        System.out.println("正常写入");
        multithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
                false, false, null, 10000, 1,
                5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
        boolean ret;
        try
        {
            //插入100行正确数据
            for (int i = 0; i < 100; ++i)
            {
                ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
                //此处不会执行到
                if (pErrorInfo.hasError())
                    System.out.println(String.format("insert wrong format data: {0}\n", pErrorInfo.toString()));
            }
        }
        catch (Exception e)
        {   //MTW 抛出异常
            System.out.println("MTW exit with exception {0}" + e.getMessage());
        }

        //等待 MTW 插入完成
        multithreadedTableWriter_.waitForThreadCompletion();
        MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
        multithreadedTableWriter_.getStatus(writeStatus);
        if (writeStatus.errorInfo.hasError())
        {
            //写入时发生错误
            System.out.println("error in writing !");
        }
        System.out.println("writeStatus: {0}\n" + writeStatus.toString());
        System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
        System.out.println("-------------------------------------------------------------------------------------");
        System.out.println("数据类型和列数不一样");

        multithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "test", "123456", "dfs://valuedb3", "pdatetest",
                false, false, null, 10000, 1,
                5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
        try
        {
            //插入100行正确数据 （类型和列数都正确），MTW正常运行
            for (int i = 0; i < 100; ++i)
            {
                ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
                //此处不会执行到
                if (pErrorInfo.hasError())
                    System.out.println(String.format("insert wrong format data: {1}\n", pErrorInfo.toString()));
            }
            Thread.sleep(2000);

            //插入1行类型错误数据，MTW立刻发现
            //MTW立刻返回错误信息
            ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), 222, random.nextInt() % 10000);
            if (ret != true)
                System.out.println("insert wrong format data: {2}\n" + pErrorInfo.toString());

            //插入1行数据，列数不匹配，MTW立刻发现
            //MTW立刻返回错误信息
            ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), random.nextInt() % 10000);
            if (ret != true)
                System.out.println("insert wrong format data: {3}\n" + pErrorInfo.toString());

            //修改test用户权限
            conn.run("deny('test', TABLE_WRITE, 'dfs://valuedb3/pdatetest')");
            conn.run("deny('test', TABLE_READ, 'dfs://valuedb3/pdatetest')");
            //在dolphindb中关闭MTW中的异步连接
            conn.run("id = exec sessionid from getSessionMemoryStat() where UserId = 'test';");
            conn.run("for(closeid in id)closeSessions(closeid);");
            Thread.sleep(2000);


            //先写一行数据，触发error
            ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
            System.out.println("先写一行数据，触发error " + pErrorInfo.toString());
            Thread.sleep(1000);

            //再插入10行正确数据，MTW会因为工作线程终止而抛出异常，且该行数据不会被写入MTW
            for (int i = 0; i < 9; ++i)
            {
                ret = multithreadedTableWriter_.insert(pErrorInfo, new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
            }
            System.out.println("再插入9行正确数据，MTW会因为工作线程终止而抛出异常，且该行数据不会被写入MTW" + pErrorInfo.toString());
            System.out.println("never run here");
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            System.out.println("MTW抛出异常");
        }
        multithreadedTableWriter_.waitForThreadCompletion();
        MultithreadedTableWriter.Status status1 = new MultithreadedTableWriter.Status();
        multithreadedTableWriter_.getStatus(status1);
        if (writeStatus.errorInfo.errorCode != "A0")
            //写入发生错误
            System.out.println("writeStatus: {4}\n" + status1.toString());
        System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
        System.out.println("-------------------------------------------------------------------------------------");
        System.out.println();

        List<List<Entity>> unwriterdata = new ArrayList<>();
        if (writeStatus.sentRows != 210)
        {
            System.out.println("error after write complete:");
            multithreadedTableWriter_.getUnwrittenData(unwriterdata);
            System.out.println("{5} unwriterdata: " + unwriterdata.size());

            //重新获取新的MTW对象
            MultithreadedTableWriter newmultithreadedTableWriter = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
                    false, false, null, 10000, 1,
                    5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
            try
            {
                boolean writesuccess = true;
                //将没有写入的数据写到新的MTW中
                pErrorInfo = new ErrorCodeInfo();
                ret = newmultithreadedTableWriter.insert(unwriterdata, pErrorInfo);

                for (int i = 0; i < 10 - unwriterdata.size(); ++i)
                {
                    ret = newmultithreadedTableWriter.insert(pErrorInfo, new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
                }

            }
            finally
            {
                newmultithreadedTableWriter.waitForThreadCompletion();
                newmultithreadedTableWriter.getStatus(writeStatus);
                System.out.println("writeStatus: {6}\n" + writeStatus.toString());
            }
        }
        else
            System.out.println("write complete : \n{7}" + writeStatus.toString());

        System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
    }



    public static void main(String[] args)throws Exception{
        testMul();
    }

}