package com.xxdb.compatibility_testing.release130;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.time.LocalDate;
import java.util.*;

public class threadperformenceTest2 {
    private static DBConnection conn= new DBConnection();
    private static String[] haSites;
    private static Random r = new Random();
    private static int i1;
    public static String addr;
    private static long max=0;
    Random rd=new Random();
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    public static String HOST2 = "192.168.1.116";
    public static Integer PORT2 = 8999;
    private MultithreadedTableWriter mutithreadTableWriter=null;
    private  String tbName=null;


    public static void main(String[] args) throws Exception {
        //Console.WriteLine("insert end: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss:fff:ffffff"));
        DBConnection conn1 = new DBConnection();
        DBConnection conn2 = new DBConnection();
        //conn1.connect("localhost", 8900, "admin", "123456");
        conn2.connect("192.168.1.116", 8999, "admin", "123456");
        String script = "";
        String DATA_FIRE1="/hdd/USPrices.csv";
        String DATA_FIRE2="D:/USPrices.csv";
        script += "dbName='dfs://test_olap_multithreadwrite';";
        script += "tableName='pt';";
        script += "DATA_FILE='/hdd/USPrices.csv';";
        script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
        script += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6));";
        script += "schema=extractTextSchema(DATA_FILE);";
        script += "table1= table(1:0,schema[`name],schema[`type]);";
        script += "pt = db.createPartitionedTable(table1, `pt, `date);";
        conn2.run(script);
        conn2.run("data = select * from loadText('"+DATA_FIRE1+"')");
        BasicTable data = (BasicTable)conn2.run("select permno, date, ticker from data");
        BasicIntVector permnov = (BasicIntVector)data.getColumn(0);
        BasicDateVector datev = (BasicDateVector)data.getColumn(1);
        BasicSymbolVector tickerv = (BasicSymbolVector)data.getColumn(2);

        int total = data.rows();
//        LinkedList<Object> obl = new LinkedList<>();
//        Object x = 1;
//        for (int i = 0; i < total; i++){
//            obl.add(x);
//        }
//        System.out.println("Start insert");
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < obl.size(); i++){
//            obl.pop();
//        }
//        long endInsert = System.currentTimeMillis();
//        long insertTimeTakes = endInsert - start;
//        System.out.println("InsertTime = " + insertTimeTakes);

        //todo:1
        BasicInt[] permnovv = new BasicInt[total];
        BasicDate[] datevv = new BasicDate[total];
        BasicString[] tickervv = new BasicString[total];
        String[] A = new String[100];
        String[] B = new String[100];
        String[] C = new String[100];
        for(int i = 0; i < 100; ++i)
        {
            A[i] = "A" + i;
            B[i] = "B" + i;
            C[i] = "C" + i;
        }
        //todo:程序参数设置 -Xms6144m -Xmx6144m -XX:+UnlockCommercialFeatures
        MultithreadedTableWriter mutithreadTableWriter = new MultithreadedTableWriter(HOST2, PORT2, "admin", "123456",
                "dfs://test_olap_multithreadwrite", "pt", false, false, haSites, 1000000, 1,
                8, "date");
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        System.out.println("Start insert");
        long start = System.currentTimeMillis();
        long finish ;
        //for (int i = 0; i < total; i++)
        //int i = 0;
        LocalDate ld = LocalDate.now();
        BasicString bs = new BasicString("12123");
        int insertTimes = 0;
        for (int i = 0; i < total; i++)
        {
//            if (i==total-1){
//                i=0;
//            }
//            mutithreadTableWriter.insert(pErrorInfo, permnov.getInt(0), datev.getDate(0), i, tickerv.get(0), i % 99, 1, 1
//                    , A[i % 100], B[i % 100], C[i % 100], (double)(1), (double)(1), (double)(1), 1, (double)(1)
//                    , (double)(1), (double)(1), 1, (double)(1), (double)(1), (double)(1));
//            if (Runtime.getRuntime().freeMemory() < 10000000){
//                System.out.println("Free memory is: " + Runtime.getRuntime().freeMemory());
//            }
//            System.out.println(i);

            mutithreadTableWriter.insert(pErrorInfo, permnov.getInt(i), datev.getDate(i), i, tickerv.get(i), i % 99, 1, 1
                    , A[i % 100], B[i % 100], C[i % 100], (double)(1), (double)(1), (double)(1), 1, (double)(1)
                    , (double)(1), (double)(1), 1, (double)(1), (double)(1), (double)(1));
            if (i%100000 == 0){
//                System.gc();
                System.out.println("Free memory is: " + Runtime.getRuntime().freeMemory());
//                // 的JVM内存总量（单位是字节）
                System.out.println("Total memory is: " + Runtime.getRuntime().totalMemory());
                System.out.println("Max memory is: " + Runtime.getRuntime().maxMemory());
//                // JVM试图使用额最大内存量（单位是字节）
//                System.out.println(Runtime.getRuntime().maxMemory());
//                // 可用处理器的数目
//                System.out.println(Runtime.getRuntime().availableProcessors());
                System.out.println(i);
            }
        }
        //todo:1
        long endInsert = System.currentTimeMillis();
        long insertTimeTakes = endInsert - start;
        System.out.println("InsertTime = " + insertTimeTakes);
//        do{
//            mutithreadTableWriter.getStatus(status);
//            if (status.sentRows==data.rows()){
//                finish = System.currentTimeMillis();
//                long timeElapsed = finish - start;
//                System.out.println("time="+timeElapsed);
//                System.out.println("Send end");
//                break;
//            }
//        }while (true);
        do {
            BasicLong bt = (BasicLong) conn2.run("exec count(*) from loadTable(\"dfs://test_olap_multithreadwrite\", \"pt\")");
            if (bt.getLong() == data.rows()){
                finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                System.out.println("time="+timeElapsed);
                System.out.println("Send end");
                break;
            }
            //Thread.sleep(1000);
        }while (true);
        mutithreadTableWriter.waitForThreadCompletion();

    }

}
