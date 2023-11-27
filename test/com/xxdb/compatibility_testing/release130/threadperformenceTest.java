package com.xxdb.compatibility_testing.release130;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import java.util.Random;
import java.util.ResourceBundle;

public class threadperformenceTest {
    private static DBConnection conn= new DBConnection();;
    private static String[] haSites;
    private static Random r = new Random();
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    private  String tbName=null;


    public static void main(String[] args) throws Exception {
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String DATA_FIRE="/hdd/USPrices.csv";
        conn.connect("192.168.1.116", 8999, "admin", "123456");
        String script = "dbName=\"dfs://test_olap_multithreadwrite\"\n" +
                "tableName=\"pt\"\n" +
                "login(\"admin\",\"123456\")\n" +
                "DATA_FIRE=\""+DATA_FIRE+"\"\n" +
                "if(existsDatabase(dbName)){\n" +
                " dropDatabase(dbName)\n" +
                "}\n" +
                "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n" +
                "schema=extractTextSchema(DATA_FIRE)\n" +
                "table1= table(1:0,schema[`name],schema[`type])\n" +
                "pt = db.createPartitionedTable(table1, `pt, `date)";
        conn.run(script);
        MultithreadedTableWriter mutithreadTableWriter = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "dfs://test_olap_multithreadwrite", "pt", false, false, haSites, 1000000, 1,
                2, "date");
        BasicTable bt= (BasicTable) conn.run("select * from loadText('"+DATA_FIRE+"')");
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();

        long start = System.currentTimeMillis();
        long finish ;
        for (int i=0;i<bt.rows();i++){
            mutithreadTableWriter.insert(pErrorInfo,bt.getColumn(0).get(i),bt.getColumn(1).get(i),bt.getColumn(2).get(i),bt.getColumn(3).get(i)
                    ,bt.getColumn(4).get(i),bt.getColumn(5).get(i),bt.getColumn(6).get(i),bt.getColumn(7).get(i)
                    ,bt.getColumn(8).get(i),bt.getColumn(9).get(i),bt.getColumn(10).get(i),bt.getColumn(11).get(i)
                    ,bt.getColumn(12).get(i),bt.getColumn(13).get(i),bt.getColumn(14).get(i),bt.getColumn(15).get(i),bt.getColumn(16).get(i)
                    ,bt.getColumn(17).get(i),bt.getColumn(18).get(i),bt.getColumn(19).get(i),bt.getColumn(20).get(i));
            if (i%100000 == 0){
                System.out.println(i);
            }
        }



        do{
            status = mutithreadTableWriter.getStatus();
            if (status.sentRows==bt.rows()){
                finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                System.out.println("time="+timeElapsed);
                break;
            }
        }while (true);
        mutithreadTableWriter.waitForThreadCompletion();
    }

}

