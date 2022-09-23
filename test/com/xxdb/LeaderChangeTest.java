package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.*;

public class LeaderChangeTest {
    private static DBConnection conn= new DBConnection();
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    //static int PORT = 9002;
    public static Random random = new Random();

    public static class MTWInsertThreads implements Runnable{
        public boolean isContinue_;
        public long curIndex_;

        public MTWInsertThreads(boolean isContinue, long curIndex){
            this.isContinue_ = isContinue;
            this.curIndex_ = curIndex;
        }

        @Override
        public void run() {
            try {
                mtwInsert(isContinue_, curIndex_);
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("Thread run Error: " + e.getMessage());
            }
        }

        public void mtwInsert(boolean isContinue, long curIndex)throws Exception{
            while (isContinue){
                MultithreadedTableWriter writer = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "hg", "",
                        false, true, null, 10000, 1, 3, "id");
                while (isContinue){
                    try {
                        List<Integer> intList = new ArrayList<>();
                        for (int i = random.nextInt() % 100 - 1; i >= 0; i--){
                            intList.add(random.nextInt() % 100);
                        }
                        Integer[] a = new Integer[]{};
                        intList.toArray(a);
                        ErrorCodeInfo pErrorCodeInfo = new ErrorCodeInfo();
                        if (writer.insert(curIndex++, "Symbol" + String.valueOf(random.nextInt() % 100), "String" + String.valueOf(random.nextInt() % 100), a).errorInfo != ""){
                            System.out.println(pErrorCodeInfo.errorInfo);
                        }
                        System.out.print(".");
                        Thread.sleep(1000);
                    }catch (Exception e){
                        System.out.println("-------------MTW exit with exception: " + e.getMessage());
                        break;
                    }
                }
            }
        }
    }

    public static class LeaderChangeThreads implements Runnable{

        @Override
        public void run() {
            String[] ipports = new String[]{"192.168.1.182:8931", "192.168.1.182:8941"};
            String[] nodes = new String[]{"P2-NODE1","P3-NODE1","P4-NODE1"};
            try {
                while (true){
                    {
                        BasicInt res = (BasicInt) conn.run("1 + 1");
                        if (res.isNull() || res.getInt() != 2){
                            conn.connect(HOST, PORT, "admin", "123456", "", true, ipports);
                        }
                    }
                    {
                        for (int i = 0; i < 3; i++){
                            String scripts = "rpc(getControllerAlias(),startDataNode,'" + nodes[i] + "');";
                            try {
                                System.out.println("Start node " + nodes[i]);
                                BasicString bs = (BasicString) conn.run(scripts);
                                System.out.println(nodes[i] + " : " + bs.getString());
                            }catch (Exception e){
                                System.out.println("Start node " + nodes[i] + " error" + e.getMessage());
                            }
                        }
                    }

                    for (int i = 0; i < 20; i++){
                        System.out.println("Switch " + i + " times");
                        for (int ni = 0; ni < 3; ni ++){
                            String scripts = "rpc('" + nodes[ni] + "',streamCampaignForLeader,2);";
                            try {
                                conn.run(scripts);
                                BasicString leader = (BasicString) conn.run("getStreamingLeader(2)");
                                System.out.println("--------------------" + leader.getString() + "--------------------");
                            }catch (Exception e){
                                System.out.println("Switch node " + nodes[ni] + " error " + e.getMessage());
                            }
                        }
                        Thread.sleep(5000);
                    }

                    try {
                        BasicTable count = (BasicTable) conn.run("select count(*) from hg");
                        System.out.println("Table count" + count.getString() + ", now stop nodes");
                        for (int i = 0; i < 3; i++){
                            String scripts = "rpc(getControllerAlias(),stopDataNode,'" + nodes[i] + "');";
                            conn.run(scripts);
                        }
                    }catch (Exception e){
                        System.out.println("Stop nodes error " + e.getMessage());
                    }
                    Thread.sleep(10000);
                }
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("Thread run Error: " + e.getMessage());
            }
        }
    }


    public static void testLeaderChange() throws Exception{
        boolean isContinue = true;

        String[] ipports = new String[]{"192.168.1.182:8931", "192.168.1.182:8941"};

        conn.connect(HOST, PORT, "admin", "123456", "", true, ipports);
        while (true){
            try {
                String scripts = "haTableName='hg'; try{ dropStreamTable(haTableName); }catch(ex){};\n " +
                        "t = table(1:0, `id`code`text`value,[INT,SYMBOL,STRING,INT[]]); haStreamTable(2,t,haTableName,1000000,'id',1440);";
                conn.run(scripts);
                break;
            }catch (Exception e){
                System.out.println("connect error : " + e.getMessage());
            }
        }


        long curIndex = 0;

        new Thread(new MTWInsertThreads(isContinue, curIndex)).start();
        new Thread(new LeaderChangeThreads()).start();

//        while (isContinue){
//            String[] nodes = new String[]{"P2-NODE1","P3-NODE1","P4-NODE1"};
//            {
//                BasicInt res = (BasicInt) conn.run("1 + 1");
//                if (res.isNull() || res.getInt() != 2){
//                    conn.connect(HOST, PORT, "admin", "123456", "", true, ipports);
//                }
//            }
//            {
//                for (int i = 0; i < 3; i++){
//                    String scripts = "rpc(getControllerAlias(),startDataNode,'" + nodes[i] + "');";
//                    try {
//                        System.out.println("Start node " + nodes[i]);
//                        BasicString bs = (BasicString) conn.run(scripts);
//                        System.out.println(nodes[i] + " : " + bs.getString());
//                    }catch (Exception e){
//                        System.out.println("Start node " + nodes[i] + " error" + e.getMessage());
//                    }
//                }
//            }
//
//            for (int i = 0; i < 20; i++){
//                System.out.println("Switch " + i + " times");
//                for (int ni = 0; ni < 3; ni ++){
//                    String scripts = "rpc('" + nodes[ni] + "',streamCampaignForLeader,2);";
//                    try {
//                        BasicString result = (BasicString) conn.run(scripts);
//                        System.out.println(nodes[ni] + " : " + result.getString());
//                    }catch (Exception e){
//                        System.out.println("Switch node " + nodes[ni] + " error" + e.getMessage());
//                    }
//                }
//                Thread.sleep(100);
//            }
//
//            try {
//                BasicLong count = (BasicLong) conn.run("select count(*) from h2");
//                System.out.println("Table count" + count + ", now stop nodes");
//                for (int i = 0; i < 3; i++){
//                    String scripts = "rpc(getControllerAlias(),stopDataNode,'" + nodes[i] + "');";
//                    BasicString bs = (BasicString) conn.run(scripts);
//                    System.out.println(nodes[i] + " : " + bs.getString());
//                }
//            }catch (Exception e){
//                System.out.println("Stop nodes error " + e.getMessage());
//            }
//            Thread.sleep(10000);
//        }

        isContinue = false;
    }

    public static void test11()throws Exception
    {
        DBConnection dBConnection = new DBConnection();
        dBConnection.connect("192.168.1.116", 8999);
        dBConnection.run("data = table([concat(char(1..1000))] as id);share data as data1;");
        BasicTable vector = (BasicTable)dBConnection.run("data1");
        Map<String, Entity> map = new HashMap<>();
        map.put("t3", vector);
        dBConnection.upload(map);
        BasicBoolean ret = (BasicBoolean)dBConnection.run("(exec id from data1)[0] == (exec id from t3)[0]");
        int a = 1;
    }

    public static void main(String[] args)throws Exception{
        test11();
    }
}
