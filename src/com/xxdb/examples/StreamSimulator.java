package com.xxdb.examples;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import java.util.ArrayList;
import java.util.List;

public class StreamSimulator {

    public static void main(String[] args) {
        DBConnection conn = new DBConnection();
        DBConnection conn1 = new DBConnection(true);

        try{
            // custom variables
            String hostName = "115.239.209.234";
            int port = 28948;

            int interval = 0;

            String dbPath = "dfs://DataYesDB";
            String tradeTableName = "trade";
            String streamingTableName = "trade1";
            String weightsTableName = "weightsTable";

            String userId = "admin";
            String password = "123456";

            if(args.length >= 1) {
                hostName = args[0];
            }
            if(args.length >= 2) {
                port = Integer.parseInt(args[1]);
            }
            if(args.length >= 3) {
                interval = Integer.parseInt(args[2]);
            }
            if(args.length >= 4) {
                dbPath = args[3];
            }
            if(args.length >= 5) {
                tradeTableName = args[4];
            }
            if(args.length >= 6) {
                streamingTableName = args[5];
            }
            if(args.length >= 7) {
                weightsTableName = args[6];
            }
            if(args.length >= 8) {
                userId = args[7];
            }
            if(args.length >= 9) {
                password = args[8];
            }

            conn.connect(hostName, port, userId, password);
            conn1.connect(hostName, port, userId, password);

            // query data
            String script ="trade = loadTable('" + dbPath + "', `" + tradeTableName + ")\n" +
                    "id=exec SecurityId from " + weightsTableName + "\n" +
                    "select TradeDate, TradeTime, SecurityID, TradePrice, TradeVolume, TradeBSFlag from " + tradeTableName + " where TradeDate=2019.09.06 and SecurityID in id order by TradeTime";
            BasicTable trade = (BasicTable) conn.run(script);
            conn.close();

            BasicTimeVector timeVector = (BasicTimeVector) trade.getColumn(1);
            int timeVectorSize = timeVector.rows();
            int recentTime = timeVector.getInt(0);
            int offset = 0;
            for(int i = 0; i < timeVectorSize; i++) {
                int curTime = timeVector.getInt(i);
                if(curTime != recentTime) {
                    if(i - offset > 1) {
                        int[] indices = new int[i - offset];
                        int value = offset;
                        for(int idx = 0; idx < indices.length; idx++) {
                            indices[idx] = value;
                            value++;
                        }
                        Table subTrade = trade.getSubTable(indices);
                        List<Entity> arguments = new ArrayList<>();
                        arguments.add(subTrade);
                        conn1.run("tableInsert{" + streamingTableName + "}", arguments);
                        // by default, sleep for 0 ms after each subTable insertion
                        Thread.sleep(interval);
                        offset = i;
                    }
                    recentTime = curTime;
                }
            }
        } 
        catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

}