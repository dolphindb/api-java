package com.xxdb;
import com.xxdb.data.*;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadPooledClient;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class testpool {


    public static void main(String[] args)  throws IOException, InterruptedException {
        String HOST = "192.168.0.69";
        int PORT = 18921;
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        String script0 = "login(`admin,`123456);" +
                "try{undef(`quote_commodity_stream, SHARED);}catch(ex){};"+
                "try{undef(`Receive, SHARED);}catch(ex){};"+
                "try{dropStreamTable('quote_commodity_stream')}catch(ex){};"+
                "try{dropStreamTable('Receive')}catch(ex){};";
        conn.run(script0);
        String script1 = "quote_commodity_ts = streamTable(100:0, [`time, `sym, `exch_time, `exch, `source, `broker, `tot_notional, `tot_sz, `last_px, `bid_sz1, `bid_px1, `ask_px1, `ask_sz1, `open, `high, `low, `close, `pre_close, `oi, `pre_oi, `settlement_px, `pre_settlement_px, `rec_time, `pub_time, `bid_sz2, `bid_px2, `ask_px2, `ask_sz2, `bid_sz3, `bid_px3, `ask_px3, `ask_sz3, `bid_sz4, `bid_px4, `ask_px4, `ask_sz4, `bid_sz5, `bid_px5, `ask_px5, `ask_sz5, `upper_limit, `lower_limit, `pre_delta, `curr_delta, `trading_date, `settlement_id, `settlement_group_id], [NANOTIMESTAMP, SYMBOL, NANOTIMESTAMP, SYMBOL, SYMBOL, SYMBOL, LONG, LONG, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, NANOTIMESTAMP, NANOTIMESTAMP, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DATE, LONG, LONG])\n" +
                "enableTableShareAndPersistence(table=quote_commodity_ts, tableName=`quote_commodity_stream, cacheSize=50000, preCache = 50000,retentionMinutes=1440)";
        conn.run(script1);
        String script2 = "st2 = streamTable(1000000:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Receive, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180)\t\n";
        conn.run(script2);
        ThreadPooledClient client = new ThreadPooledClient(HOST,PORT,3);
        System.out.println("client created");

        client.subscribe(HOST, PORT, "quote_commodity_stream", "subTrades", MessageHandler_handler, -1, true);
    }

    public static MessageHandler MessageHandler_handler = new MessageHandler() {
        @Override
        public void doEvent(IMessage msg) {
            try {
                String HOST = "192.168.0.69";
                int PORT = 18921;
                String script = String.format("insert into Receive values(%d,%s,%f)", Integer.parseInt(msg.getEntity(0).getString()), msg.getEntity(1).getString(), Double.valueOf(msg.getEntity(2).toString()));
                DBConnection conn = new DBConnection();
                conn.connect(HOST, PORT, "admin", "123456");
                conn.run(script);
                System.out.println(msg.getEntity(0).getString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    };
}
