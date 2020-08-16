package com.xxdb;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicTable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DBTaskCallable implements Callable<String> {
    private DBConnection conn;
    private ArrayList<String> symbolList;
    private String connStr;
    private long breath;
    private String date;
    private String queryType;

    public DBTaskCallable(DBConnection conn, ArrayList<String> symbolList, String date, String queryType, long breath) {
        this.conn = conn;
        this.date = date;
        this.symbolList = symbolList;
        this.queryType = queryType;
        this.connStr = conn.getHostName() + ":" + conn.getPort();
        this.breath = breath;
    }


    @Override
    public String call() throws Exception {
        Thread.sleep(this.breath);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date1 = new Date();
        //System.out.println( connStr + " running " + this.symbol + " " + date + " " + dateFormat.format(date1));
        long sum = 0;
        long total = 0;
        String sql;
        BasicTable table;
        try {
            long start = 0;
            int step = 100000;
            if (queryType.equalsIgnoreCase("groupbyMinute")) {
                sql = "select sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol=\"" + symbolList.get(0) + "\", date=" + date + " group by minute(time)";
                table = (BasicTable) conn.run(sql);
                System.out.println(dateFormat.format(date1) + "   " + connStr + "   " + sql);
            } else if (queryType.equalsIgnoreCase("groupbyDate")) {
                BasicDateVector dateVec = (BasicDateVector) conn.run("(" + date + "-4)..(" + date + "+5)");
                if (symbolList.size() > 1)
                    sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol in [\"" + String.join("\",\"", symbolList) + "\"], date>=" + dateVec.get(0).toString() + " and date<=" + dateVec.get(9).toString() + " group by symbol, date";
                else
                    sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol =\"" + symbolList.get(0) + "\", date>= " + dateVec.get(0).toString() + " and date<=" + dateVec.get(9).toString() + " group by date";
                System.out.println(dateFormat.format(date1) + "   " + connStr + "   " + sql);
                table = (BasicTable) conn.run(sql);

            } else {
                String symbol = symbolList.get(0);
                sql = "select  count(*) from TAQ where symbol=\"" + symbol + "\", date=" + date;
                table = (BasicTable) conn.run(sql);
                total = table.getColumn(0).get(0).getNumber().longValue();
                System.out.println(dateFormat.format(date1) + "   " + connStr + "   " + sql);
                while (sum < total) {
                    if (total <= step) {
                        sql = "select * from TAQ where symbol=\"" + symbol + "\", date=" + date;
                        System.out.println(dateFormat.format(date1) + "   " + connStr + "   " + sql);
                        table = (BasicTable) conn.run(sql);
                    } else {
                        sql = "select top " + start + ":" + (start + step) + " * from TAQ where symbol=\"" + symbol + "\", date=" + date;
                        System.out.println(dateFormat.format(date1) + "   " + connStr + "   " + sql);
                        table = (BasicTable) conn.run(sql);
                    }

                    start += step;
                    sum += table.rows();
                }
            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return connStr + ": " + String.join("\",\"", symbolList) + ": " + date + " Failed ";
        }

        if (queryType.equalsIgnoreCase("download")) {
            return dateFormat.format(date1) + ":" + connStr + ": " + String.join("\",\"", symbolList) + ": " + date + ": " + String.valueOf(total) + "==" + String.valueOf(sum) + " is " + String.valueOf(total == sum);
        } else {
            return dateFormat.format(date1) + ":" + connStr + ": " + String.join("\",\"", symbolList) + ": " + date + ": " + queryType + ": " + table.rows();
        }

    }
}