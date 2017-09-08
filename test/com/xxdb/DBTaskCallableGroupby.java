package com.xxdb;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import com.xxdb.data.BasicTable;
import java.io.IOException;



public class DBTaskCallableGroupby implements Callable<String> {
	private DBConnection conn;
	private ArrayList<String>  symbolList;
	private String connStr;
	private long breath;
	
	public DBTaskCallableGroupby(DBConnection conn, ArrayList<String> symbolList, long breath) {
		this.conn = conn;
		this.symbolList = symbolList;
		this.connStr = conn.getHostName() + ":" + conn.getPort();
		this.breath = breath;
	}
	
	
    @Override
    public String call() throws Exception {
        Thread.sleep(this.breath);
		long total;
		String sql;
		if(symbolList.size()>1)
			sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol in [\"" + String.join("\",\"", symbolList) + "\"] group by symbol, date" ;
		else
			sql = "select  sum(bidSize), avg(bidPrice) as avgBidPrice,  avg(underlyerLastBidPrice) as avgUnderlyerPrice from TAQ where symbol =\"" + symbolList.get(0) + "\"] group by date" ;
		System.out.println(sql);
		try {			
			BasicTable table = (BasicTable)conn.run(sql);
			total = table.rows();
			
		} catch (IOException e) {
			e.printStackTrace();
			return connStr + ":" +sql + " failed";
		}
        return connStr + ": " + total + ":" +sql ;
    }
}