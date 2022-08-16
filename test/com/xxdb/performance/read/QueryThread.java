package com.xxdb.performance.read;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.performance.PerformanceReadTest;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.xxdb.performance.PerformanceReadTest.*;

public class QueryThread implements Runnable {
	public static AtomicInteger cdl = new AtomicInteger(0);

	public static int port;
	//public static int queryNum;
	private DBConnection conn;
	public static String sql;
	public static String preSql;
	//public static String type;
	public static BasicDateVector dayList;
	public static BasicStringVector idList;
	public static BasicStringVector BSFlagList = new BasicStringVector(new String[]{"N","B","S"});
	public static String queryColName = "col1";
	public static AtomicLong totalCount = new AtomicLong(0);
	public static AtomicLong minSt = new AtomicLong(Long.MAX_VALUE);
	public static AtomicLong maxEd = new AtomicLong(Long.MIN_VALUE);
	private int id;
	public static volatile boolean isInterrupt = false;


	public QueryThread(int id) throws IOException {
		this.id = id;
		this.conn = new DBConnection(false, false, true);
		String[] node = nodeList[id % nodeList.length].split(":");
		String ip = node[0];
		int port = Integer.parseInt(node[1]);
		this.conn.connect(ip, port, "admin", "123456");
		this.conn.run(String.format("pt = loadTable('%s','%s')", dbName, tableName));
	}

	@Override
	public void run() {
		String script = null;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
		String sqlFormat = "";
		String[][] arguments = new String[0][];

		if (tableName.equals("snapshot"))
			queryColName = "DateTime";
		if (tableName.equals("tick"))
			queryColName = "TradeTime";
		if (tableName.equals("entrust"))
			queryColName = "TransactTime";
		if (type.equals("-1")){
			sqlFormat = "select top 49 * from pt where date(%s) = %s,  SecurityID = `%s, TradeBSFlag = `%s;";
			arguments = new String[queryNum][4];
			for (int i = 0; i < queryNum; i++) {
				arguments[i][0] = queryColName;
				int r = (int) (Math.random() * dayList.rows());
				arguments[i][1] = dayList.getDate(r).format(formatter);
				arguments[i][2] = idList.getString((int) (Math.random() * idList.rows()));
				arguments[i][3] = BSFlagList.getString((int) (Math.random() * BSFlagList.rows()));
			}
		}else if (type.equals("0")) {
			sqlFormat = "select * from pt where date(%s) = %s,  SecurityID = `%s;";
			arguments = new String[queryNum][3];
			for (int i = 0; i < queryNum; i++) {
				arguments[i][0] = queryColName;
				int r = (int) (Math.random() * dayList.rows());
				if (queryColName == "TradeTime"){
					arguments[i][1] = "2021.12.08";
				}else {
					arguments[i][1] = dayList.getDate(r).format(formatter);
				}
				arguments[i][2] = idList.getString((int) (Math.random() * idList.rows()));
			}
		} else if (type.equals("1")){
			//sqlFormat = "select * from pt where date(%s) = %s;";
			if (queryColName == "TradeTime"){
				//sqlFormat = "select * from pt where date(%s) = %s limit 1909999;";
				sqlFormat = "select * from pt where date(%s) = %s;";
			}else if(queryColName == "DateTime"){
				sqlFormat = "select * from pt where date(%s) = %s;";
				//sqlFormat = "select * from pt where date(%s) = %s limit 200000;";
			}else {
				sqlFormat = "select * from pt where date(%s) = %s;";
				//sqlFormat = "select * from pt where date(%s) = %s limit 1000000;";
			}
			arguments = new String[queryNum][2];
			for (int i = 0; i < queryNum; i++) {
				arguments[i][0] = queryColName;
				if (queryColName == "TradeTime"){
					arguments[i][1] = "2021.12.08";
				}else {
					arguments[i][1] = dayList.getDate((int) (Math.random() * dayList.rows())).format(formatter);
				}
			}
		}

		try {
			long count = 0;
			long st = System.currentTimeMillis();
			BasicTable table;
			if (type.equals("2")){
				for (int i = 0; i < queryNum; i++) {
					long begin = System.currentTimeMillis();
					table = (BasicTable) this.conn.run(sql);
					long end = System.currentTimeMillis();
					count += table.rows();
					if (queryNum%100 == 0){
						//PerformanceReadTest.mtw.insert(QpsQuery.queryTableName, type, QpsQuery.threadNum, queryNum, id + "", i, end - begin, Long.parseLong(table.rows() + ""), "", begin + Utils.timeDelta, end + Utils.timeDelta);
					}
				}
			}
			else {
				for (int i = 0; i < queryNum; i++) {
					script = String.format(sqlFormat, arguments[i]);
					long begin = System.currentTimeMillis();
					table = (BasicTable) this.conn.run(script);
					long end = System.currentTimeMillis();
					count += table.rows();
					//PerformanceReadTest.mtw.insert(PerformanceReadTest.queryTableName, type, PerformanceReadTest.threadNum, queryNum, id + "", i, end - begin, Long.parseLong(table.rows() + ""), "", begin + Utils.timeDelta, end + Utils.timeDelta);
					//System.out.println("cueernt thread:" + id + ",count:" + count);
				}
			}
			long ed = System.currentTimeMillis();
			totalCount.addAndGet(count);
			minSt.set(Math.min(minSt.get(), st));
			maxEd.set(Math.max(maxEd.get(), ed));
			this.conn.close();
			cdl.addAndGet(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(script);
		}


	}

}
