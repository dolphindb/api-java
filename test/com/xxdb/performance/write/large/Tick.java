package com.xxdb.performance.write.large;

import com.xxdb.performance.write.ResWriter;
import com.xxdb.performance.read.Utils;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.performance.PerformanceWriteTest;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class Tick {
	public static DBConnection conn = new DBConnection();

	public static int threadNum = 10;
	public static String[] nodeList;
	public static int port;
	public static String dbName = "dfs://SH_TSDB_tick";
	public static String tableName = "tick";
	public static String writeDBName = "dfs://tick_table";
	public static String writeTableName = "tick";
	public static Double size = 100.0;
	public static int rows = 500;
	public static int insertCount = 10;
	public static DecimalFormat df = new DecimalFormat("#.00");

	public static void init2(String ienv,String[] inodeList,int ithreadNum,int irows,int iinsertCount) {
		String env = ienv;
		nodeList = inodeList;
		threadNum = ithreadNum;
		rows = irows;
		insertCount = iinsertCount;
		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
		System.out.println("\tthreadNum = " + threadNum);
		System.out.println("\trows = " + rows);
		System.out.println("\tinsertCount = " + insertCount);
		TickTableWriter.nodeList = nodeList;
	}
	public static BasicTable loadData(String sql) throws IOException {
		return (BasicTable) conn.run(sql);
	}

	public static void write(ArrayList<ArrayList<BasicTable>> lists) throws Exception {
		for (int i = 0; i < lists.size(); i++) {
			new Thread(new TickTableWriter(i, lists.get(i),writeDBName,writeTableName,insertCount)).start();
		}
	}

	public static void start2(String ienv,String[] inodeList,int ithreadNum,int irows,int iinsertCount) throws Exception {
		init2(ienv,inodeList,ithreadNum,irows,iinsertCount);
		String ip = nodeList[2 % nodeList.length].split(":")[0];
		int port = Integer.parseInt(nodeList[2 % nodeList.length].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		ArrayList<ArrayList<BasicTable>> lists = new ArrayList<>();

		for (int i = 1; i <= threadNum; i++) {
			conn.run(String.format("t = select top %s * from loadTable('%s','%s') where date(TradeTime) = 2021.12.08;",rows,dbName,tableName));
			ArrayList<BasicTable> basicTables = new ArrayList<>();
			String sql = String.format("update t set channel = %s;t;",i);
			BasicTable basicTable = loadData(sql);
			basicTables.add(basicTable);
			for (int j = 0; j < insertCount; j++) {
				sql = "update t set TradeTime = temporalAdd(TradeTime,1,'d');t;";
				basicTable = loadData(sql);
				basicTables.add(basicTable);
			}
			lists.add(basicTables);
		}

		// countdown
		PerformanceWriteTest.writeFlag.decrementAndGet();
		while (true){
			Thread.sleep(1);
			if (PerformanceWriteTest.writeFlag.get() == 0 )
				break;
		}
		System.out.println("tick start");
		long st = System.currentTimeMillis();
		write(lists);
		while (true) {
			Thread.sleep(1);
			if (TickTableWriter.cdl.get() == threadNum)
				break;
		}
		long ed = System.currentTimeMillis();
		double cost = (ed - st) / 1000.0;
		ResWriter.mtw.insert("tick", "1", threadNum, cost, rows, rows * insertCount * threadNum / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
		System.out.printf("Data : Tick, rows : %s, size : %s MB, Thread : %s, Insert : %s, cost : %s s, RPS : %s, 吞吐量 : %s, Per RPS : %s, StartTime : %s, EndTime : %s", rows, df.format(size) , threadNum, insertCount, cost, df.format(rows*insertCount*threadNum / cost), df.format(size*insertCount*threadNum / cost), df.format(rows*insertCount*threadNum / cost / threadNum), Utils.timeStamp2Date(st),Utils.timeStamp2Date(ed));
		System.out.println("complete");
	}
}
