package com.xxdb.performance.read;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.Entity;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

public class QpsQuery {

	private static String mode;
	private static String type;
	private static String[] nodeList;
	private static String user;
	private static String password;
	private static String dayDBName = "dfs://dayDB";
	private static String dayTableName = "day";
	private static String symDBName = "dfs://symDB";
	private static String symTableName = "sym";
	public static String queryDBName;
	public static String queryTableName;
	private static String clientIp;
	private static int clientPort;
	public static int threadNum;
	public static int queryNum;
	public static DecimalFormat df = new DecimalFormat("#.00");
	public static MultithreadedTableWriter mtw;

	public static void init() throws Exception {
		init("DolphinDB.setting");
	}

	public static void init(String path) throws Exception {
		System.out.println("\tmode = " + mode);
		System.out.println("\ttype = " + type);
		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
		System.out.println("\tuser = " + user);
		System.out.println("\tpassword = " + password);
		System.out.println("\tqueryDBName = " + queryDBName);
		System.out.println("\tqueryTableName = " + queryTableName);
		System.out.println("\tdayDBName = " + dayDBName);
		System.out.println("\tdayTableName = " + dayTableName);
		System.out.println("\tthreadNum = " + threadNum);
		System.out.println("\tqueryNum = " + queryNum);
		System.out.println("\tclientIp = " + clientIp);
		System.out.println("\tclientPort = " + clientPort);
	}

	public static void query(int threadNum) throws IOException {
		for (int i = 1; i <= threadNum; i++) {
			new Thread(new QueryThread(i)).start();
		}
	}

	public static void start(String path) throws Exception {
		init(path);
		DBConnection conn = new DBConnection(false,false,true);
		if (!type.equals("2")) {
			try {
				String ip = nodeList[0].split(":")[0];
				int port = Integer.parseInt(nodeList[0].split(":")[1]);
				conn.connect(ip, port, user, password);
				QueryThread.dayList = (BasicDateVector) conn.run(String.format("exec DateTime from loadTable('%s', '%s')", dayDBName, dayTableName));
				QueryThread.idList = (BasicStringVector) conn.run(String.format("exec SecurityID from loadTable('%s', '%s')", symDBName, symTableName));
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		DBConnection clientConn = new DBConnection();
		clientConn.connect(clientIp,clientPort,"admin","123456");
		Entity run = clientConn.run("c = exec count(*) from objs(true) where name=`queryResult and form=`TABLE\n" +
				"if(c == 0){\n" +
				"\tcolnames = `data`type`threadNum`queryNum`threadName`times`cost`rows`sqlMsg`begin`end\n" +
				"\tcoltype = [SYMBOL,SYMBOL,INT,INT,SYMBOL,INT,LONG,LONG,STRING,TIMESTAMP,TIMESTAMP]\n" +
				"\tshare table(1:0,colnames,coltype) as `queryResult\n" +
				"}");
		clientConn.close();

		mtw = new MultithreadedTableWriter(clientIp, clientPort, "admin", "123456", "", "queryResult",
				false, false, null, 100, 0.001f, 1, "threadName");

		query(threadNum);

		while (true) {
			Thread.sleep(1);
			if (QueryThread.cdl.get() == threadNum)
				break;
		}

		long count = QueryThread.totalCount.get();
		double cost = (QueryThread.maxEd.get() - QueryThread.minSt.get())/1000;
		if (cost == 0) cost =1.0;
		double qps = queryNum * threadNum / cost;
		double rps = count / cost;
		System.out.printf("Total Count : %s, Cost : %s s,QPS : %s, Per Thread QPS : %s, RPS : %s, Min StartTime : %s, Max EndTime : %s", count, df.format(cost), df.format(qps), df.format(qps / threadNum), df.format(rps),Utils.timeStamp2Date(QueryThread.minSt.get()), Utils.timeStamp2Date(QueryThread.maxEd.get()));
		System.out.println();
		mtw.waitForThreadCompletion();
	}
}
