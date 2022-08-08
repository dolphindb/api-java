package com.xxdb.performance.stream;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.performance.PerformanceReadTest;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Sub {
	private static DBConnection conn;
	public static String HOST = "localhost";
	public static Integer PORT = 8848;
	public static ThreadedClient client;
	public static Integer subscribePORT = 8892;
//	private static Setting setting;
	private static String[] nodeList;
	private static String clientIp;
	private static int clientPort;
	private static int subRows;

	public static void init2(String ienv,String[] inodeList,int isubscribePORT,String iclientIp,int iclientPort,String iHOST,int iPORT,int isubRows) throws Exception {
		String env = ienv;
		nodeList = inodeList;
		subscribePORT = isubscribePORT;
		System.out.println("\tenv = " + env);
		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
		System.out.println("\tsubscribePORT = " + subscribePORT);
		clientIp = iclientIp;
		clientPort = iclientPort;
		HOST = iHOST;
		PORT = iPORT;
	}

	public void createStreamTable() throws IOException {
		conn.login("admin", "123456", false);
		conn.run("def cleanEnvironment(){\n" +
				"        try{ dropStreamTable(`sharedTick) } catch(ex){ print(ex) }\n" +
				"        undef all\n" +
				"}\n" +
				"cleanEnvironment()\n" +
				"go");
		conn.run("name = `SecurityID`TradeTime`TradePrice`TradeQty`TradeAmount`BuyNo`SellNo`TradeIndex`ChannelNo`TradeBSFlag`BizIndex\n" +
				"type = `SYMBOL`TIMESTAMP`DOUBLE`INT`DOUBLE`INT`INT`INT`INT`SYMBOL`INT\n" +
				"schemaTable = streamTable(1:0, name, type)\n" +
				"share schemaTable as sharedTick\n" +
				"go");
	}

	public static class SampleMessageHandler implements MessageHandler {
		private AtomicLong count = new AtomicLong();
		public long start = 0;
		public long end = 0;
		public long rows = 33613835;
		public boolean finished = false;
		private static DBConnection connection = new DBConnection();
		static {
			try {
				connection.connect(clientIp,clientPort,"admin","123456");
				connection.run("colnames = `begin`end`cost`count`indicator\n" +
						"coltype = [TIMESTAMP,TIMESTAMP,LONG,LONG,DOUBLE]\n" +
						"share table(1:0,colnames,coltype) as `streamResult");
				connection.run("def saveData(x1,x2,x3,x4,x5){ streamResult.tableInsert(x1,x2,x3,x4,x5)}");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void doEvent(IMessage msg) {
			count.incrementAndGet();
			if (count.get() == 1)
				start = System.currentTimeMillis();
			if (count.get() == rows) {
				end = System.currentTimeMillis();
				finished = true;
				BasicTimestampVector startV = new BasicTimestampVector(new long[]{start});
				BasicTimestampVector endV = new BasicTimestampVector(new long[]{end});
				BasicLongVector costV = new BasicLongVector(new long[]{(end - start) / 1000});
				BasicLongVector countV = new BasicLongVector(new long[]{count.get()});
				BasicDoubleVector indicatorV = new BasicDoubleVector(new double[]{Double.parseDouble(PerformanceReadTest.df.format((double) count.get() * 1000 / (end - start)))});
				List<Entity> args = new ArrayList<Entity>(5);
				args.add(startV);
				args.add(endV);
				args.add(costV);
				args.add(countV);
				args.add(indicatorV);
				try {
					connection.run("saveData",args);
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("Done");
				connection.close();
				System.exit(0);
			}
		}
	}

	public void ThreadedClient() throws SocketException {
		ThreadedClient client = new ThreadedClient(subscribePORT);
		try {
			client.subscribe(HOST, PORT, "sharedTick", "subJava", new SampleMessageHandler());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void replay() throws IOException {
		conn.run("tick = loadTable(\"dfs://SH_TSDB_tick\", \"tick\")\n" +
				"tickData = replayDS(<select * from tick where date(TradeTime)=2021.12.08>, `TradeTime, `TradeTime, 08:00:00.000 + (1..8) * 3600000);" +
				"submitJob(\"replay\", \"replay\", replay{[tickData], [sharedTick], `TradeTime, `TradeTime, -1, true, 1})");
	}

	public static void start2(String ienv,String[] inodeList,int isubscribePORT
			,String iclientIp,int iclientPort,String iHOST,int iPORT,int isubRows) throws Exception {

		init2(ienv,inodeList,isubscribePORT,iclientIp,iclientPort,iHOST,iPORT,isubRows);
		conn = new DBConnection();
		try {
			System.out.println(conn.connect(HOST, PORT));
		} catch (IOException e) {
			System.out.println("Connection error");
			e.printStackTrace();
		}
		Sub sub = new Sub();
		// 01.create stream table
		try {
			sub.createStreamTable();
		} catch (IOException e) {
			System.out.println("Writing error");
		}
		// 02.sub
		try {
			sub.ThreadedClient();
		} catch (IOException e) {
			System.out.println("Subscription error");
		}
		// 03.replay
		sub.replay();

	}
}
