package com.xxdb.performance.write.HA;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.read.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class HAMtw {

	public static DBConnection conn = new DBConnection();
	// csv大小(MB)
	private static double size;
//	private static Setting setting;
	private static String mode;
	private static String type;
	private static String[] nodeList;
	private static String user;
	private static String password;
	public static int batchSize = 10000;
	public static int writeNum = 10;
	public static int insertNum = 10;
	public static String csvPath;
	private static String dbName = "dfs://tick_mtw";
	private static String tableName = "tick";
	private static int rows;

//	public static void init(String path) {
//		setting = new Setting(FileUtil.touch(path), CharsetUtil.CHARSET_UTF_8, false);
//		String env = setting.get("env");
//		mode = setting.get("mode");
//		type = setting.get("type");
//		nodeList = setting.getByGroup("node.list", env).split(",");
//		user = setting.getByGroup("user", env);
//		password = setting.getByGroup("password", env);
//		batchSize = Integer.parseInt(setting.getByGroup("batchSize", env));
//		writeNum = Integer.parseInt(setting.getByGroup("writeNum", env));
//		insertNum = Integer.parseInt(setting.getByGroup("insertNum", env));
//		csvPath = setting.getByGroup("tick.csvPath", env);
//		rows = Integer.parseInt(setting.getByGroup("rows", env));
//		System.out.println("\tenv = " + env);
//		System.out.println("\tmode = " + mode);
//		System.out.println("\ttype = " + type);
//		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
//		System.out.println("\tuser = " + user);
//		System.out.println("\tpassword = " + password);
//		System.out.println("\tinsertNum = " + insertNum);
//		System.out.println("\twriteNum = " + writeNum);
//		System.out.println("\tbatchSize = " + batchSize);
//		System.out.println("\ttrade.csvPath = " + csvPath);
//		System.out.println("\trows = " + rows);
//	}

	public static BasicTable loadData(String sql) throws IOException {
		String ip = nodeList[0].split(":")[0];
		int port = Integer.parseInt(nodeList[0].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		return (BasicTable) conn.run(sql);
	}

	public static ArrayList<ArrayList<Object>> reverse(BasicTable table) {
		ArrayList<ArrayList<Object>> list = new ArrayList<>();
		for (int i = 0; i < table.rows(); i++) {
			ArrayList<Object> row = new ArrayList<>();
			for (int j = 0; j < table.columns(); j++) {
				Vector column = table.getColumn(j);
				if (column instanceof BasicArrayVector) {
					if (column.getDataType() == Entity.DATA_TYPE.DT_DOUBLE_ARRAY) {
						ArrayList<Double> doubles = new ArrayList<>();
						for (int k = 0; k < 10; k++) {
							Double aDouble = ((BasicDouble) column.get(i * 10 + k)).getDouble();
							doubles.add(aDouble);
						}
						row.add(doubles.toArray(new Double[doubles.size()]));
					} else {
						ArrayList<Integer> ints = new ArrayList<>();
						for (int k = 0; k < 10; k++) {
							int aInt = ((BasicInt) column.get(i * 10 + k)).getInt();
							ints.add(aInt);
						}
						row.add(ints.toArray(new Integer[ints.size()]));
					}
				} else {
					row.add(table.getColumn(j).get(i));
				}
			}
			list.add(row);
		}
		return list;
	}

	// 读表写入
	public static void write(String key, ArrayList data, MultithreadedTableWriter mtw) throws Exception {
		for (int i = 0; i < insertNum; i++) {
			new Thread(new DataWriter(key, i, insertNum, data, mtw)).start();
		}
	}

//	public static void start(String path) throws Exception {
//		init(path);
//		String ip = nodeList[0].split(":")[0];
//		int port = Integer.parseInt(nodeList[0].split(":")[1]);
//		MultithreadedTableWriter mtwTrade = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
//				false, true, nodeList, batchSize, 0.001f, writeNum, "SecurityID");
//		String sql = String.format("t = select top %s * from loadTable(\"dfs://SH_TSDB_tick\",\"tick\") where date(TradeTime) = 2021.12.03;t",rows);
//		BasicTable basicTable = loadData(sql);
//		size = ((BasicDouble)conn.run("memSize(t) \\ 1024 \\ 1024;")).getDouble();
//		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);
//
//		System.out.println("HA tick start");
//
//
//		long st = System.currentTimeMillis();
//
//		write("tick", reverse, mtwTrade);
//
//		// 等待 MTW 插入完成
//		MultithreadedTableWriter.Status statusTrade;
//		do {
//			Thread.sleep(1);
//			statusTrade = mtwTrade.getStatus();
//		} while (statusTrade.sentRows < rows);
//		long ed = System.currentTimeMillis();
//		double cost = (ed - st) / 1000.0;
//		System.out.printf("Data : tick, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, 吞吐量 : %s MB/s, Per RPS : %s", rows, insertNum, writeNum, com.xxdb.performance.read.Utils.df.format(cost), com.xxdb.performance.read.Utils.df.format(statusTrade.sentRows / cost), com.xxdb.performance.read.Utils.df.format(size / cost), Utils.df.format(statusTrade.sentRows / cost / writeNum));
//		System.out.println();
//		mtwTrade.waitForThreadCompletion();
//	}

//	public static void main(String[] args) throws Exception {
//		start(args[0]);
//	}
}
