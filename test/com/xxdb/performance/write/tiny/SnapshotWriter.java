package com.xxdb.performance.write.tiny;

import com.xxdb.performance.write.ResWriter;
import com.xxdb.performance.read.Utils;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.PerformanceWriteTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class SnapshotWriter {

	public static DBConnection conn = new DBConnection();
	// csv大小(MB)
	private static double size;
//	private static Setting setting;
	private static String mode;
	private static String type;
	public static String[] nodeList;
	private static String user;
	private static String password;
	public static int batchSize = 10000;
	public static int writeNum = 10;
	public static int insertNum = 10;
	public static String csvPath;
	private static final String dbName = "dfs://snapshot_mtw";
	private static final String tableName = "snapshot";
	private static String streamTableName = "snapshotMTWStream";
	private static int rows;
	private static String writeDay = "2021.12.01";

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
//		rows = Integer.parseInt(setting.getByGroup("rows", env));
//		writeDay = setting.getByGroup("writeDay", env);
//		System.out.println("\tenv = " + env);
//		System.out.println("\tmode = " + mode);
//		System.out.println("\ttype = " + type);
//		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
//		System.out.println("\tinsertNum = " + insertNum);
//		System.out.println("\twriteNum = " + writeNum);
//		System.out.println("\tbatchSize = " + batchSize);
//		System.out.println("\trows = " + rows);
//		System.out.println("\twriteDay = " + writeDay);
//	}

	public static void init2(String ienv,String imode,String itype,String[] inodeList,
							 String iuser,String ipassword,int ibatchSize,int iwriteNum,
							 int iinsertNum,int irows,String iwriteDay) {
		String env = ienv;
		mode = imode;
		type = itype;
		nodeList = inodeList;
		user = iuser;
		password = ipassword;
		batchSize = ibatchSize;
		writeNum = iwriteNum;
		insertNum = iinsertNum;
		//csvPath = setting.getByGroup("entrust.csvPath", env);
		rows = irows;
		writeDay = iwriteDay;
		System.out.println("\tenv = " + env);
		System.out.println("\tmode = " + mode);
		System.out.println("\ttype = " + type);
		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
		System.out.println("\tinsertNum = " + insertNum);
		System.out.println("\twriteNum = " + writeNum);
		System.out.println("\tbatchSize = " + batchSize);
		System.out.println("\trows = " + rows);
		System.out.println("\twriteDay = " + writeDay);
	}

	public static Double[] toDoubleArrayVector(String str){

		String[] split = str.split(",");
		Double[] ret = new Double[split.length];
		for (int i = 0; i < split.length; i++) {
			ret[i] = Double.parseDouble(split[i]);
		}
		return ret;
	}


	public static Integer[] toIntArrayVector(String str){
		String[] split = str.split(",");
		Integer[] ret = new Integer[split.length];
		for (int i = 0; i < split.length; i++) {
			ret[i] = Integer.parseInt(split[i]);
		}
		return ret;
	}

	public static BasicTable loadData(String sql) throws IOException {
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
						int len = 10;
						if (j == 13 || j == 17)
							len = 50;
						for (int k = 0; k < len; k++) {
							int aInt = ((BasicInt) column.get(i * len + k)).getInt();
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
		if (type.equals("2") || type.equals("3")){
			for (int i = 0; i < insertNum; i++) {
				new Thread(new SnapshotSlowDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}else {
			for (int i = 0; i < insertNum; i++) {
				new Thread(new SnapshotDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}
	}

//	public static void start(String path) throws Exception {
//		init(path);
//		String ip = nodeList[1 % nodeList.length].split(":")[0];
//		int port = Integer.parseInt(nodeList[1 % nodeList.length].split(":")[1]);
//		conn.connect(ip, port, "admin", "123456");
//
//		conn.run(String.format("def getData(hashId){\n" +
//				"         return select top %s * from loadTable(\"dfs://SH_TSDB_snapshot_ArrayVector\", \"snapshot\") where date(DateTime) = %s, hashBucket(SecurityID,30) =hashId\n" +
//				"}\n" +
//				"result2 = ploop(getData, 0..29).unionAll(false)\n" +
//				"share result2 as mtwSnapQueryResult",rows / 40, writeDay));
//		MultithreadedTableWriter mtw;
//		if (type.equals("3")) {
//			mtw = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
//					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
//		}else {
//			mtw = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
//					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
//		}
//
//		String sql = "t = select * from mtwSnapQueryResult;t;";
//		BasicTable basicTable = loadData(sql);
//		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);		// countdown
//		PerformanceWriteTest.writeFlag.decrementAndGet();
//		while (true){
//			Thread.sleep(1);
//			if (PerformanceWriteTest.writeFlag.get() == 0 )
//				break;
//		}
//		System.out.println("snapshot start");
//		long st = System.currentTimeMillis();
//		write("snapshot", reverse, mtw);
//		// 等待 MTW 插入完成
//		MultithreadedTableWriter.Status statusSnapshot;
//		do {
//			Thread.sleep(1);
//			statusSnapshot = mtw.getStatus();
//		} while (statusSnapshot.sentRows < rows);
//		long ed = System.currentTimeMillis();
//		double cost = (ed - st) / 1000.0;
//		ResWriter.mtw.insert("snapshot", "0", writeNum,  cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
//		System.out.printf("Data : snapshot, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, Utils.df.format(cost),Utils.df.format(statusSnapshot.sentRows / cost), Utils.df.format(statusSnapshot.sentRows / cost / writeNum));
//		System.out.println();
//		mtw.waitForThreadCompletion();
//	}

	public static void start2(String ienv,String imode,String itype,String[] inodeList,
							  String iuser,String ipassword,int ibatchSize,int iwriteNum,
							  int iinsertNum,int irows,String iwriteDay) throws Exception {
		init2(ienv,imode,itype,inodeList,iuser,ipassword,ibatchSize,iwriteNum,iinsertNum,irows,iwriteDay);
		String ip = nodeList[1 % nodeList.length].split(":")[0];
		int port = Integer.parseInt(nodeList[1 % nodeList.length].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		String script = String.format("def getData(hashId){\n" +
				"         return select top %s * from loadTable(\"dfs://SH_TSDB_snapshot_ArrayVector\", \"snapshot\") where date(DateTime) = %s, hashBucket(SecurityID,30) =hashId\n" +
				"}\n" +
				"result2 = ploop(getData, 0..29).unionAll(false)\n" +
				"share result2 as mtwSnapQueryResult",rows / 20, writeDay);
		conn.run(script);
		MultithreadedTableWriter mtw;
		if (type.equals("3")) {
			mtw = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}else {
			mtw = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}

		String sql = "t = select * from mtwSnapQueryResult;t;";
		BasicTable basicTable = loadData(sql);
		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);		// countdown
		PerformanceWriteTest.writeFlag.decrementAndGet();
		while (true){
			Thread.sleep(1);
			if (PerformanceWriteTest.writeFlag.get() == 0 )
				break;
		}
		System.out.println("snapshot start");
		long st = System.currentTimeMillis();
		write("snapshot", reverse, mtw);
		// 等待 MTW 插入完成
		MultithreadedTableWriter.Status statusSnapshot;
		do {
			Thread.sleep(1);
			statusSnapshot = mtw.getStatus();
		} while (statusSnapshot.sentRows < rows*0.95);
		long ed = System.currentTimeMillis();
		double cost = (ed - st) / 1000.0;
		ResWriter.mtw.insert("snapshot", "0", writeNum,  cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
		System.out.printf("Data : snapshot, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, Utils.df.format(cost),Utils.df.format(statusSnapshot.sentRows / cost), Utils.df.format(statusSnapshot.sentRows / cost / writeNum));
		System.out.println();
		mtw.waitForThreadCompletion();
	}
}
