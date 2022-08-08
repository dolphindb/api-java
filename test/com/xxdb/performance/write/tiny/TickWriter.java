package com.xxdb.performance.write.tiny;

import com.xxdb.performance.write.ResWriter;
import com.xxdb.performance.read.Utils;
import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.PerformanceWriteTest;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TickWriter {

	public static DBConnection conn = new DBConnection();
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
	private static String dbName = "dfs://tick_mtw";
	private static String tableName = "tick";
	private static String streamTableName = "tickMTWStream";
	private static int rows;
	private static String writeDay = "2021.12.01";

	public static void init(String path) {
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
		System.out.println("\tmode = " + mode);
		System.out.println("\ttype = " + type);
		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
		System.out.println("\tinsertNum = " + insertNum);
		System.out.println("\twriteNum = " + writeNum);
		System.out.println("\tbatchSize = " + batchSize);
		System.out.println("\trows = " + rows);
		System.out.println("\twriteDay = " + writeDay);
	}

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

	public static BasicTable loadData(String sql) throws IOException {
		return (BasicTable) conn.run(sql);
	}

//	public static List<Object[]> reverseTrade(String csvPath) {
//
//		DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSS");
//		CsvReader reader = CsvUtil.getReader();
//		// 从文件中读取CSV数据
//		CsvData data = reader.read(FileUtil.file(csvPath));
//		List<CsvRow> rows = data.getRows();
//		List<Object[]> list = new ArrayList<>();
//		// 遍历行
//		int i = 1;
//		for (CsvRow csvRow : rows) {
//			if (i == 1) {
//				i++;
//				continue;
//			}
//			Object[] array = csvRow.getRawList().toArray();
//			if (i == rows.size()) {
//				array[10] = array[10].toString().substring(1, array[10].toString().length() - 1);
//			}
//			array[1] = new BasicTimestamp(LocalDateTime.parse((CharSequence) array[1], formatter2));
//			array[2] = Double.parseDouble((String) array[2]);
//			array[3] = Integer.parseInt((String) array[3]);
//			array[4] = Double.parseDouble((String) array[4]);
//			array[5] = Integer.parseInt((String) array[5]);
//			array[6] = Integer.parseInt((String) array[6]);
//			array[7] = Integer.parseInt((String) array[7]);
//			array[8] = Integer.parseInt((String) array[8]);
//			array[10] = Integer.parseInt((String) array[10]);
//			list.add(array);
//			i++;
//		}
//		return list;
//	}

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
		if (type.equals("2") || type.equals("3")){
			for (int i = 0; i < insertNum; i++) {
				new Thread(new TickSlowDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}else {
			for (int i = 0; i < insertNum; i++) {
				new Thread(new TickDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}
	}

	public static void start(String path) throws Exception {
		init(path);
		String ip = nodeList[2 % nodeList.length].split(":")[0];
		int port = Integer.parseInt(nodeList[2 % nodeList.length].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		conn.run(String.format("def getData(hashId){\n" +
				"         return select top %s * from loadTable(\"dfs://SH_TSDB_tick\", \"tick\") where date(TradeTime) = %s, hashBucket(SecurityID,20) =hashId\n" +
				"}\n" +
				"result3 = ploop(getData, 0..19).unionAll(false)\n" +
				"share result3 as mtwTickQueryResult", rows / 20, writeDay));

		MultithreadedTableWriter mtwTrade;
		if (type.equals("3")) {
			mtwTrade = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}else {
			mtwTrade = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}

		String sql = "t = select  * from mtwTickQueryResult;t;";
		BasicTable basicTable = loadData(sql);
		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);
		// countdown
		PerformanceWriteTest.writeFlag.decrementAndGet();
		while (true) {
			Thread.sleep(1);
			if (PerformanceWriteTest.writeFlag.get() == 0)
				break;
		}
		System.out.println("tick start");

		long st = System.currentTimeMillis();

		write("tick", reverse, mtwTrade);
		// 等待 MTW 插入完成
		MultithreadedTableWriter.Status statusTrade;
		do {
			Thread.sleep(1);
			statusTrade = mtwTrade.getStatus();
		} while (statusTrade.sentRows < rows*0.95);
		long ed = System.currentTimeMillis();
		double cost = (ed - st) / 1000.0;
		ErrorCodeInfo tick = ResWriter.mtw.insert("tick", "0", writeNum, cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
		System.out.println(tick);
		System.out.println("writeStatus: {0}\n" + statusTrade.toString());
		System.out.printf("Data : tick, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, Utils.df.format(cost),Utils.df.format(statusTrade.sentRows / cost), Utils.df.format(size / cost), Utils.df.format(statusTrade.sentRows / cost / writeNum));
		System.out.println();
		mtwTrade.waitForThreadCompletion();
	}

	public static void start2(String ienv,String imode,String itype,String[] inodeList,
							  String iuser,String ipassword,int ibatchSize,int iwriteNum,
							  int iinsertNum,int irows,String iwriteDay) throws Exception {
		init2(ienv,imode,itype,inodeList,iuser,ipassword,ibatchSize,iwriteNum,iinsertNum,irows,iwriteDay);
		String ip = nodeList[2 % nodeList.length].split(":")[0];
		int port = Integer.parseInt(nodeList[2 % nodeList.length].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		String script = String.format("def getData(hashId){\n" +
				"         return select top %s * from loadTable(\"dfs://SH_TSDB_tick\", \"tick\") where date(TradeTime) = %s, hashBucket(SecurityID,20) =hashId\n" +
				"}\n" +
				"result3 = ploop(getData, 0..19).unionAll(false)\n" +
				"share result3 as mtwTickQueryResult", rows / 20, writeDay);
		conn.run(script);

		MultithreadedTableWriter mtwTrade;
		if (type.equals("3")) {
			mtwTrade = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}else {
			mtwTrade = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}

		String sql = "t = select  * from mtwTickQueryResult;t;";
		BasicTable basicTable = loadData(sql);
		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);
		// countdown
		PerformanceWriteTest.writeFlag.decrementAndGet();
		while (true) {
			Thread.sleep(1);
			if (PerformanceWriteTest.writeFlag.get() == 0)
				break;
		}
		System.out.println("tick start");

		long st = System.currentTimeMillis();

		write("tick", reverse, mtwTrade);
		// 等待 MTW 插入完成
		MultithreadedTableWriter.Status statusTrade;
		do {
			//Thread.sleep(1);
			TimeUnit.SECONDS.sleep(1);
			statusTrade = mtwTrade.getStatus();
		} while (statusTrade.sentRows < rows);
		long ed = System.currentTimeMillis();
		double cost = (ed - st) / 1000.0;
		ErrorCodeInfo tick = ResWriter.mtw.insert("tick", "0", writeNum, cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
		System.out.println(tick);
		System.out.println("writeStatus: {0}\n" + statusTrade.toString());
		System.out.printf("Data : tick, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, Utils.df.format(cost),Utils.df.format(statusTrade.sentRows / cost), Utils.df.format(size / cost), Utils.df.format(mtwTrade.getStatus().sentRows / cost / writeNum));
		System.out.println();
		mtwTrade.waitForThreadCompletion();
	}
}
