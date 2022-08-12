package com.xxdb.performance.write.tiny;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import com.xxdb.performance.PerformanceWriteTest;
import com.xxdb.performance.write.ResWriter;
import com.xxdb.performance.read.Utils;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EntrustWriter {

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
	private static String dbName = "dfs://entrust_mtw";
	private static String tableName = "entrust";
	private static String streamTableName = "entrustMTWStream";
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
//		csvPath = setting.getByGroup("entrust.csvPath", env);
//		rows = Integer.parseInt(setting.getByGroup("rows", env));
//		writeDay = setting.getByGroup("writeDay", env);
//		System.out.println("\tenv = " + env);
//		System.out.println("\tmode = " + mode);
//		System.out.println("\ttype = " + type);
//		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
//		System.out.println("\tinsertNum = " + insertNum);
//		System.out.println("\twriteNum = " + writeNum);
//		System.out.println("\tbatchSize = " + batchSize);
//		System.out.println("\tdbName = " + dbName);
//		System.out.println("\ttableName = " + tableName);
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
		System.out.println("\tdbName = " + dbName);
		System.out.println("\ttableName = " + tableName);
		System.out.println("\trows = " + rows);
		System.out.println("\twriteDay = " + writeDay);
	}

	public static BasicTable loadData(String sql) throws IOException {
		return (BasicTable) conn.run(sql);
	}

//	public static List<Object[]> reverseOrder(String csvPath) {
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
//			if (i == rows.size()){
//				array[9] = array[9].toString().substring(1,array[9].toString().length()-1);
//			}
//
//			array[1] = new BasicTimestamp(LocalDateTime.parse((CharSequence) array[1], formatter2));
//			array[2] = Integer.parseInt((String) array[2]);
//			array[3] = Double.parseDouble((String) array[3]);
//			array[4] = Integer.parseInt((String) array[4]);
//			array[7] = Integer.parseInt((String) array[7]);
//			array[8] = Integer.parseInt((String) array[8]);
//			array[9] = Integer.parseInt((String) array[9]);
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

//	public static void write(String key, List data, MultithreadedTableWriter mtw) throws Exception {
//		for (int i = 0; i < insertNum; i++) {
//			new Thread(new EntrustDataWriter(key, i, insertNum, data, mtw)).start();
//		}
//	}

	// 读表写入
	public static void write(String key, ArrayList data, MultithreadedTableWriter mtw) throws Exception {
		if (type.equals("2") || type.equals("3")){
			for (int i = 0; i < insertNum; i++) {
				new Thread(new EntrustSlowDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}else {
			for (int i = 0; i < insertNum; i++) {
				new Thread(new EntrustDataWriter(key, i, insertNum, data, mtw)).start();
			}
		}
	}

//	public static void start(String path) throws Exception {
//
//		init(path);
//		String ip = nodeList[0 % nodeList.length].split(":")[0];
//		int port = Integer.parseInt(nodeList[0 % nodeList.length].split(":")[1]);
//		conn.connect(ip, port, "admin", "123456");
//		conn.run(String.format("def getData(hashId){\n" +
//				"         return select top %s * from loadTable(\"dfs://SH_TSDB_entrust\", \"entrust\") where date(TransactTime) = %s, hashBucket(SecurityID,20) =hashId\n" +
//				"}\n" +
//				"result1 = ploop(getData, 0..19).unionAll(false)\n" +
//				"share result1 as mtwEntrustQueryResult",rows / 20,writeDay));
//		MultithreadedTableWriter mtwOrder;
//		if (type.equals("3")) {
//			mtwOrder = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
//					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
//		}else {
//			mtwOrder = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
//					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
//		}
//		String sql = "t = select * from mtwEntrustQueryResult limit 100;t;";
//		BasicTable basicTable = loadData(sql);
//		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);
//		// countdown
//		PerformanceWriteTest.writeFlag.decrementAndGet();
//		while (true){
//			Thread.sleep(1);
//			if (PerformanceWriteTest.writeFlag.get() == 0 )
//				break;
//		}
//		System.out.println("entrust start");
//
//		long st = System.currentTimeMillis();
//		write("order", reverse, mtwOrder);
//		// 等待 MTW 插入完成
//		MultithreadedTableWriter.Status statusOrder;
//		do {
//			Thread.sleep(1);
//			statusOrder = mtwOrder.getStatus();
//		} while (statusOrder.sentRows < rows);
//		long ed = System.currentTimeMillis();
//		double cost = (ed - st) / 1000.0;
//		ResWriter.mtw.insert("entrust", "0", writeNum, cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
//		System.out.printf("Data : entrust, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, com.xxdb.performance.read.Utils.df.format(cost), com.xxdb.performance.read.Utils.df.format(statusOrder.sentRows / cost), Utils.df.format(statusOrder.sentRows / cost / writeNum));
//		System.out.println();
//		mtwOrder.waitForThreadCompletion();
//	}

	public static void start2(String ienv,String imode,String itype,String[] inodeList,
							  String iuser,String ipassword,int ibatchSize,int iwriteNum,
							  int iinsertNum,int irows,String iwriteDay) throws Exception {

		init2(ienv,imode,itype,inodeList,iuser,ipassword,ibatchSize,iwriteNum,iinsertNum,irows,iwriteDay);
		String ip = nodeList[0 % nodeList.length].split(":")[0];
		int port = Integer.parseInt(nodeList[0 % nodeList.length].split(":")[1]);
		conn.connect(ip, port, "admin", "123456");
		String script = String.format("def getData(hashId){\n" +
				"         return select top %s * from loadTable(\"dfs://SH_TSDB_entrust\", \"entrust\") where date(TransactTime) = %s, hashBucket(SecurityID,20) =hashId\n" +
				"}\n" +
				"result1 = ploop(getData, 0..19).unionAll(false)\n" +
				"share result1 as mtwEntrustQueryResult",rows / 20,writeDay);
		conn.run(script);
		MultithreadedTableWriter mtwOrder;
		if (type.equals("3")) {
			mtwOrder = new MultithreadedTableWriter(ip, port, user, password, streamTableName, "",
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}else {
			mtwOrder = new MultithreadedTableWriter(ip, port, user, password, dbName, tableName,
					false, false, null, batchSize, 0.001f, writeNum, "SecurityID");
		}
		String sql = "t = select * from mtwEntrustQueryResult;t;";
		BasicTable basicTable = loadData(sql);
		rows = basicTable.rows();
		ArrayList<ArrayList<Object>> reverse = reverse(basicTable);
		// countdown
		PerformanceWriteTest.writeFlag.decrementAndGet();
		while (true){
			Thread.sleep(1);
			if (PerformanceWriteTest.writeFlag.get() == 0 )
				break;
		}
		System.out.println("entrust start");

		long st = System.currentTimeMillis();
		write("order", reverse, mtwOrder);
		// 等待 MTW 插入完成
		MultithreadedTableWriter.Status statusOrder;
		do {
			//Thread.sleep(1);
			TimeUnit.SECONDS.sleep(1);
			statusOrder = mtwOrder.getStatus();
		} while (statusOrder.sentRows < rows*0.95);
		long ed = System.currentTimeMillis();
		double cost = (ed - st) / 1000.0;
		ResWriter.mtw.insert("entrust", "0", writeNum, cost, rows, rows / cost, st + Utils.timeDelta, ed + Utils.timeDelta);
		System.out.printf("Data : entrust, rows : %s, Insert Thread : %s, Write Thread : %s, cost : %s s, RPS : %s, Per RPS : %s", rows, insertNum, writeNum, com.xxdb.performance.read.Utils.df.format(cost), com.xxdb.performance.read.Utils.df.format(statusOrder.sentRows / cost), Utils.df.format(statusOrder.sentRows / cost / writeNum));
		System.out.println();
		mtwOrder.waitForThreadCompletion();
	}
}
