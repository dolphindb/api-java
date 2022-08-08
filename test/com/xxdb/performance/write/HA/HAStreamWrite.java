package com.xxdb.performance.write.HA;

import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.Arrays;


public class HAStreamWrite {

//	private static Setting setting;
	private static String mode;
	private static String type;
	private static String[] nodeList;
	private static String user;
	private static String password;
	private static String streamTableName = "haStreamTB";
	private static int rows;
	private static long sleepTime;


//	public static void init(String path) throws Exception {
//		setting = new Setting(FileUtil.touch(path), CharsetUtil.CHARSET_UTF_8, false);
//		String env = setting.get("env");
//		mode = setting.get("mode");
//		type = setting.get("type");
//		nodeList = setting.getByGroup("node.list", env).split(",");
//		user = setting.getByGroup("user", env);
//		password = setting.getByGroup("password", env);
//		rows = Integer.parseInt(setting.getByGroup("rows", env));
//		sleepTime = Long.parseLong(setting.getByGroup("sleepTime", env));
//
//		System.out.println("\tenv = " + env);
//		System.out.println("\tmode = " + mode);
//		System.out.println("\ttype = " + type);
//		System.out.println("\tnodeList = " + Arrays.toString(nodeList));
//		System.out.println("\tuser = " + user);
//		System.out.println("\tpassword = " + password);
//		System.out.println("\trows = " + rows);
//		System.out.println("\tsleepTime = " + sleepTime);
//	}

//	public static void start(String path) throws Exception {
//		init(path);
//		String ip = nodeList[0].split(":")[0];
//		int port = Integer.parseInt(nodeList[0].split(":")[1]);
//		MultithreadedTableWriter mtw = new MultithreadedTableWriter(ip,port,user,password, streamTableName, "",
//				false, true, nodeList, 10, 0.001f, 1, "Symbol");
//		for (int i = 0; i < rows; i++) {
//			Thread.sleep(sleepTime * 1000);
//			mtw.insert(System.currentTimeMillis(), "00000A",i,i);
//		}
//		mtw.waitForThreadCompletion();
//		System.out.println("HAStream Case Finish");
//	}
}
