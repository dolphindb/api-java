package com.xxdb.performance.write;

import com.xxdb.DBConnection;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

public class ResWriter {

	private static String clientIp;
	private static int clientPort;
	public static MultithreadedTableWriter mtw;

//	public static void init(String path) {
//		Setting setting = new Setting(FileUtil.touch(path), CharsetUtil.CHARSET_UTF_8, false);
//		String env = setting.get("env");
//		clientIp = setting.getByGroup("client.ip", env);
//		clientPort = Integer.parseInt(setting.getByGroup("client.port", env));
//		System.out.println("\tclientIp = " + clientIp);
//		System.out.println("\tclientPort = " + clientPort);
//	}

	public static void init2(String iclientIp,int iclientPort) {
		clientIp = iclientIp;
		clientPort = iclientPort;
		System.out.println("\tclientIp = " + clientIp);
		System.out.println("\tclientPort = " + clientPort);

	}

	public static void start(String path) throws Exception {
//		init(path);
//		DBConnection connection = new DBConnection();
//		connection.connect(clientIp,clientPort,"admin","123456");
//		connection.run("c = exec count(*) from objs(true) where name=`writeResult and form=`TABLE\n" +
//				"if(c == 0){\n" +
//				"\tcolnames = `data`type`threadNum`cost`rows`RPS`begin`end\n" +
//				"\tcoltype = [SYMBOL,SYMBOL,INT,DOUBLE,LONG,DOUBLE,TIMESTAMP,TIMESTAMP]\n" +
//				"\tshare table(1:0,colnames,coltype) as `writeResult\n" +
//				"}");
//		mtw = new MultithreadedTableWriter(clientIp,clientPort,"admin","123456","","writeResult",false,false,null,10,0.001f,1,"threadNum");
//		connection.close();
	}

	public static void start2(String iclientIp,int iclientPort) throws Exception {
		init2(iclientIp,iclientPort);
		DBConnection connection = new DBConnection();
		connection.connect(clientIp,clientPort,"admin","123456");
		connection.run("c = exec count(*) from objs(true) where name=`writeResult and form=`TABLE\n" +
				"if(c == 0){\n" +
				"\tcolnames = `data`type`threadNum`cost`rows`RPS`begin`end\n" +
				"\tcoltype = [SYMBOL,SYMBOL,INT,DOUBLE,LONG,DOUBLE,TIMESTAMP,TIMESTAMP]\n" +
				"\tshare table(1:0,colnames,coltype) as `writeResult\n" +
				"}");
		mtw = new MultithreadedTableWriter(clientIp,clientPort,"admin","123456","","writeResult",false,false,null,10,0.001f,1,"threadNum");
		connection.close();
	}
}
