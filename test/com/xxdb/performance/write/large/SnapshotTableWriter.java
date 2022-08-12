package com.xxdb.performance.write.large;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SnapshotTableWriter implements Runnable{

	private DBConnection conn;
	private List<List<Entity>> argsList = new ArrayList<>();
	private int insertCount;
	public static String[] nodeList;
	public static AtomicInteger cdl = new AtomicInteger(0);

	public SnapshotTableWriter(int id, ArrayList<BasicTable> data, String dbName, String tableName, int insertCount) throws IOException {
		for (int i = 0; i < data.size(); i++) {
			List<Entity> args = new ArrayList<Entity>(1);
			args.add(data.get(i));
			argsList.add(args);
		}
		this.conn = new DBConnection();
		String[] node = nodeList[id % nodeList.length].split(":");
		String ip = node[0];
		int port = Integer.parseInt(node[1]);
		this.conn.connect(ip,port,"admin","123456");
		this.conn.run(String.format("def saveData(data){ loadTable('%s','%s').tableInsert(data)}",dbName, tableName));
		this.insertCount = insertCount;
	}

	@Override
	public void run() {
		try {
			for (int i = 0; i < insertCount; i++) {
				this.conn.run("saveData", this.argsList.get(i));
			}
			cdl.addAndGet(1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}