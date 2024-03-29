package com.xxdb.performance.write.HA;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.ArrayList;
import java.util.List;

public class DataWriter implements Runnable{

	private int insertCount;
	public static ArrayList<ArrayList<Object>> data;
	private MultithreadedTableWriter mtw;
	private int id;
	private String key;

	public DataWriter(String key, int id, int insertCount, ArrayList data, MultithreadedTableWriter mtw) {
		this.key = key;
		this.id = id;
		this.insertCount = insertCount;
		this.data = data;
		this.mtw = mtw;
	}

	@Override
	public void run() {
		int st = id * (data.size() / insertCount);
		List<ArrayList<Object>> subData = data.subList(st, st + (data.size() / insertCount));
		for (int i = 0; i < subData.size(); i++) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ErrorCodeInfo insert = mtw.insert(subData.get(i).toArray());
			if (!insert.getErrorInfo().equals(""))
				System.out.println(insert);
		}
	}
}