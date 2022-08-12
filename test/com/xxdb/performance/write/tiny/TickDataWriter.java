package com.xxdb.performance.write.tiny;


import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.util.ArrayList;
import java.util.List;

public class TickDataWriter implements Runnable{

	private int insertCount;
//	public static List<Object[]> data;
	public static ArrayList<ArrayList<Object>> data;
	private MultithreadedTableWriter mtw;
	private int id;
	private String key;

	public TickDataWriter(String key, int id, int insertCount, ArrayList data, MultithreadedTableWriter mtw) {
		this.key = key;
		this.id = id;
		this.insertCount = insertCount;
		this.data = data;
		this.mtw = mtw;
	}

//	@Override
//	public void run() {
//		int st = id * (data.size() / insertCount);
//		List<Object[]> subData = data.subList(st, st + (data.size() / insertCount));
//
//		for (int i = 0; i < subData.size(); i++) {
//			mtw.insert(subData.get(i));
//		}
//		System.out.println();
//	}

	@Override
	public void run() {
		int st = id * (data.size() / insertCount);

		List<ArrayList<Object>> subData = data.subList(st, st + (data.size() / insertCount));

		for (int i = 0; i < subData.size(); i++) {
			mtw.insert(subData.get(i).toArray());
		}
	}
}