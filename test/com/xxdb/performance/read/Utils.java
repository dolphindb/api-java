package com.xxdb.performance.read;

import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class Utils {
	public static DecimalFormat df = new DecimalFormat("#.00");
	public static long timeDelta = 8 * 60 * 60 * 1000;
	public static MultithreadedTableWriter mtw;
	public static String timeStamp2Date(long time) {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateStr = dateformat.format(time);
		return dateStr;
	}
}
