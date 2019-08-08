import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

public class Simulator {
	private String path;
	private DBConnection conn;
	
	private List<String> vsymbol;
	private List<Integer> vdate;
	private List<Integer> vtime;
	private List<Double> vbid;
	private List<Double> vofr;
	private List<Integer> vbidsiz;
	private List<Integer> vofrsiz;
	private List<Integer> vmode;
	private List<String> vex;
	private List<String> vmmid;

	private static final int BATCH_SIZE = 1000;
	private static final String TABLE_1_NAME = "trades1";
	
	public Simulator(String path, DBConnection conn) {
		this.path = path;
		this.conn = conn;
		resetVectors();
	}
	
	public void simulate() {
		long lastTime = -1;
		SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
		long beginAt, endAt, t0, t1, startTime, endTime;
		try {
			beginAt = timeFormatter.parse("09:30:00").getTime();
			endAt = timeFormatter.parse("21:00:00").getTime();
		} catch (ParseException e1) {
			e1.printStackTrace();
			return;
		}

		Iterable<CSVRecord> records;
		try {
			Reader in = new FileReader(path);
			records = CSVFormat.EXCEL
					.withHeader("SYMBOL", "DATE", "TIME", "BID", "OFR", "BIDSIZ", "OFRSIZ", "MODE", "EX", "MMID").parse(in);
		} catch (FileNotFoundException e) {
			System.out.println("Cannot find file: " + path);
			return;
		} catch (IOException e) {
			System.out.println("IOException");
			return;
		}
		
		System.out.println("Start simulation");
		int count = 0;
		int batchCount = 0;
		t0 = startTime = System.currentTimeMillis();

		for (CSVRecord record : records) {
			if (count == 0) {    // header line
				count++;
				continue;
			}
			String time = record.get("TIME");
			long currentTime;    // time of current record
			try {
				currentTime = timeFormatter.parse(time).getTime();
			} catch (ParseException e) {
				System.out.println("Cannot parse time: " + time);
				return;
			}
			if (beginAt != 0 && currentTime < beginAt)
				continue;

			if (count == 1)    // first line
				lastTime = currentTime;
			
			if (endAt != 0 && currentTime >= endAt) {
				runInsert();
				break;
			}
			
			if (batchCount >= BATCH_SIZE) {
				runInsert();
				// uploadAndInsert();    // Another method: upload and call tableInsert
				resetVectors();
				batchCount = 0;
			}

			batchCount++;
			String symbol = record.get("SYMBOL");
			String date = record.get("DATE");
			String bid = record.get("BID");
			String ofr = record.get("OFR");
			String bidsiz = record.get("BIDSIZ");
			String ofrsiz = record.get("OFRSIZ");
			String mode = record.get("MODE");
			String ex = record.get("EX");
			String mmid = record.get("MMID");
			
			vsymbol.add(symbol);
			vdate.add(Utils.countDays(LocalDate.parse(date, dateFormatter)));
			vtime.add(Utils.countSeconds(LocalTime.parse(time)));
			vbid.add(Double.parseDouble(bid));
			vofr.add(Double.parseDouble(ofr));
			vbidsiz.add(Integer.parseInt(bidsiz));
			vofrsiz.add(Integer.parseInt(ofrsiz));
			vmode.add(Integer.parseInt(mode));
			vex.add(ex);
			vmmid.add(mmid);

			lastTime = currentTime;
			count++;
			
			if (count % 100000 == 0) {
				t1 = System.currentTimeMillis();
				System.out.println(count + " messages has been sent. Average time: " + 100000000 / (t1 - t0));
				t0 = t1;
			}
		}
		System.out.println((count - 1) + " messages has been sent.");
		System.out.println("End simulation");
	}
	
	private void resetVectors() {
		vsymbol = new ArrayList<>();
		vdate = new ArrayList<>();
		vtime = new ArrayList<>();
		vbid = new ArrayList<>();
		vofr = new ArrayList<>();
		vbidsiz = new ArrayList<>();
		vofrsiz = new ArrayList<>();
		vmode = new ArrayList<>();
		vex = new ArrayList<>();
		vmmid = new ArrayList<>();
	}
	
	private void uploadAndInsert() {
		Map<String, Entity> result = new HashMap<>();
		result.put("tsymbol", new BasicStringVector(vsymbol));
		result.put("tdate", new BasicDateVector(vdate));
		result.put("ttime", new BasicSecondVector(vtime));
		result.put("tbid", new BasicDoubleVector(vbid));
		result.put("tofr", new BasicDoubleVector(vofr));
		result.put("tbidsiz", new BasicIntVector(vbidsiz));
		result.put("tofrsiz", new BasicIntVector(vofrsiz));
		result.put("tmode", new BasicIntVector(vmode));
		result.put("tex", new BasicStringVector(vex));
		result.put("tmmid", new BasicStringVector(vmmid));
		try {
			conn.upload(result);
			conn.run("insert into " + TABLE_1_NAME + " values (tsymbol, tdate, ttime, tbid, tofr, tbidsiz, tofrsiz, tmode, tex, tmmid)");
		} catch (Exception e) {
			System.out.println("Insert error");
			System.out.println(result.toString());
			e.printStackTrace();
		}
	}
	
	private void runInsert() {
		ArrayList<Entity> arguments = new ArrayList<>(11);
		arguments.add(new BasicString(TABLE_1_NAME));
		arguments.add(new BasicStringVector(vsymbol));
		arguments.add(new BasicDateVector(vdate));
		arguments.add(new BasicSecondVector(vtime));
		arguments.add(new BasicDoubleVector(vbid));
		arguments.add(new BasicDoubleVector(vofr));
		arguments.add(new BasicIntVector(vbidsiz));
		arguments.add(new BasicIntVector(vmode));
		arguments.add(new BasicStringVector(vex));
		arguments.add(new BasicStringVector(vmmid));
		try {
			conn.run("tableInsert", arguments);
		} catch (Exception e) {
			System.out.println("Insert error");
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		/*
		 	On DolphinDB server:

			n=10000000
			share streamTable(n:0, `symbol`date`time`bid`ofr`bidsiz`ofrsiz`mode`ex`mmid, [SYMBOL,DATE,SECOND,DOUBLE,DOUBLE,INT,INT,INT,SYMBOL,SYMBOL]) as trades1
		 */
		if (args.length < 3) {
			System.out.println("Usage: Simulator hostname port path/to/csv");
			return;
		}
		DBConnection conn = new DBConnection();
		String hostname = args[0];
		int port = Integer.parseInt(args[1]);
		String path = args[2];
		try {
			conn.connect(hostname, port);
			new Simulator(path, conn).simulate();
		} catch (NumberFormatException e) {
			System.out.println("Cannot parse port: " + args[1]);
		} catch (IOException e) {
			System.out.println("Connection error");
			e.printStackTrace();
		}
	}
}
