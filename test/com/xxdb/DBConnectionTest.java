package com.xxdb;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

public class DBConnectionTest {
	private DBConnection conn;
	public DBConnectionTest(){

	}
	public DBConnectionTest(String host, int port) throws IOException{
		conn = new DBConnection();
		if(!conn.connect(host,port,"admin","123456")){
			throw new IOException("Failed to connect to 2xdb server");
		}
	}
	public int caseCount = 0;
	public int failedCount = 0;

	public void testVoid() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
	}
	
	public void testStringVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicStringVector vector = (BasicStringVector)conn.run("`IBM`GOOG`YHOO");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<size; ++i)
//			System.out.println(vector.getString(i));
		if(size != 3)
			failedCount++;
	}
	
	public void testFunctionDef() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		Entity obj = conn.run("def(a,b){return a+b}");
//		System.out.println(obj.getString());
	}
	
	public void testSymbolVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU,10)");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<size; ++i)
//			System.out.println(vector.getString(i));
		if(size != 10)
			failedCount++;
	}
	
	public void testIntegerVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		Date start = new Date();
		BasicIntVector vector = (BasicIntVector)conn.run("rand(10000,1000000)");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<20; ++i)
//			System.out.println(vector.getInt(i));
//		System.out.println("Time used:" + ((new Date()).getTime() - start.getTime()));
		if(size != 1000000)
			failedCount++;
	}
	
	public void testDoubleVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0,10)");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<size; ++i)
//			System.out.println(vector.getDouble(i));
		if(size != 10)
			failedCount++;
	}
	
	public void testDateVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicDateVector vector = (BasicDateVector)conn.run("2012.10.01 +1..10");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<size; ++i)
//			System.out.println(vector.getDate(i).toString());
		if(size != 10)
			failedCount++;
	}
	
	public void testDateTimeVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicDateTimeVector vector = (BasicDateTimeVector)conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
		int size = vector.rows();
//		System.out.println("size: "+size);
//		for(int i=0; i<size; ++i)
//			System.out.println(vector.getDateTime(i).toString());
		if(size != 10)
			failedCount++;
	}
	
	public void testIntMatrix() throws IOException {
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$2:3");
//		System.out.println(matrix.getString());
		if(matrix.rows()!=2)
			failedCount++;
	}
	
	public void testIntMatrixWithLabel() throws IOException {
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicIntMatrix matrix = (BasicIntMatrix)conn.run("cross(add,1..5,1..10)");
		if(matrix.rows()!=5)
			failedCount++;
		//System.out.println(matrix.getString());
	}
	
	public void testTable() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		StringBuilder sb =new StringBuilder();
		sb.append("n=20000\n");
		sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
		sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
		sb.append("select qty,price from mytrades where sym==`IBM;");
		BasicTable table = (BasicTable)conn.run(sb.toString());
//		System.out.println(table.getString());
		if(table.rows()>0)
			failedCount++;
	}
	
	public void testDictionary() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
//		System.out.println(dict.get(new BasicInt(1)).getString());
		if(dict.rows()==3)
			failedCount++;
	}
	
	public void testFunction() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		caseCount++;
		List<Entity> args = new ArrayList<Entity>(1);
		double[] array = {1.5, 2.5, 7};
		BasicDoubleVector vec = new BasicDoubleVector(array);
		args.add(vec);
		Scalar result = (Scalar)conn.run("sum", args);
		if(((BasicDouble)result).getDouble()==4.7)
			failedCount++;
//		System.out.println(result.getString());

	}

	public void testFunction1() throws IOException{
		caseCount++;
		Map<String, Entity> vars = new HashMap<String, Entity>();
		BasicDoubleVector vec = new BasicDoubleVector(3);
		vec.setDouble(0, 1.5);
		vec.setDouble(1, 2.5);
		vec.setDouble(2, 7);
		vars.put("a",vec);
		conn.upload(vars);
		Entity result = conn.run("accumulate(+,a)");
		if(((BasicDouble)result).getDouble()!=11)
			failedCount++;
//		System.out.println(result.getString());
	}

	public void testFunction2() throws IOException{
		caseCount++;
		conn.run("login('admin','123456')");
		conn.run("def Foo5(a,b){ f=file('testsharp.csv','w');f.writeLine(string(a)); }");
		List<Entity> args = new ArrayList<Entity>();
		args.add(new BasicDouble(1.3));
		args.add(new BasicDouble(1.4));
		Entity result = conn.run("Foo5",args);
		System.out.println("over Foo5");
	}

	public void testAnyVector() throws IOException, Exception{
		BasicAnyVector result = (BasicAnyVector)conn.run("{1, 2, {1,3, 5},{0.9, 0.8}}");
		caseCount++;
		//System.out.println(result.getString());
		if(result.get(0).getNumber().intValue()!=1)
			failedCount++;
		caseCount++;
		result = (BasicAnyVector)conn.run("eachRight(def(x,y):x+y,1,(1,2,3))");
		if(result.get(0).getNumber().intValue()!=2)
			failedCount++;

//		System.out.println(result.getString());
	}
	
	public void testSet() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicSet result = (BasicSet)conn.run("set(1+3*1..100)");
		System.out.println(result.getString());
	}
	
	public void testChart() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		StringBuilder sb =new StringBuilder();
		sb.append("dates=(2012.01.01..2016.07.31)[def(x):weekday(x) between 1:5]\n");
		sb.append("chartData=each(cumsum,reshape(rand(10000,dates.size()*5)-4500, dates.size():5))\n");
		sb.append("chartData.rename!(dates, \"Strategy#\"+string(1..5))\n");
		sb.append("plot(chartData,,[\"Cumulative Pnls of Five Strategies\",\"date\",\"pnl\"],LINE)");
		BasicChart chart = (BasicChart)conn.run(sb.toString());
		System.out.println(chart.getTitle());
		System.out.println(chart.getData().getRowLabel(0).getString());
	}

	public void testMatrixUpload() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		Entity a = conn.run("cross(+, 1..5, 1..5)");
		Entity b = conn.run("1..25$5:5");
		Map<String, Entity> map = new HashMap<String, Entity>();
		map.put("a", a);
		map.put("b", b);
		conn.upload(map);
		Entity matrix = conn.run("a+b");
		System.out.println(matrix.getString());
	}

    public void testUserDefineFunction() throws IOException{
    	System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        conn.run("def f(a,b) {return a+b};");
        List<Entity> args = new ArrayList<Entity>(2);
        BasicInt arg = new BasicInt(1);
        BasicInt arg2 = new BasicInt(2);
        args.add(arg);
        args.add(arg2);
        
        Scalar result = (Scalar)conn.run("f", args);
        System.out.println(result.getString());
    }
    
    public void testFunctionIntMatrix(final int nrow, final int ncol) throws Exception {
    	System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        List<int[]> data = new ArrayList<int[]>();
        for (int i=0; i<ncol; ++i) {
        	int[] array = IntStream.range(i*nrow,i*nrow+nrow).toArray();
        	data.add(array);
        }
        BasicIntMatrix matrix = new BasicIntMatrix(nrow, ncol, data);
        BasicIntVector lables = new BasicIntVector(IntStream.range(1, nrow+1).toArray());
        matrix.setRowLabels(lables);
        lables = new BasicIntVector(IntStream.range(1, ncol+1).toArray());
        matrix.setColumnLabels(lables);
        System.out.println(matrix.getString());
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicIntVector vector = (BasicIntVector)conn.run("flatten", args);
        System.out.println(vector.getString());
    }
    
    public void testFunctionDoubleMatrix(final int nrow, final int ncol) throws Exception {
    	System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        List<double[]> data = new ArrayList<double[]>();
        for (int i=0; i<ncol; ++i) {
        	double[] array = DoubleStream.iterate(i*nrow, n->n+1).limit(nrow).toArray();
        	data.add(array);
        }
        BasicDoubleMatrix matrix = new BasicDoubleMatrix(nrow, ncol, data);
        System.out.println(matrix.getString());
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicDoubleVector vector = (BasicDoubleVector) conn.run("flatten", args);
        System.out.println(vector.getString());
    }
    
    public void testFunctionStrMatrix() throws Exception {
    	System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        List<String[]> data = new ArrayList<String[]>();
        String[] array = new String[]{"test1", "test2", "test3"};
       	data.add(array);
        array = new String[]{"test4", "test5", "test6"};
     	data.add(array);
       
        BasicStringMatrix matrix = new BasicStringMatrix(3, 2, data);
        System.out.println(matrix.getString());
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicStringVector vector = (BasicStringVector)conn.run("flatten", args);
        System.out.println(vector.getString());
    }
    public void Test_upload_table() throws IOException{
    	BasicTable tb = (BasicTable)conn.run("table(1..100 as id,take(`aaa,100) as name)");
    	Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_uploaded", (Entity)tb);
        conn.upload(upObj);
        Entity table = conn.run("table_uploaded");
		System.out.println(table.getString());
    
    }
    
	public void testTableUpload() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		List<String> colNames = new ArrayList<String>();
		colNames.add("id");
		colNames.add("value");
		colNames.add("x");
		
		List<Vector> cols = new ArrayList<Vector>();
		
		int[] intArray = new int[]{1,2,3,4,3};
		BasicIntVector vec = new BasicIntVector(intArray);
		cols.add(vec);
		
		double[] doubleArray = new double[]{7.8, 4.6, 5.1, 9.6, 0.1};
		BasicDoubleVector vecDouble = new BasicDoubleVector(doubleArray);
		cols.add(vecDouble);
		
		intArray = new int[]{5,4,3,2,1};
		vec = new BasicIntVector(intArray);
		cols.add(vec);
		
		BasicTable t1 = new BasicTable(colNames, cols);
		
		colNames = new ArrayList<String>();
		colNames.add("id");
		colNames.add("qty");
		colNames.add("x");
		
		cols = new ArrayList<Vector>();
		intArray = new int[]{3,1};
		vec = new BasicIntVector(intArray);
		cols.add(vec);
		
		short[] shortArray = new short[]{500, 800};
		BasicShortVector vecShort = new BasicShortVector(shortArray);
		cols.add(vecShort);
		
		doubleArray = new double[]{66.0, 88.0};
		vecDouble = new BasicDoubleVector(doubleArray);
		cols.add(vecDouble);
		
		BasicTable t2 = new BasicTable(colNames, cols);
		
		Map<String, Entity> map = new HashMap<String, Entity>();
		map.put("t1", t1);
		map.put("t2", t2);
		conn.upload(map);
		conn.upload(map);
		Entity table = conn.run("lj(t1, t2, `id)");
		System.out.println(table.getString());
	}
	
	/**
	 * This method bulk loads a table to DolphinDB partitioned historical database through Java API. 
	 * The whole process consists of three steps:
	 * (1) Prepare a DolphinDB table in Java.
	 * (2) Upload the table to DolphinDB Server
	 * (3) Call savePartition function on server to a historical database
	 */
	public void testBulkLoad() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		
		//prepare a table with five columns
		List<String> colNames = new ArrayList<String>();
		colNames.add("sym");
		colNames.add("date");
		colNames.add("time");
		colNames.add("price");
		colNames.add("qty");
		
		List<Vector> cols = new ArrayList<Vector>();
		int n = 5000000;
		
		String[] syms = new String[]{"AAPL","AMZN","AB"};
		String[] vsym = new String[n];
		int[] indices = generateRandomIntegers(syms.length, n);
		for(int i=0; i<n; ++i)
			vsym[i] = syms[indices[i]];
		cols.add(new BasicStringVector(vsym));
		
		BasicDate date = new BasicDate(LocalDate.of(2017,8, 7));
		int[] dates = new int[]{0,1,2};
		BasicDateVector vdate = new BasicDateVector(n);
		indices = generateRandomIntegers(dates.length, n);
		for(int i=0; i<n; ++i)
			vdate.setInt(i, date.getInt() + dates[indices[i]]);
		cols.add(vdate);
		
		BasicTime time = new BasicTime(LocalTime.of(9,30, 0));
		BasicTimeVector vtime = new BasicTimeVector(n);
		indices = generateRandomIntegers(21600000, n);
		for(int i=0; i<n; ++i)
			vtime.setInt(i, time.getInt() + indices[i]);
		cols.add(vtime);

		
		double[] prices = new double[]{7.8, 7.6, 7.1, 7.5, 7.4};
		double[] vprice = new double[n];
		indices = generateRandomIntegers(prices.length, n);
		for(int i=0; i<n; ++i)
			vprice[i] = prices[indices[i]];
		cols.add(new BasicDoubleVector(vprice));
		
		int[] qtys = new int[]{5,4,3,2,1};
		int[] vqty = new int[n];
		indices = generateRandomIntegers(qtys.length, n);
		for(int i=0; i<n; ++i)
			vqty[i] = qtys[indices[i]];
		cols.add(new BasicIntVector(vqty));;
		
		BasicTable t1 = new BasicTable(colNames, cols);
	
		//upload the table to DolphinDB server
		Map<String, Entity> vars = new HashMap<String, Entity>();
		vars.put("t1", t1);
		LocalDateTime start = LocalDateTime.now();
		conn.upload(vars);
		Duration elapsed = Duration.between(start, LocalDateTime.now());
		System.out.println("Table upload time: " + elapsed.getSeconds() + " s " + (elapsed.getNano()/1000000));
		
		/**
		 * run the script on server to save data to historical database
		 * (1) create or open the database which has value-based partition on date.
		 * (2) convert the column sym from STRING type to SYMBOL type. The SYMBOL type has much better query performance than STRING type.
		 * (3) call savePartition function to dump data to historical database on disk.
		*/
		String script = "tickdb = database('c:/DolphinDB/db_testing/TickDB_A', VALUE, 2000.01.01..2024.12.31) \n" + 
				"t1[`sym] = symbol(t1.sym) \n" +
				"tb = tickdb.createPartitionedTable(t1,`Trades,`date) \n" +
				"tb.append!(t1)";
		
		start = LocalDateTime.now();
		conn.run(script);
		elapsed = Duration.between(start, LocalDateTime.now());
		System.out.println("Table save time: " + elapsed.getSeconds() + " s " + (elapsed.getNano()/1000000));
	}
	
	private int[] generateRandomIntegers(int uplimit, int count){
		Random randomGenerator = new Random();
		int[] indices = new int[count];
		for(int i=0; i<count; ++i)
			indices[i] = randomGenerator.nextInt(uplimit);
		return indices;
	}

	private	BasicTable createBasicTable(){
		List<String> colNames = new ArrayList<String>();
		colNames.add("cbool");
		colNames.add("cchar");
		colNames.add("cshort");
		colNames.add("cint");
		colNames.add("clong");
		colNames.add("cdate");
		colNames.add("cmonth");
		colNames.add("ctime");
		colNames.add("cminute");
		colNames.add("csecond");
		colNames.add("cdatetime");
		colNames.add("ctimestamp");
		colNames.add("cnanotime");
		colNames.add("cnanotimestamp");
		colNames.add("cfloat");
		colNames.add("cdouble");
		colNames.add("csymbol");
		colNames.add("cstring");
		List<Vector> cols = new ArrayList<Vector>(){};


		//boolean
		byte[] vbool = new byte[]{1,0};
		BasicBooleanVector bbv = new BasicBooleanVector(vbool);
		cols.add(bbv);
		//char
		byte[] vchar = new byte[]{(byte)'c',(byte)'a'};
		BasicByteVector bcv = new BasicByteVector(vchar);
		cols.add(bcv);
		//cshort
		short[] vshort = new short[]{32767,29};
		BasicShortVector bshv = new BasicShortVector(vshort);
		cols.add(bshv);
		//cint
		int[] vint = new int[]{2147483647,483647};
		BasicIntVector bintv = new BasicIntVector(vint);
		cols.add(bintv);
		//clong
		long[] vlong = new long[]{2147483647,483647};
		BasicLongVector blongv = new BasicLongVector(vlong);
		cols.add(blongv);
		//cdate
		int[] vdate = new int[]{Utils.countDays(LocalDate.of(2018,2,14)),Utils.countDays(LocalDate.of(2018,8,15))};
		BasicDateVector bdatev = new BasicDateVector(vdate);
		cols.add(bdatev);
		//cmonth
		int[] vmonth = new int[]{Utils.countMonths(YearMonth.of(2018,2)),Utils.countMonths(YearMonth.of(2018,8))};
		BasicMonthVector bmonthv = new BasicMonthVector(vmonth);
		cols.add(bmonthv);
		//ctime
		int[] vtime = new int[]{Utils.countMilliseconds(16,46,05,123),Utils.countMilliseconds(18,32,05,321)};
		BasicTimeVector btimev = new BasicTimeVector(vtime);
		cols.add(btimev);
		//cminute
		int[] vminute = new int[]{Utils.countMinutes(LocalTime.of(16,30)),Utils.countMinutes(LocalTime.of(9,30))};
		BasicMinuteVector bminutev = new BasicMinuteVector(vminute);
		cols.add(bminutev);
		//csecond
		int[] vsecond = new int[]{Utils.countSeconds(LocalTime.of(9,30,30)),Utils.countSeconds(LocalTime.of(16,30,50))};
		BasicSecondVector bsecondv = new BasicSecondVector(vsecond);
		cols.add(bsecondv);
		//cdatetime
		int[] vdatetime = new int[]{Utils.countSeconds(LocalDateTime.of(2018,9,8,9,30,01)),Utils.countSeconds(LocalDateTime.of(2018,11,8,16,30,01))};
		BasicDateTimeVector bdatetimev = new BasicDateTimeVector(vdatetime);
		cols.add(bdatetimev);
		//ctimestamp
		long[] vtimestamp = new long[]{Utils.countMilliseconds(2018,11,12,9,30,01,123),Utils.countMilliseconds(2018,11,12,16,30,01,123)};
		BasicTimestampVector btimestampv = new BasicTimestampVector(vtimestamp);
		cols.add(btimestampv);
		//cnanotime
		long[] vnanotime = new long[]{Utils.countNanoseconds(LocalTime.of(9,30,05,123456789)),Utils.countNanoseconds(LocalTime.of(16,30,05,987654321))};
		BasicNanoTimeVector bnanotimev = new BasicNanoTimeVector(vnanotime);
		cols.add(bnanotimev);
		//cnanotimestamp
		long[] vnanotimestamp = new long[]{Utils.countNanoseconds(LocalDateTime.of(2018,11,12,9,30,05,123456789)),Utils.countNanoseconds(LocalDateTime.of(2018,11,13,16,30,05,987654321))};
		BasicNanoTimestampVector bnanotimestampv = new BasicNanoTimestampVector(vnanotimestamp);
		cols.add(bnanotimestampv);
		//cfloat
		float[] vfloat = new float[]{2147.483647f,483.647f};
		BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
		cols.add(bfloatv);
		//cdouble
		double[] vdouble = new double[]{214.7483647,48.3647};
		BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
		cols.add(bdoublev);
		//csymbol
		String[] vsymbol = new String[]{"GOOG","MS"};
		BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
		cols.add(bsymbolv);
		//cstring
		String[] vstring = new String[]{"","test string"};
		BasicStringVector bstringv = new BasicStringVector(vstring);
		cols.add(bstringv);
		BasicTable t1 = new BasicTable(colNames, cols);
		return t1;
	}
	public void test_save_memoryTable() throws IOException{
		BasicTable table1 = createBasicTable();
		conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
		conn.run("share t as memoryTable");
		conn.run("def saveData(data){ memoryTable.append!(data)}");
		List<Entity> args = new ArrayList<Entity>(1);
		args.add(table1);
		conn.run("saveData", args);
	}

	public void test_save_dfsTable() throws IOException{
		BasicTable table1 = createBasicTable();
		conn.login("admin","123456",false);
		conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
		conn.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
		conn.run("db = database('dfs://testDatabase',RANGE,2018.01.01..2018.12.31)");
		conn.run("db.createPartitionedTable(t,'tb1','cdate')");
		conn.run("def saveData(data){ loadTable('dfs://testDatabase','tb1').append!(data)}");
		List<Entity> args = new ArrayList<Entity>(1);
		args.add(table1);
		conn.run("saveData", args);
	}

	public void test_save_localTable(List<Byte> boolArray,List<Integer> intArray,double[] dblArray, List<Integer> dateArray,List<String> strArray) throws IOException{
		List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
		List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
		BasicTable table1 = new BasicTable(colNames,cols);
		String dbpath = "/home/user1/testDatabase";
		conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
		conn.run(String.format("if(existsDatabase('{0}')){dropDatabase('{0}')}",dbpath));
		conn.run(String.format("db = database('{0}',VALUE,'MS' 'GOOG' 'FB')",dbpath));
		conn.run("db.createPartitionedTable(t,'tb1','csymbol')");
		conn.run(String.format("def saveData(data){ loadTable('{0}','tb1').append!(data)}",dbpath));
		List<Entity> args = new ArrayList<Entity>(1);
		args.add(table1);
		conn.run("saveData", args);
	}

	public void test_loop_basicTable(BasicTable table1) throws Exception{
		BasicStringVector stringv = (BasicStringVector) table1.getColumn("cstring");
		BasicIntVector intv = (BasicIntVector) table1.getColumn("cint");
		BasicTimestampVector timestampv = (BasicTimestampVector) table1.getColumn("ctimestamp");
		BasicDoubleVector doublev = (BasicDoubleVector) table1.getColumn("cdouble");
		for(int ri=0;ri<table1.rows();ri++){
			System.out.println(stringv.getString(ri));
			System.out.println(intv.getInt(ri));
			LocalDateTime timestamp = timestampv.getTimestamp(ri);
			System.out.println(timestamp);
			System.out.println(doublev.getDouble(ri));
		}
	}


	public void test_temperal(){
		//BasicDate bd = new BasicDate(Utils.countDays(LocalDate.of(2018,11,12)));
		BasicDate bd = new BasicDate(LocalDate.of(2018,11,12));
		BasicMonth bm = new BasicMonth(YearMonth.of(2018,11));
		BasicTime bt = new BasicTime(LocalTime.of(20,8,1,123000000));
		BasicMinute bmn = new BasicMinute(LocalTime.of(20,8));
		BasicSecond bs = new BasicSecond(LocalTime.of(20,8,1));
		BasicDateTime bdt = new BasicDateTime(LocalDateTime.of(2018,11,12,8,1,1));
		BasicTimestamp bts = new BasicTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123000000));
		BasicNanoTime bnt = new BasicNanoTime(LocalTime.of(20,8,1,123456789));
		BasicNanoTimestamp bnts = new BasicNanoTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123456789));

		BasicTimestamp btt = new BasicTimestamp(Utils.parseTimestamp(1543494854000l));
		LocalDateTime dt = btt.getTimestamp();
	}

	public void test_partialFunction() throws IOException{
		conn.run("share table(1..50 as id) as sharedTable");
		int[] intArray = new int[]{30,40,50};
		List<Entity> args = Arrays.asList(new BasicIntVector(intArray));
		conn.run("tableInsert{sharedTable}",args);
	}

	public void testFunction3() throws IOException{
		DBConnection conn = new DBConnection();
		conn.connect("192.168.1.135", 8981, "admin", "123456");

		List<Entity> args = new ArrayList<Entity>();
		args.add(new BasicDouble(0.15));
		args.add(new BasicDouble(0.28));
		args.add(new BasicDouble(2));
		args.add(new BasicDouble(1.8));
		args.add(new BasicDouble(-0.025));
		args.add(new BasicDouble(0.055));
		args.add(new BasicDouble(0));
		args.add(new BasicDouble(0));
		args.add(new BasicDouble(100));
		args.add(new BasicDouble(1.1));
		args.add(new BasicDouble(2));
		args.add(new BasicDouble(2.243));
		args.add(new BasicDouble(2.32895));
		args.add(new BasicDouble(2.5));

		BasicDouble rt1 = (BasicDouble)conn.run("WingModel", args);
		System.out.println(rt1.getDouble());
	}
	
	public void testUUID() throws IOException {
		String uuidStr = "92274dfe-d589-4598-84a3-c381592fdf3f";
		BasicUuid a = BasicUuid.fromString(uuidStr);
		List<Entity> args = new ArrayList<Entity>(1);
		args.add(a);
		System.out.println(conn.run("string", args));
	}

	public void Test_ReLogin() throws IOException {
		conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10);	t = table(1..100 as id);db.createPartitionedTable(t,'t1', 'id')");
		conn.run("logout()");
		conn.run("exec count(*) from loadTable('dfs://db1','t1')");
		BasicInt re = (BasicInt)conn.run("exec count(*) from loadTable('dfs://db1','t1')");
		System.out.println(re.getInt());
	}

	public void test_set_nullvalue() throws IOException {
		try{
			List<String> colNames =  Arrays.asList("cint","cdouble","clong","cfloat","cshort");
			List<Vector> colData = Arrays.asList(new BasicIntVector(1),new BasicDoubleVector(1),new BasicLongVector(1),new BasicFloatVector(1),new BasicShortVector(1));
			BasicTable bt = new BasicTable(colNames,colData);
			bt.getColumn("cint").set(0,new BasicInt(Integer.MIN_VALUE));
			bt.getColumn("cdouble").set(0,new BasicDouble(-Double.MIN_VALUE));
			bt.getColumn("clong").set(0,new BasicLong(Long.MIN_VALUE));
			bt.getColumn("cfloat").set(0,new BasicFloat(-Float.MAX_VALUE));
			bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
			bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
			bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
			System.out.println(bt.toString());
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public BasicTable buildTable(List<String> colNames , List<String> colTypes, int msgSize) throws Exception{
		List<Vector> colData = new ArrayList<Vector>();
		for (String colType:colTypes) {
			switch (colType){
				case "SYMBOL":
					colData.add(new BasicStringVector(msgSize));
					break;
				case "DATE":
					colData.add(new BasicDateVector(msgSize));
					break;
				case "FLOAT":
					colData.add(new BasicFloatVector(msgSize));
					break;
				case "INT":
					colData.add(new BasicIntVector(msgSize));
					break;
				case "LONG":
					colData.add(new BasicLongVector(msgSize));
					break;
				case "STRING":
					colData.add(new BasicStringVector(msgSize));
					break;
				case "TIME":
					colData.add(new BasicTimeVector(msgSize));
					break;
				case "TIMESTAMP":
					colData.add(new BasicTimestampVector(msgSize));
					break;
			}
		}
		return new BasicTable(colNames,colData);
	}


	private void bulk_load() throws Exception{
		//示例，实际数据从csv文件中构建
		List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
		List<String> colTypes = Arrays.asList("BOOL","INT","DOUBLE","DATE","SYMBOL");
		//msgSize
		int msgSize = 100;
		BasicTable table1 = buildTable(colNames,colTypes, msgSize);
		for(int i=0;i<msgSize;i++){ //行数
			for(int j=0;j<colNames.size();j++){ //列数，从key中取
				String key = "colName1";
				String val = "1234";//假设value是string
				setTableValue(table1,i,key, val);
			}
		}
		List<Entity> args = new ArrayList<Entity>(1);
		args.add(table1);
		conn.run("tableInsert{market_data}", args);
	}

	private void setTableValue(BasicTable bt, int rowIndex, String key, String value) throws Exception {
		Scalar v = null;
		switch (bt.getColumn(key).getDataType()){
			case DT_STRING:
				v = new BasicString(value);
				break;
			case DT_DOUBLE:
				v = new BasicDouble(Double.parseDouble(value));
				break;
			case DT_FLOAT:
				v = new BasicFloat(Float.parseFloat(value));
				break;
			case DT_LONG:
				v = new BasicLong(Long.parseLong(value));
				break;
			case DT_INT:
				v = new BasicInt(Integer.parseInt(value));
				break;
			case DT_DATE:
				if(value.equals("")){
					v = new BasicDate(LocalDate.now());
					v.setNull();
				}else {
					v = new BasicDate(LocalDate.parse(value));
				}
				// vlaue 是毫秒值用如下方式
				//v = new BasicDate(Utils.parseDate(Integer.parseInt(value)));
				break;
			case DT_TIMESTAMP:
				if(value.equals("")) {
					v = new BasicTimestamp(LocalDateTime.now());
					v.setNull();
				}else {
					v = new BasicTimestamp(LocalDateTime.parse(value));
				}
				// vlaue 是毫秒值用如下方式
				//v = new BasicTimestamp(Utils.parseTimestamp(Long.parseLong(value)));
				break;
			case DT_TIME:
				if(value.equals("")) {
					v = new BasicTime(LocalTime.now());
					v.setNull();
				}else {
					v = new BasicTime(LocalTime.parse(value));
				}
				// vlaue 是毫秒值用如下方式
				//v = new BasicTime(Utils.parseTime(Integer.parseInt(value)));
				break;
		}
		bt.getColumn(key).set(rowIndex,v);
	}




	public static void main(String[] args){

		try{
			String host = "192.168.1.142";
			int port = 8848;
			if(args.length>=2){
				host = args[0];
				port = Integer.parseInt(args[1]);
			}

			DBConnectionTest test = new DBConnectionTest(host,port);
//			test.testUUID();
//			test.testVoid();
//			test.testFunctionDef();
//			test.testIntegerVector();
//			test.testStringVector();
//			test.testSymbolVector();
//			test.testDoubleVector();
//			test.testDateVector();
//			test.testDateTimeVector();
//			test.testIntMatrix();
//			test.testIntMatrixWithLabel();
//			test.testDictionary();
//			test.testTable();
//			test.testFunction();
			//test.testAnyVector();
			test.Test_ReLogin();
			/*test.testSet();
			test.testChart();
			test.testMatrixUpload();
			test.testUserDefineFunction();
			test.testFunctionIntMatrix(4, 2);
			test.testFunctionDoubleMatrix(4, 2);
			test.testFunctionStrMatrix();
			test.testTableUpload();
			//test.testBulkLoad();
			test.Test_upload_table();
			test.testLoginWithLogin();
			test.test_save_memoryTable();
			test.test_save_dfsTable();
			List<Integer> iv = Arrays.asList(1,2,3);
			List<Double> dbv = Arrays.asList(1.1,2.2,3.3);
			long n = Utils.countMilliseconds(LocalDateTime.now());
			List<Long> dtv = Arrays.asList(n,n+1,n+2);
			List<String> sv = Arrays.asList("aaa","bbb","ccc");
			String dbPath = "C:/data/testDatabase";
			String tbName = "tb1";
			test.test_save_TableInsert(dbPath,tbName,sv,iv,dtv,dbv);*/
			//test.test_save_localTable();
			//test.test_loop_basicTable();
			//test.testFunction1();
			//test.test_partialFunction();
			//test.testFunction2();
			//test.testFunction3();
			System.out.println("test cases :" + test.failedCount + "/" + test.caseCount);
		}

		catch(Exception ex){
			ex.printStackTrace();
			System.out.println(ex.getMessage());
		}
	}
}
