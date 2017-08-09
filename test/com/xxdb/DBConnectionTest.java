package com.xxdb;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicChart;
import com.xxdb.data.BasicDate;
import com.xxdb.data.BasicDateTimeVector;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicDictionary;
import com.xxdb.data.BasicDoubleMatrix;
import com.xxdb.data.BasicDoubleVector;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntMatrix;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicSet;
import com.xxdb.data.BasicShortVector;
import com.xxdb.data.BasicStringMatrix;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.BasicTime;
import com.xxdb.data.BasicTimeVector;
import com.xxdb.data.Entity;
import com.xxdb.data.Scalar;
import com.xxdb.data.Vector;

public class DBConnectionTest {
	private DBConnection conn;
	
	public DBConnectionTest() throws IOException{
		conn = new DBConnection();
		if(!conn.connect("localhost",8080)){
			throw new IOException("Failed to connect to 2xdb server");
		}
	}
	
	public void testVoid() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		Entity obj = conn.run("NULL");
		System.out.println(obj.getDataType());
	}
	
	public void testStringVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicStringVector vector = (BasicStringVector)conn.run("`IBM`GOOG`YHOO");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<size; ++i)
			System.out.println(vector.getString(i));
	}
	
	public void testFunctionDef() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		Entity obj = conn.run("def(a,b){return a+b}");
		System.out.println(obj.getString());
	}
	
	public void testSymbolVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU,10)");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<size; ++i)
			System.out.println(vector.getString(i));
	}
	
	public void testIntegerVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		Date start = new Date();
		BasicIntVector vector = (BasicIntVector)conn.run("rand(10000,1000000)");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<20; ++i)
			System.out.println(vector.getInt(i));
		System.out.println("Time used:" + ((new Date()).getTime() - start.getTime()));
	}
	
	public void testDoubleVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0,10)");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<size; ++i)
			System.out.println(vector.getDouble(i));
	}
	
	public void testDateVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicDateVector vector = (BasicDateVector)conn.run("2012.10.01 +1..10");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<size; ++i)
			System.out.println(vector.getDate(i).toString());
	}
	
	public void testDateTimeVector() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicDateTimeVector vector = (BasicDateTimeVector)conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
		int size = vector.rows();
		System.out.println("size: "+size);
		for(int i=0; i<size; ++i)
			System.out.println(vector.getDateTime(i).toString());
	}
	
	public void testIntMatrix() throws IOException {
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$2:3");
		System.out.println(matrix.getString());
	}
	
	public void testIntMatrixWithLabel() throws IOException {
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicIntMatrix matrix = (BasicIntMatrix)conn.run("cross(add,1..5,1..10)");
		System.out.println(matrix.getString());
	}
	
	public void testTable() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		StringBuilder sb =new StringBuilder();
		sb.append("n=20000\n");
		sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
		sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
		sb.append("select qty,price from mytrades where sym==`IBM;");
		BasicTable table = (BasicTable)conn.run(sb.toString());
		System.out.println(table.getString());
	}
	
	public void testDictionary() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
		System.out.println(dict.get(new BasicInt(1)).getString());
	}
	
	public void testFunction() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		List<Entity> args = new ArrayList<Entity>(1);
		double[] array = {1.5, 2.5, 7};
		BasicDoubleVector vec = new BasicDoubleVector(array);
		args.add(vec);
		Scalar result = (Scalar)conn.run("sum", args);
		System.out.println(result.getString());
	}
	
	
	public void testAnyVector() throws IOException{
		BasicAnyVector result = (BasicAnyVector)conn.run("{1, 2, {1,3, 5},{0.9, 0.8}}");
		System.out.println(result.getString());
		
		result = (BasicAnyVector)conn.run("eachRight(def(x,y):x+y,1,(1,2,3))");
		System.out.println(result.getString());
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
		sb.append("plot(chartData,[\"Cumulative Pnls of Five Strategies\",\"date\",\"pnl\"],,LINE)");
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
		int n = 2000000;
		
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
		Map<String, Entity> map = new HashMap<String, Entity>();
		map.put("t1", t1);
		LocalDateTime start = LocalDateTime.now();
		conn.upload(map);
		LocalDateTime end = LocalDateTime.now();
		Duration elapsed = Duration.between(start, end);
		System.out.println("Table upload time: " + elapsed.getSeconds() + " s " + (elapsed.getNano()/1000000));
	}
	
	private int[] generateRandomIntegers(int uplimit, int count){
		Random randomGenerator = new Random();
		int[] indices = new int[count];
		for(int i=0; i<count; ++i)
			indices[i] = randomGenerator.nextInt(uplimit);
		return indices;
	}
	
	public static void main(String[] args){
		try{
			DBConnectionTest test = new DBConnectionTest();
			/*test.testVoid();
			test.testFunctionDef();
			test.testIntegerVector();
			test.testStringVector();
			test.testSymbolVector();
			test.testDoubleVector();
			test.testDateVector();
			test.testDateTimeVector();
			test.testIntMatrix();
			test.testIntMatrixWithLabel();
			test.testDictionary();
			test.testTable();
			test.testFunction();
			test.testAnyVector();
			test.testSet();
			test.testChart();
			test.testMatrixUpload();
			test.testUserDefineFunction();
			test.testFunctionIntMatrix(4, 2);
			test.testFunctionDoubleMatrix(4, 2);
			test.testFunctionStrMatrix();
			test.testTableUpload();*/
			test.testBulkLoad();

		}
		catch(Exception ex){
			ex.printStackTrace();
			System.out.println(ex.getMessage());
		}
	}
}
