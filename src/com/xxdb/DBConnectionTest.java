package com.xxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicChart;
import com.xxdb.data.BasicDateTimeVector;
import com.xxdb.data.BasicDateVector;
import com.xxdb.data.BasicDictionary;
import com.xxdb.data.BasicDoubleVector;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntMatrix;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicSet;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Scalar;
import com.xxdb.data.Vector;

public class DBConnectionTest {
	private DBConnection conn;
	
	public DBConnectionTest() throws IOException{
		conn = new DBConnection();
		if(!conn.connect("localhost",80)){
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
		BasicDoubleVector vec = new BasicDoubleVector(3);
		vec.setDouble(0, 1.5);
		vec.setDouble(1, 2.5);
		vec.setDouble(2, 7);
		args.add(vec);
		Scalar result = (Scalar)conn.run("sum", args);
		System.out.println(result.getString());
	}
	
	
	public void testAnyVector() throws IOException{
		BasicAnyVector result = (BasicAnyVector)conn.run("{1, 2, {1,3, 5},{0.9, 0.8}}");
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
		List<String> variables = new ArrayList<String>();
		List<Entity> entities = new ArrayList<Entity>();
		variables.add("a");
		variables.add("b");
		entities.add(a);
		entities.add(b);
		conn.upload(variables, entities);
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
    
    public void testFunctionIntMatrix(final int nrow, final int ncol) throws IOException {
    	System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        BasicIntMatrix matrix = new BasicIntMatrix(nrow,ncol);
        int value = 0;
        for (int i=0; i<nrow; ++i) {
            for (int j=0; j<ncol; ++j) {
                matrix.setInt(i, j, value++);
            }
        }
        System.out.println(matrix.getString());
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicIntVector vector = (BasicIntVector)conn.run("flatten", args);
        System.out.println(vector.getString());
    }
    
	public void testTableUpload() throws IOException{
		System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
		List<String> colNames = new ArrayList<String>();
		colNames.add("id");
		colNames.add("value");
		colNames.add("x");
		
		List<Vector> cols = new ArrayList<Vector>();
		
		BasicIntVector vec = new BasicIntVector(5);
		vec.setInt(0, 1);
		vec.setInt(1, 2);
		vec.setInt(2, 3);
		vec.setInt(3, 4);
		vec.setInt(4, 3);
		cols.add(vec);
		
		BasicDoubleVector vecDouble = new BasicDoubleVector(5);
		vecDouble.setDouble(0, 7.8);
		vecDouble.setDouble(1, 4.6);
		vecDouble.setDouble(2, 5.1);
		vecDouble.setDouble(3, 9.6);
		vecDouble.setDouble(4, 0.1);
		cols.add(vecDouble);
		
		vec = new BasicIntVector(5);
		vec.setInt(0, 5);
		vec.setInt(1, 4);
		vec.setInt(2, 3);
		vec.setInt(3, 2);
		vec.setInt(4, 1);
		cols.add(vec);
		
		BasicTable t1 = new BasicTable(colNames, cols);
		
		colNames = new ArrayList<String>();
		colNames.add("id");
		colNames.add("qty");
		colNames.add("x");
		
		cols = new ArrayList<Vector>();
		vec = new BasicIntVector(2);
		vec.setInt(0, 3);
		vec.setInt(1, 1);
		cols.add(vec);
		
		vec = new BasicIntVector(2);
		vec.setInt(0, 500);
		vec.setInt(1, 800);
		cols.add(vec);
		
		vecDouble = new BasicDoubleVector(2);
		vecDouble.setDouble(0, 66.0);
		vecDouble.setDouble(1, 88.0);
		cols.add(vecDouble);
		
		BasicTable t2 = new BasicTable(colNames, cols);
		
		List<String> variables = new ArrayList<String>();
		List<Entity> entities = new ArrayList<Entity>();
		variables.add("t1");
		variables.add("t2");
		entities.add(t1);
		entities.add(t2);
		conn.upload(variables, entities);
		Entity table = conn.run("lj(t1, t2, `id)");
		System.out.println(table.getString());
	}   
	
	public static void main(String[] args){
		try{
			DBConnectionTest test = new DBConnectionTest();
			test.testVoid();
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
			test.testFunctionIntMatrix(3,3);
			test.testTableUpload();

		}
		catch(Exception ex){
			ex.printStackTrace();
			System.out.println(ex.getMessage());
		}
	}
}
