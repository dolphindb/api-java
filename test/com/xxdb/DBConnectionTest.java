package com.xxdb;

import java.io.*;
import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.LittleEndianDataOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.xxdb.data.*;
import com.xxdb.data.Vector;

public class DBConnectionTest {

    private DBConnection conn;
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8848;


    @Before
    public void setUp() {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }

    @Test
    public void testStringVector() throws IOException {
        BasicStringVector vector = (BasicStringVector) conn.run("`IBM`GOOG`YHOO");
        int size = vector.rows();
        assertEquals(3, size);
    }

    @Test
    public void testFunctionDef() throws IOException {
        Entity obj = conn.run("def(a,b){return a+b}");
        assertEquals(Entity.DATA_TYPE.DT_FUNCTIONDEF, obj.getDataType());
    }

    @Test
    public void testSymbolVector() throws IOException {
        BasicStringVector vector = (BasicStringVector) conn.run("rand(`IBM`MSFT`GOOG`BIDU,10)");
        int size = vector.rows();
        assertEquals(10, size);
    }

    @Test
    public void testIntegerVector() throws IOException {
        BasicIntVector vector = (BasicIntVector) conn.run("rand(10000,1000000)");
        int size = vector.rows();
        assertEquals(1000000, size);
    }

    @Test
    public void testDoubleVector() throws IOException {
        BasicDoubleVector vector = (BasicDoubleVector) conn.run("rand(10.0,10)");
        int size = vector.rows();
        assertEquals(10, size);
    }

    @Test
    public void testDateVector() throws IOException {
        BasicDateVector vector = (BasicDateVector) conn.run("2012.10.01 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
    }

    @Test
    public void testDateTimeVector() throws IOException {

        BasicDateTimeVector vector = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
        int size = vector.rows();
        assertEquals(10, size);
    }

    @Test
    public void testIntMatrix() throws IOException {
        BasicIntMatrix matrix = (BasicIntMatrix) conn.run("1..6$2:3");
        assertEquals(2, matrix.rows());
        assertEquals(3, matrix.columns());
    }

    @Test
    public void testIntMatrixWithLabel() throws IOException {
        BasicIntMatrix matrix = (BasicIntMatrix) conn.run("cross(add,1..5,1..10)");
        assertEquals(5, matrix.rows());
        assertEquals(10, matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }

    @Test
    public void testTable() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("n=20000\n");
        sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
        sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
        sb.append("select qty,price from mytrades where sym==`IBM;");
        BasicTable table = (BasicTable) conn.run(sb.toString());
        Integer q = ((BasicInt) table.getColumn("qty").get(0)).getInt();
        assertTrue(table.rows() > 0);
        assertTrue(q > 10);
    }

    @Test
    public void testBasicTableSerialize() throws IOException{
        StringBuilder sb = new StringBuilder();
        sb.append("n=20000\n");
        sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
        sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
        sb.append("select qty,price from mytrades where sym==`IBM;");
        BasicTable table = (BasicTable) conn.run(sb.toString());

        File f = new File("F:\\tmp\\test.dat");
        FileOutputStream fos = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        LittleEndianDataOutputStream dataStream = new LittleEndianDataOutputStream(bos);
        table.write(dataStream);
        bos.flush();
        dataStream.close();
        fos.close();
    }
    @Test
    public void testBasicTableDeserialize() throws IOException{

        File f = new File("F:\\tmp\\test.dat");
        FileInputStream fis = new FileInputStream("F:\\tmp\\test.dat");
        BufferedInputStream bis = new BufferedInputStream(fis);
        LittleEndianDataInputStream dataStream = new LittleEndianDataInputStream(bis);
        short flag = dataStream.readShort();
        BasicTable table = new BasicTable(dataStream);
    }
    @Test
    public void testDictionary() throws IOException {
        BasicDictionary dict = (BasicDictionary) conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        assertEquals(3, dict.rows());
    }

    @Test
    public void testFunction() throws IOException {
        List<Entity> args = new ArrayList<Entity>(1);
        double[] array = {1.5, 2.5, 7};
        BasicDoubleVector vec = new BasicDoubleVector(array);
        args.add(vec);
        Scalar result = (Scalar) conn.run("sum", args);
        assertEquals(11, ((BasicDouble) result).getDouble(), 2);
    }

    @Test
    public void testFunction1() throws IOException {
        Map<String, Entity> vars = new HashMap<String, Entity>();
        BasicDoubleVector vec = new BasicDoubleVector(3);
        vec.setDouble(0, 1.5);
        vec.setDouble(1, 2.5);
        vec.setDouble(2, 7);
        vars.put("a", vec);
        conn.upload(vars);
        Entity result = conn.run("accumulate(+,a)");
        assertEquals(11, ((BasicDoubleVector) result).getDouble(2), 1);
    }

    @Test
    public void testAnyVector() throws IOException, Exception {
        BasicAnyVector result = (BasicAnyVector) conn.run("(1, 2, (1,3, 5),(0.9, 0.8))");
        assertEquals(1, result.get(0).getNumber().intValue());

        result = (BasicAnyVector) conn.run("eachRight(def(x,y):x+y,1,(1,2,3))");
        assertEquals(2, result.get(0).getNumber().intValue());
    }

    @Test
    public void testSet() throws IOException {
        BasicSet result = (BasicSet) conn.run("set(1+3*1..100)");
        assertEquals(Entity.DATA_TYPE.DT_INT, result.getDataType());
        assertEquals(Entity.DATA_FORM.DF_SET, result.getDataForm());
    }

    @Test
    public void testChart() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("dates=(2012.01.01..2016.07.31)[def(x):weekday(x) between 1:5]\n");
        sb.append("chartData=each(cumsum,reshape(rand(10000,dates.size()*5)-4500, dates.size():5))\n");
        sb.append("chartData.rename!(dates, \"Strategy#\"+string(1..5))\n");
        sb.append("plot(chartData,,[\"Cumulative Pnls of Five Strategies\",\"date\",\"pnl\"],LINE)");
        BasicChart chart = (BasicChart) conn.run(sb.toString());
        assertTrue(chart.getTitle().equals("Cumulative Pnls of Five Strategies"));
        assertTrue(chart.isChart());
    }

    @Test
    public void testMatrixUpload() throws IOException {
        Entity a = conn.run("cross(+, 1..5, 1..5)");
        Entity b = conn.run("1..25$5:5");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("a", a);
        map.put("b", b);
        conn.upload(map);
        Entity matrix = conn.run("a+b");
        assertEquals(5, matrix.rows());
        assertEquals(5, matrix.columns());
        assertTrue(((BasicIntMatrix) matrix).get(0, 0).getString().equals("3"));
    }

    @Test
    public void testUserDefineFunction() throws IOException {
        conn.run("def f(a,b) {return a+b};");
        List<Entity> args = new ArrayList<Entity>(2);
        BasicInt arg = new BasicInt(1);
        BasicInt arg2 = new BasicInt(2);
        args.add(arg);
        args.add(arg2);
        BasicInt result = (BasicInt) conn.run("f", args);
        assertEquals(3, result.getInt());
    }

    @Test
    public void testFunctionIntMatrix() throws Exception {
        int nrow = 5;
        int ncol = 5;
        List<int[]> data = new ArrayList<int[]>();
        for (int i = 0; i < ncol; ++i) {
            int[] array = IntStream.range(i * nrow, i * nrow + nrow).toArray();
            data.add(array);
        }
        BasicIntMatrix matrix = new BasicIntMatrix(nrow, ncol, data);
        BasicIntVector lables = new BasicIntVector(IntStream.range(1, nrow + 1).toArray());
        matrix.setRowLabels(lables);
        lables = new BasicIntVector(IntStream.range(1, ncol + 1).toArray());
        matrix.setColumnLabels(lables);

        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicIntVector vector = (BasicIntVector) conn.run("flatten", args);
        assertEquals(4, vector.getInt(4));
    }

    @Test
    public void testFunctionDoubleMatrix() throws Exception {
        int nrow = 5;
        int ncol = 5;
        List<double[]> data = new ArrayList<double[]>();
        for (int i = 0; i < ncol; ++i) {
            double[] array = DoubleStream.iterate(i * nrow, n -> n + 1).limit(nrow).toArray();
            data.add(array);
        }
        BasicDoubleMatrix matrix = new BasicDoubleMatrix(nrow, ncol, data);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicDoubleVector vector = (BasicDoubleVector) conn.run("flatten", args);
        Double re = vector.getDouble(4);
        assertEquals(3.0, re, 1);
    }

    @Test
    public void testFunctionStrMatrix() throws Exception {
        List<String[]> data = new ArrayList<String[]>();
        String[] array = new String[]{"test1", "test2", "test3"};
        data.add(array);
        array = new String[]{"test4", "test5", "test6"};
        data.add(array);

        BasicStringMatrix matrix = new BasicStringMatrix(3, 2, data);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(matrix);
        BasicStringVector vector = (BasicStringVector) conn.run("flatten", args);
        String re = vector.getString(4);
        assertEquals("test5", re);
    }

    @Test
    public void Test_upload_table() throws IOException {
        BasicTable tb = (BasicTable) conn.run("table(1..100 as id,take(`aaa,100) as name)");
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_uploaded", (Entity) tb);
        conn.upload(upObj);
        BasicTable table = (BasicTable) conn.run("table_uploaded");
        assertEquals(100, table.rows());
        assertEquals(2, table.columns());
    }

    @Test
    public void testTableUpload() throws IOException {
        List<String> colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("value");
        colNames.add("x");

        List<Vector> cols = new ArrayList<Vector>();

        int[] intArray = new int[]{1, 2, 3, 4, 3};
        BasicIntVector vec = new BasicIntVector(intArray);
        cols.add(vec);

        double[] doubleArray = new double[]{7.8, 4.6, 5.1, 9.6, 0.1};
        BasicDoubleVector vecDouble = new BasicDoubleVector(doubleArray);
        cols.add(vecDouble);

        intArray = new int[]{5, 4, 3, 2, 1};
        vec = new BasicIntVector(intArray);
        cols.add(vec);

        BasicTable t1 = new BasicTable(colNames, cols);

        colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("qty");
        colNames.add("x");

        cols = new ArrayList<Vector>();
        intArray = new int[]{3, 1};
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
        BasicTable table = (BasicTable) conn.run("lj(t1, t2, `id)");
        assertEquals(5, table.rows());
    }

    @Test
    public void test_partialFunction() throws IOException {
        conn.run("share table(1..50 as id) as sharedTable");
        int[] intArray = new int[]{30, 40, 50};
        List<Entity> args = Arrays.asList(new BasicIntVector(intArray));
        conn.run("tableInsert{sharedTable}", args);
        BasicTable re = (BasicTable) conn.run("sharedTable");
        assertEquals(53, re.rows());
    }

    @Test
    public void test_tableInsertPartialFunction() throws IOException {

        String sql = "v=1..5;table(2019.01.01 12:00:00.000+v as OPDATE, `sd`d`d`d`d as OPMODE, take(`ss,5) as tsymbol, 4+v as tint, 3+v as tlong, take(true,5) as tbool, 2.5+v as tfloat)";
        BasicTable data = (BasicTable)conn.run(sql);
        List<Entity> args = Arrays.asList(data);
        conn.run("tb=table(100:0,`OPDATE`OPMODE`tsymbol`tint`tlong`tbool`tfloat,[TIMESTAMP,STRING,SYMBOL,INT,LONG,BOOL,FLOAT])");
        BasicInt re = (BasicInt)conn.run("tableInsert{tb}", args);
        assertEquals(5, re.getInt());
    }


    @Test
    public void testUUID() throws IOException {
        String uuidStr = "92274dfe-d589-4598-84a3-c381592fdf3f";
        BasicUuid a = BasicUuid.fromString(uuidStr);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(a);
        BasicString re = (BasicString) conn.run("string", args);
        assertEquals("92274dfe-d589-4598-84a3-c381592fdf3f", re.getString());
    }

    @Test
    public void testIPADDR_V6() throws IOException {
        String ipv6Str = "aba8:f04:e12c:e0aa:b967:f4bf:481c:d400";
        BasicIPAddr b = BasicIPAddr.fromString(ipv6Str);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(b);
        BasicIPAddr reip = (BasicIPAddr) conn.run("ipaddr", args);
        BasicString re = (BasicString) conn.run("string", args);
        assertEquals("aba8:f04:e12c:e0aa:b967:f4bf:481c:d400", re.getString());
    }

    @Test
    public void testIPADDR_V4() throws IOException {
        String ipv4Str = "192.168.1.142";
        BasicIPAddr b = BasicIPAddr.fromString(ipv4Str);
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(new BasicString(ipv4Str));
        BasicIPAddr reip = (BasicIPAddr) conn.run("ipaddr", args);
        assertEquals(ipv4Str, reip.getString());
    }

    @Test
    public void Test_ReLogin() throws IOException {
        conn.login("admin", "123456", false);
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10);	t = table(1..100 as id);db.createPartitionedTable(t,'t1', 'id')");
        conn.run("logout()");
        try {
            conn.run("exec count(*) from loadTable('dfs://db1','t1')");
            BasicInt re = (BasicInt) conn.run("exec count(*) from loadTable('dfs://db1','t1')");
        } catch (IOException ex) {
            assertTrue(ServerExceptionUtils.isNotLogin(ex.getMessage()));
        }

    }

    @Test
    public void testConnect(){
        DBConnection connClose = new DBConnection();
        try {
            connClose.connect(HOST, PORT, "admin", "123456");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        int connCount = getConnCount();
        connClose.close();
        int connCount1 = getConnCount();
        assertEquals(connCount - 1, connCount1);
    }
}
