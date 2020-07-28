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
import com.xxdb.data.*;
import com.xxdb.data.Vector;

import static org.junit.Assert.*;

public class DBConnectionTest {

    private DBConnection conn;
 //   public static String HOST = "127.0.0.1";
  //  public static Integer PORT = 28848;
    public static String HOST  = "localhost";
    public static Integer PORT = 8848;
    private int getConnCount() throws IOException{
        return ((BasicInt)conn.run("getClusterPerf().connectionNum[0]")).getInt();
    }

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
    public  void  testCharScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("'a'");
        assertEquals('a',((BasicByte)scalar).getByte());
    }
    @Test
    public void testShortScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("11h");
        assertEquals(11,((BasicShort)scalar).getShort());
    }
    @Test
    public  void  testIntScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("6");
        assertEquals(6,((BasicInt)scalar).getInt());
    }
    @Test
    public  void  testLongScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("22l");
        assertEquals(22,((BasicLong)scalar).getLong());
    }
    @Test
    public  void  testDateScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("2013.06.13");
        assertEquals(15869,((BasicDate)scalar).getInt());
    }
    @Test
    public  void  testMonthScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("2012.06M");
        assertEquals(24149,((BasicMonth)scalar).getInt());
    }
    @Test
    public  void  testTimeScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("13:30:10.008");
        assertEquals(48610008,((BasicTime)scalar).getInt());
    }
    @Test
    public  void  testMinuteScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("13:30m");
        assertEquals(810,((BasicMinute)scalar).getInt());
    }
    @Test
    public  void  testSecondScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("13:30:10");
        assertEquals(48610,((BasicSecond)scalar).getInt());
    }
    @Test
    public  void  testTimeStampScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("2012.06.13 13:30:10");
        assertEquals(1339594210,((BasicDateTime)scalar).getInt());
    }
    @Test
    public  void  testDateTimeScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("2012.06.13 13:30:10.008");
        assertEquals(1339594210008l,((BasicTimestamp)scalar).getLong());
    }
    @Test
    public  void  testNanoTimeScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("13:30:10.008007006");
        assertEquals(48610008007006l,((BasicNanoTime)scalar).getLong());
    }
    @Test
    public  void  testNanoTimeStampScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("2012.06.13 13:30:10.008007006");
        assertEquals(1339594210008007006l,((BasicNanoTimestamp)scalar).getLong());
    }
    @Test
    public  void  testStringScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("`IBM");
        assertEquals("IBM",((BasicString)scalar).getString());
    }
    @Test
    public  void  testBooleanScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("true");
        assertEquals(true,((BasicBoolean)scalar).getBoolean());
    }
    @Test
    public  void  testFloatScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("1.2f");
        assertEquals(1.2,((BasicFloat)scalar).getFloat(),2);
    }
    @Test
    public  void  testDoubleScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("1.22");
        assertEquals(1.22,((BasicDouble)scalar).getDouble(),2);
    }
    @Test
    public  void  testUuidScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')");
        assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee87",((BasicUuid)scalar).getString());
    }
    @Test
    public  void  testDateHourScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("datehour(2012.06.13 13:30:10)");
        assertEquals(372109,((BasicDateHour)scalar).getInt());
    }
    @Test
    public  void  testIpAddrScalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("ipaddr('192.168.1.13')");
        assertEquals("192.168.1.13",((BasicIPAddr)scalar).getString());
    }
    @Test
    public  void testInt128Scalar() throws IOException {
        Scalar scalar = (Scalar) conn.run("int128('e1671797c52e15f763380b45e841ec32')");
        assertEquals("e1671797c52e15f763380b45e841ec32",((BasicInt128)scalar).getString());
    }
    @Test
    public void testStringVector() throws IOException {
        BasicStringVector vector = (BasicStringVector) conn.run("`IBM`GOOG`YHOO");
        int size = vector.rows();
        assertEquals(3, size);
    }
    @Test
    public void testRunFuncArgs()
    {

    }
    @Test
    public void testFunctionDef() throws IOException {
        Entity obj = conn.run("def(a,b){return a+b}");
        assertEquals(Entity.DATA_TYPE.DT_FUNCTIONDEF, obj.getDataType());
        Entity AnonymousDef= conn.run("each(def(a,b):a+b, 1..10, 2..11);");
        int length = AnonymousDef.rows();
        assertEquals(10,length);
        StringBuilder sb = new StringBuilder();
        sb.append("a=100;");
        sb.append("g=add{a*a};");
        sb.append("g(8);");
        BasicInt res = (BasicInt) conn.run(sb.toString());
        assertEquals(10008,res.getInt());
        StringBuilder sb1 = new StringBuilder();
        sb1.append("def f(x):x pow 2 + 3*x + 4.0;");
        sb1.append("f(2);");
        BasicDouble lambda = (BasicDouble) conn.run(sb1.toString());
        assertEquals(14.0,lambda.getDouble(),2);
        StringBuilder sb2 = new StringBuilder();
        sb2.append("g=def(a){return def(b): a pow b};");
        sb2.append("g(10)(5);");
        BasicDouble closed = (BasicDouble) conn.run(sb2.toString());
        assertEquals(100000.0,closed.getDouble(),2);
        StringBuilder sb4 = new StringBuilder();
        sb4.append("x=[9,6,8];");
        sb4.append("def wage(x){if(x<=8) return 10*x; else return 20*x-80};");
        sb4.append("each(wage,x);");
        BasicIntVector each = (BasicIntVector) conn.run(sb4.toString());
        assertEquals(100,each.getInt(0));
        StringBuilder sb5 = new StringBuilder();
        sb5.append("t = table(1 2 3 as id, 4 5 6 as value,`IBM`MSFT`GOOG as name);");
        sb5.append("loop(max, t.values());");
        BasicAnyVector loop = (BasicAnyVector) conn.run(sb5.toString());
        assertEquals("3",loop.getEntity(0).getString());
    }

    @Test
    public void testScriptOutOfRange() throws IOException {
        try {
            conn.run("rand(1..10,10000000000000);");
        }
        catch(IOException ex){
            assertEquals("Out of memory",ex.getMessage());
        }
    }
    @Test
    public void testBoolVector() throws IOException{
        BasicBooleanVector vector = (BasicBooleanVector)conn.run("rand(1b 0b true false,10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testCharVector() throws IOException{
        BasicByteVector vector = (BasicByteVector)conn.run("rand('d' '1' '@',10)");
        int size = vector.rows();
        assertEquals(10,size);
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
    public void testFloatVector() throws IOException{
        BasicFloatVector vector = (BasicFloatVector)conn.run("rand(10.0f,10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testLongVector() throws IOException{
        BasicLongVector vector = (BasicLongVector)conn.run("rand(0l..11l,10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testShortVector() throws IOException{
        BasicShortVector vector = (BasicShortVector)conn.run("rand(1h..22h,10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testDateVector() throws IOException {
        BasicDateVector vector = (BasicDateVector) conn.run("2012.10.01 +1..10");
        int size = vector.rows();
        assertEquals(10, size);
    }
    @Test
    public void testMonthVector() throws IOException{
        BasicMonthVector vector = (BasicMonthVector)conn.run("2012.06M +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testTimeVector() throws IOException{
        BasicTimeVector vector = (BasicTimeVector)conn.run("13:30:10.008 +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testMinuteVector() throws IOException{
        BasicMinuteVector vector = (BasicMinuteVector)conn.run("13:30m +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testSecondVector() throws IOException{
        BasicSecondVector vector = (BasicSecondVector)conn.run("13:30:10 +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testTimeStampVector() throws IOException{
        BasicTimestampVector vector = (BasicTimestampVector)conn.run("2012.06.13 13:30:10.008 +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testNanoTimeVector() throws IOException{
        BasicNanoTimeVector vector = (BasicNanoTimeVector)conn.run("13:30:10.008007006 +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testNanoTimeStampVector() throws IOException{
        BasicNanoTimestampVector vector = (BasicNanoTimestampVector)conn.run("2012.06.13 13:30:10.008007006 +1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testDateTimeVector() throws IOException {

        BasicDateTimeVector vector = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
        int size = vector.rows();
        assertEquals(10, size);
    }
    @Test
    public void testUuidVector() throws IOException{
        BasicUuidVector vector = (BasicUuidVector)conn.run("take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testDateHourVector() throws IOException{
        BasicDateHourVector vector = (BasicDateHourVector)conn.run("datehour('2012.06.13T13')+1..10");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testIpAddrVector() throws IOException{
        BasicIPAddrVector vector = (BasicIPAddrVector)conn.run("rand(ipaddr('192.168.0.1'),10)");
        int size = vector.rows();
        assertEquals(10,size);
    }
    @Test
    public void testInt128Vector() throws IOException{
        BasicInt128Vector vector = (BasicInt128Vector)conn.run("rand(int128('e1671797c52e15f763380b45e841ec32'),10)");
        int size = vector.rows();
        assertEquals(10,size);
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
    public void testLongMatrix() throws IOException {
        BasicLongMatrix matrix = (BasicLongMatrix)conn.run("1l..6l$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testLongMatrixWithLabel() throws IOException {
        BasicLongMatrix matrix = (BasicLongMatrix)conn.run("cross(add,1l..5l,1l..10l)");
        assertEquals(5,matrix.rows());
        assertEquals(10,matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testShortMatrix() throws IOException {
        BasicShortMatrix matrix = (BasicShortMatrix)conn.run("1h..6h$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testShortMatrixWithLabel() throws IOException {
        BasicShortMatrix matrix = (BasicShortMatrix)conn.run("short(cross(add,1h..5h,1h..10h))");
        assertEquals(5,matrix.rows());
        assertEquals(10,matrix.columns());
        assertEquals("1", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testSymbolMatrix() throws IOException {
        BasicStringMatrix matrix = (BasicStringMatrix)conn.run("matrix(`SYMBOL,2,2, ,`T)");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
    }
    @Test
    public void testSymbolMatrixWithLabel() throws IOException {
        try{
            BasicStringMatrix matrix = (BasicStringMatrix)conn.run("cross(add,matrix(`SYMBOL,2,2, ,`T),matrix(`SYMBOL,2,2, ,`T))");
            assertEquals(0,matrix.rows());
            assertEquals(0,matrix.columns());
        }catch (IOException ex){
            assertEquals("The add function does not support symbol data",ex.getMessage());
        }
    }

    @Test
    public void testDoubleMatrix() throws IOException {
        BasicDoubleMatrix matrix = (BasicDoubleMatrix)conn.run("2.1 1.1 2.6$1:3");
        assertEquals(1,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testDoubleMatrixWithLabel() throws IOException {
        BasicDoubleMatrix matrix = (BasicDoubleMatrix)conn.run("cross(pow,2.1 5.0 4.88,1.0 9.6 5.2)");
        assertEquals(3,matrix.rows());
        assertEquals(3,matrix.columns());
        assertEquals("2.1", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testFloatMatrix() throws IOException {
        BasicFloatMatrix matrix = (BasicFloatMatrix)conn.run("2.1f 1.1f 2.6f$1:3");
        assertEquals(1,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testFloatMatrixWithLabel() throws IOException {
        BasicFloatMatrix matrix = (BasicFloatMatrix)conn.run("float(cross(pow,2.1f 5.0f 4.88f,1.0f 9.6f 5.2f))");
        assertEquals(3,matrix.rows());
        assertEquals(3,matrix.columns());
        assertEquals("2.1", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testBooleanMatrix() throws IOException {
        BasicBooleanMatrix matrix = (BasicBooleanMatrix)conn.run("rand(true false,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testBooleanMatrixWithLabel() throws IOException {
        BasicBooleanMatrix matrix = (BasicBooleanMatrix)conn.run("bool(cross(add,true false,false true))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("true", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testByteMatrix() throws IOException {
        BasicByteMatrix matrix = (BasicByteMatrix)conn.run("rand('q' '1' '*',6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testByteMatrixWithLabel() throws IOException {
        BasicByteMatrix matrix = (BasicByteMatrix)conn.run("cross(add,true false,false true)");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("true", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testDateHourMatrix() throws IOException {
        BasicDateHourMatrix matrix = (BasicDateHourMatrix)conn.run("rand(datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008]),6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testDateHourMatrixWithLabel() throws IOException {
        BasicDateHourMatrix matrix = (BasicDateHourMatrix)conn.run("datehour(cross(add,2012.06.15 15:32:10.158 2012.06.15 15:32:10.158,2012.06.15 17:30:10.008 2012.06.15 15:32:10.158))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("2012.06.15T15:32:10.158", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testMinuteMatrix() throws IOException {
        BasicMinuteMatrix matrix = (BasicMinuteMatrix)conn.run("rand(13:30m 16:19m,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testMinuteMatrixWithLabel() throws IOException {
        BasicMinuteMatrix matrix = (BasicMinuteMatrix)conn.run("minute(cross(add,13:30m 13:15m,14:30m 13:20m))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("13:30m", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testSecondMatrix() throws IOException {
        BasicSecondMatrix matrix = (BasicSecondMatrix)conn.run("rand(13:30:12 13:30:10,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testSecondMatrixWithLabel() throws IOException {
        BasicSecondMatrix matrix = (BasicSecondMatrix)conn.run("second(cross(add,13:30:12 13:30:10,13:30:12 13:30:10))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("13:30:12", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testMonthMatrix() throws IOException {
        BasicMonthMatrix matrix = (BasicMonthMatrix)conn.run("rand(2015.06M 2012.09M,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testMonthMatrixWithLabel() throws IOException {
        BasicMonthMatrix matrix = (BasicMonthMatrix)conn.run("month(cross(add,2015.06M 2012.09M,2015.06M 2012.09M))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("2015.06M", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testNanoTimeMatrix() throws IOException {
        BasicNanoTimeMatrix matrix = (BasicNanoTimeMatrix)conn.run("rand(17:30:10.008007006 17:35:10.008007006,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testNanoTimeMatrixWithLabel() throws IOException {
        BasicNanoTimeMatrix matrix = (BasicNanoTimeMatrix)conn.run("nanotime(cross(add,17:30:10.008007006 17:35:10.008007006,17:30:10.008007006 17:35:10.008007006))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("17:30:10.008007006", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testNanoTimestampMatrix() throws IOException {
        BasicNanoTimestampMatrix matrix = (BasicNanoTimestampMatrix)conn.run("rand(2014.06.13T13:30:10.008007006 2012.06.13T13:30:10.008007006,6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testNanoTimestampMatrixWithLabel() throws IOException {
        BasicNanoTimestampMatrix matrix = (BasicNanoTimestampMatrix)conn.run("nanotimestamp(cross(add,2012.06.13T13:30:10.008007006 2012.06.14T13:30:10.008007006,2012.06.13T13:30:10.008007006))");
        assertEquals(2,matrix.rows());
        assertEquals(1,matrix.columns());
        assertEquals("2012.06.13T13:30:10.008007006", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testDateMatrix() throws IOException {
        BasicDateMatrix matrix = (BasicDateMatrix)conn.run("rand(date([2013.06.13,2014.06.13]),6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testDateMatrixWithLabel() throws IOException {
        BasicDateMatrix matrix = (BasicDateMatrix)conn.run("date(cross(add,2013.06.13 2015.03.13,2012.06.15 2012.06.19))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("2013.06.13", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testDateTimeMatrix() throws IOException {
        BasicDateTimeMatrix matrix = (BasicDateTimeMatrix)conn.run("rand(datetime([2012.06.13T13:30:10,2012.06.13T16:30:10]),6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testDateTimeMatrixWithLabel() throws IOException {
        BasicDateTimeMatrix matrix = (BasicDateTimeMatrix)conn.run("datetime(cross(add,2012.06.13T13:30:10 2019.06.13T16:30:10,2013.06.13T13:30:10 2014.06.13T16:30:10))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("2012.06.13T13:30:10", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testTimeMatrix() throws IOException {
        BasicTimeMatrix matrix = (BasicTimeMatrix)conn.run("rand(time([13:31:10.008,12:30:10.008]),6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testTimeMatrixWithLabel() throws IOException {
        BasicTimeMatrix matrix = (BasicTimeMatrix)conn.run("time(cross(add,13:31:10.008 12:30:10.008,13:31:10.008 12:30:10.008))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("13:31:10.008", matrix.getRowLabel(0).getString());
    }
    @Test
    public void testTimestampMatrix() throws IOException {
        BasicTimestampMatrix matrix = (BasicTimestampMatrix)conn.run("rand(timestamp([2012.06.13T13:30:10.008,2014.06.13T13:30:10.008]),6)$2:3");
        assertEquals(2,matrix.rows());
        assertEquals(3,matrix.columns());
    }
    @Test
    public void testTimestampMatrixWithLabel() throws IOException {
        BasicTimestampMatrix matrix = (BasicTimestampMatrix)conn.run("timestamp(cross(add,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008))");
        assertEquals(2,matrix.rows());
        assertEquals(2,matrix.columns());
        assertEquals("2012.06.13T13:30:10.008", matrix.getRowLabel(0).getString());
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
    public void testDictionaryUpload() throws IOException {
        Entity dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("dict",dict);
        conn.upload(map);
        Entity dict1 = conn.run("dict");
        assertEquals(3, dict1.rows());
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
    public void testSetUpload() throws IOException {
        Entity set = conn.run("set(1+3*1..100)");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("set",set);
        conn.upload(map);
        Entity sets = conn.run("set");
        assertEquals(Entity.DATA_TYPE.DT_INT, sets.getDataType());
        assertEquals(Entity.DATA_FORM.DF_SET, sets.getDataForm());
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
    public void testScalarUpload() throws IOException {
        Map<String, Entity> map = new HashMap<String, Entity>();
        BasicInt a = (BasicInt) conn.run("1");
        map.put("a", a);
        BasicFloat f = (BasicFloat) conn.run("1.1f");
        map.put("f", f);
        BasicDouble d = (BasicDouble) conn.run("1.1");
        map.put("d", d);
        BasicLong l = (BasicLong) conn.run("1l");
        map.put("l", l);
        BasicShort s = (BasicShort) conn.run("1h");
        map.put("s", s);
        BasicBoolean b = (BasicBoolean) conn.run("true");
        map.put("b", b);
        BasicByte c = (BasicByte) conn.run("'a'");
        map.put("c", c);
        BasicString str = (BasicString) conn.run("`hello");
        map.put("str", str);
        BasicDate date = (BasicDate) conn.run("2013.06.13");
        map.put("date", date);
        BasicMonth month = (BasicMonth) conn.run("2012.06M");
        map.put("month", month);
        BasicTime time = (BasicTime) conn.run("13:30:10.008");
        map.put("time", time);
        BasicMinute minute = (BasicMinute) conn.run("13:30m");
        map.put("minute", minute);
        BasicSecond second = (BasicSecond) conn.run("13:30:10");
        map.put("second", second);
        BasicDateTime dateTime = (BasicDateTime) conn.run("2012.06.13 13:30:10");
        map.put("dateTime", dateTime);
        BasicTimestamp timestamp = (BasicTimestamp) conn.run("2012.06.13 13:30:10.008");
        map.put("timestamp", timestamp);
        BasicNanoTime nanoTime = (BasicNanoTime) conn.run("13:30:10.008007006");
        map.put("nanoTime", nanoTime);
        BasicNanoTimestamp nanoTimestamp = (BasicNanoTimestamp) conn.run("2012.06.13T13:30:10.008007006");
        map.put("nanoTimestamp", nanoTimestamp);
        BasicUuid uuid = (BasicUuid) conn.run("uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\")");
        map.put("uuid", uuid);
        BasicDateHour dateHour = (BasicDateHour) conn.run("datehour(\"2012.06.13T13\")");
        map.put("dateHour", dateHour);
        BasicIPAddr ipAddr = (BasicIPAddr) conn.run("ipaddr(\"192.168.1.13\")");
        map.put("ipAddr", ipAddr);
        BasicInt128 int128 = (BasicInt128) conn.run("int128(\"e1671797c52e15f763380b45e841ec32\")");
        map.put("int128", int128);
        conn.upload(map);
        BasicInt scalarInt = (BasicInt) conn.run("a");
        assertEquals(1, scalarInt.getInt());
        BasicFloat scalarFloat = (BasicFloat) conn.run("f");
        assertEquals(1.1, scalarFloat.getFloat(), 2);
        BasicDouble scalarDouble = (BasicDouble) conn.run("d");
        assertEquals(1.1, scalarDouble.getDouble(), 2);
        BasicLong scalarLong = (BasicLong) conn.run("l");
        assertEquals(1, scalarLong.getLong());
        BasicShort scalarShort = (BasicShort) conn.run("s");
        assertEquals(1, scalarShort.getShort());
        BasicBoolean scalarBool = (BasicBoolean) conn.run("b");
        assertEquals(true, scalarBool.getBoolean());
        BasicByte scalarChar = (BasicByte) conn.run("c");
        assertEquals('a', scalarChar.getByte());
        BasicString scalarStr = (BasicString) conn.run("str");
        assertEquals("hello", scalarStr.getString());
        BasicDate scalarDate = (BasicDate) conn.run("date");
        assertEquals(15869, scalarDate.getInt());
        BasicMonth scalarMonth = (BasicMonth) conn.run("month");
        assertEquals(24149, scalarMonth.getInt());
        BasicTime scalarTime = (BasicTime) conn.run("time");
        assertEquals(48610008, scalarTime.getInt());
        BasicMinute scalarMinute = (BasicMinute) conn.run("minute");
        assertEquals(810, scalarMinute.getInt());
        BasicSecond scalarSecond = (BasicSecond) conn.run("second");
        assertEquals(48610, scalarSecond.getInt());
        BasicDateTime scalarDateTime = (BasicDateTime) conn.run("dateTime");
        assertEquals(1339594210, scalarDateTime.getInt());
        BasicTimestamp scalarTimeStamp = (BasicTimestamp) conn.run("timestamp");
        assertEquals(1339594210008l, scalarTimeStamp.getLong());
        BasicNanoTimestamp scalarNanoTimeStamp = (BasicNanoTimestamp) conn.run("nanoTimestamp");
        assertEquals(1339594210008007006l, scalarNanoTimeStamp.getLong());
        BasicUuid scalarUuid = (BasicUuid) conn.run("uuid");
        assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee87", scalarUuid.getString());
        BasicDateHour scalarDateHour = (BasicDateHour) conn.run("dateHour");
        assertEquals(372109, scalarDateHour.getInt());
        BasicIPAddr scalarIPaddr = (BasicIPAddr) conn.run("ipAddr");
        assertEquals("192.168.1.13", scalarIPaddr.getString());
        BasicInt128 scalarInt182 = (BasicInt128) conn.run("int128");
        assertEquals("e1671797c52e15f763380b45e841ec32", scalarInt182.getString());
    }
        @Test
        public void testVectorUpload() throws IOException {
            Map<String, Entity> map = new HashMap<String, Entity>();
            BasicBooleanVector boolv = (BasicBooleanVector) conn.run("rand(1b 0b true false,10)");
            BasicByteVector bytev = (BasicByteVector) conn.run("rand('d' '1' '@',10)");
            BasicStringVector stringv = (BasicStringVector) conn.run("rand(`IBM`MSFT`GOOG`BIDU,10)");
            BasicIntVector intv = (BasicIntVector) conn.run("rand(1..10,10000)");
            BasicDoubleVector doublev = (BasicDoubleVector) conn.run("take(10.0,10)");
            BasicFloatVector floatV = (BasicFloatVector) conn.run("take(10.0f,10)");
            BasicLongVector longv = (BasicLongVector) conn.run("rand(1l..11l,10)");
            BasicShortVector shortv = (BasicShortVector) conn.run("take(22h,10)");
            BasicDateVector datev = (BasicDateVector) conn.run("2012.10.01 +1..10");
            BasicMonthVector monthv = (BasicMonthVector) conn.run("2012.06M +1..10");
            BasicTimeVector timev = (BasicTimeVector) conn.run("13:30:10.008 +1..10");
            BasicMinuteVector minutev = (BasicMinuteVector) conn.run("13:30m +1..10");
            BasicSecondVector secondv = (BasicSecondVector) conn.run("13:30:10 +1..10");
            BasicTimestampVector timestampv = (BasicTimestampVector) conn.run("2012.06.13 13:30:10.008 +1..10");
            BasicNanoTimeVector nanotimev = (BasicNanoTimeVector) conn.run("13:30:10.008007006 +1..10");
            BasicNanoTimestampVector nanotimestampv = (BasicNanoTimestampVector) conn.run("2012.06.13 13:30:10.008007006 +1..10");
            BasicDateTimeVector datetimev = (BasicDateTimeVector) conn.run("2012.10.01 15:00:04 + (rand(10000,10))");
            BasicUuidVector uuidv = (BasicUuidVector) conn.run("take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),98)");
            BasicDateHourVector datehourv = (BasicDateHourVector) conn.run("datehour('2012.06.13T13')+1..10");
            BasicIPAddrVector ipaddrv = (BasicIPAddrVector) conn.run("rand(ipaddr('192.168.0.1'),10)");
            BasicInt128Vector int128v = (BasicInt128Vector) conn.run("rand(int128('e1671797c52e15f763380b45e841ec32'),10)");
            map.put("boolv", boolv);
            map.put("bytev", bytev);
            map.put("stringv", stringv);
            map.put("intv", intv);
            map.put("doublev", doublev);
            map.put("floatV", floatV);
            map.put("longv", longv);
            map.put("shortv", shortv);
            map.put("datev", datev);
            map.put("monthv", monthv);
            map.put("timev", timev);
            map.put("minutev", minutev);
            map.put("secondv", secondv);
            map.put("timestampv", timestampv);
            map.put("nanotimev", nanotimev);
            map.put("nanotimestampv", nanotimestampv);
            map.put("datetimev", datetimev);
            map.put("uuidv", uuidv);
            map.put("datehourv", datehourv);
            map.put("ipaddrv", ipaddrv);
            map.put("int128v", int128v);
            conn.upload(map);
            BasicBooleanVector boolvRes = (BasicBooleanVector) conn.run("boolv");
            BasicByteVector bytevRes = (BasicByteVector) conn.run("bytev");
            BasicStringVector stringvRes = (BasicStringVector) conn.run("stringv");
            BasicIntVector intvRes = (BasicIntVector) conn.run("intv");
            BasicDoubleVector doublevRes = (BasicDoubleVector) conn.run("doublev");
            BasicFloatVector floatVRes = (BasicFloatVector) conn.run("floatV");
            BasicLongVector longvRes = (BasicLongVector) conn.run("longv");
            BasicShortVector shortvRes = (BasicShortVector) conn.run("shortv");
            BasicDateVector datevRes = (BasicDateVector) conn.run("datev");
            BasicMonthVector monthvRes = (BasicMonthVector) conn.run("monthv");
            BasicTimeVector timevRes = (BasicTimeVector) conn.run("timev");
            BasicMinuteVector minutevRes = (BasicMinuteVector) conn.run("minutev");
            BasicSecondVector secondvRes = (BasicSecondVector) conn.run("secondv");
            BasicTimestampVector timestampvRes = (BasicTimestampVector) conn.run("timestampv");
            BasicNanoTimeVector nanotimevRes = (BasicNanoTimeVector) conn.run("nanotimev");
            BasicNanoTimestampVector nanotimestampvRes = (BasicNanoTimestampVector) conn.run("nanotimestampv");
            BasicDateTimeVector datetimevRes = (BasicDateTimeVector) conn.run("datetimev");
            BasicUuidVector uuidvRes = (BasicUuidVector) conn.run("uuidv");
            BasicDateHourVector datehourvRes = (BasicDateHourVector) conn.run("datehourv");
            BasicIPAddrVector ipaddrvRes = (BasicIPAddrVector) conn.run("ipaddrv");
            BasicInt128Vector int128vRes = (BasicInt128Vector) conn.run("int128v");
            assertEquals(10,boolvRes.rows());
            assertEquals(10,bytevRes.rows());
            assertEquals(10000,intvRes.rows());
            assertEquals(10,stringvRes.rows());
            assertEquals(10,boolvRes.rows());
            assertEquals(10,doublevRes.rows());
            assertEquals(10,floatVRes.rows());
            assertEquals(10,longvRes.rows());
            assertEquals(10,shortvRes.rows());
            assertEquals(10,datevRes.rows());
            assertEquals(10,monthvRes.rows());
            assertEquals(10,timevRes.rows());
            assertEquals(10,minutevRes.rows());
            assertEquals(10,secondvRes.rows());
            assertEquals(10,timestampvRes.rows());
            assertEquals(10,nanotimevRes.rows());
            assertEquals(10,nanotimestampvRes.rows());
            assertEquals(10,datetimevRes.rows());
            assertEquals(98,uuidvRes.rows());
            assertEquals(10,datehourvRes.rows());
            assertEquals(10,ipaddrvRes.rows());
            assertEquals(10,int128vRes.rows());
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

        Entity matrixDoubleCross = conn.run("cross(pow,2.1 5.0 4.88,1.0 9.6 5.2)");
        Entity matrixDouble= conn.run("1..9$3:3");
        map.put("matrixDoubleCross", matrixDoubleCross);
        map.put("matrixDouble", matrixDouble);
        conn.upload(map);
        Entity matrixDoubleRes =  conn.run("matrixDoubleCross + matrixDouble");
        assertEquals(3, matrixDoubleRes.rows());
        assertEquals(3, matrixDoubleRes.columns());
        assertTrue(((BasicDoubleMatrix) matrixDoubleRes).get(0, 0).getString().equals("3.1"));

        Entity matrixFloatCross = conn.run("cross(pow,2.1f 5.0f 4.88f,1.0f 9.6f 5.2f)");
        Entity matrixFloat= conn.run("take(2.33f,9)$3:3");
        map.put("matrixFloatCross", matrixFloatCross);
        map.put("matrixFloat", matrixFloat);
        conn.upload(map);
        Entity matrixFloatRes =  conn.run("matrixFloatCross + matrixFloat");
        assertEquals(3, matrixFloatRes.rows());
        assertEquals(3, matrixFloatRes.columns());
        assertTrue(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString().equals("4.43"));

        Entity matrixlc = conn.run("cross(+, 1l..6l, -6l..-1l)");
        Entity matrixl= conn.run("1l..36l$6:6");
        map.put("matrixlc", matrixlc);
        map.put("matrixl", matrixl);
        conn.upload(map);
        Entity matrixlRes = conn.run("matrixlc+matrixl");
        assertEquals(6, matrixlRes.rows());
        assertEquals(6, matrixlRes.columns());
        assertTrue(((BasicLongMatrix) matrixlRes).get(0, 0).getString().equals("-4"));

        Entity matrixBoolCross = conn.run("bool(cross(add,true false,false true))");
        Entity matrixBool= conn.run("true true false false$2:2");
        map.put("matrixBoolCross", matrixBoolCross);
        map.put("matrixBool", matrixBool);
        conn.upload(map);
        Entity matrixBoolRes = conn.run("matrixBoolCross+matrixBool");
        assertEquals(2, matrixBoolRes.rows());
        assertEquals(2, matrixBoolRes.columns());
        assertTrue(((BasicByteMatrix) matrixBoolRes).get(0, 0).getString().equals("2"));

        Entity matrixDateHourCross = conn.run("datehour(cross(add,2012.06.15 15:32:10.158 2012.06.15 15:32:10.158,2012.06.15 17:30:10.008 2012.06.15 15:32:10.158))");
        Entity matrixDateHour= conn.run("take(datehour([2012.06.15 15:32:10.158]),4)$2:2");
        map.put("matrixDateHourCross", matrixDateHourCross);
        map.put("matrixDateHour", matrixDateHour);
        conn.upload(map);
        Entity matrixDateHourRes = conn.run("datehour(matrixDateHourCross+matrixDateHour)");
        assertEquals(2, matrixBoolRes.rows());
        assertEquals(2, matrixBoolRes.columns());
        assertTrue(((BasicDateHourMatrix) matrixDateHourRes).get(0, 0).getString().equals("+55468.01.20T21"));

        Entity matrixMinuteCross = conn.run("minute(cross(add,13:30m 13:15m 13:17m,0 1 -1))");
        Entity matrixMinute= conn.run("take(13:30m,9)$3:3");
        map.put("matrixMinuteCross", matrixMinuteCross);
        map.put("matrixMinute", matrixMinute);
        conn.upload(map);
        Entity matrixMinuteCrossRes = conn.run("matrixMinuteCross");
        Entity matrixMinuteRes = conn.run("matrixMinute");
        assertEquals(3, matrixMinuteCrossRes.rows());
        assertEquals(3, matrixMinuteCrossRes.columns());
        assertEquals(3, matrixMinuteRes.rows());
        assertEquals(3, matrixMinuteRes.columns());
        assertTrue(((BasicMinuteMatrix) matrixMinuteRes).get(0, 0).getString().equals("13:30m"));
        assertTrue(((BasicMinuteMatrix) matrixMinuteCrossRes).get(2, 2).getString().equals("13:16m"));

        Entity matrixSecondCross = conn.run("second(cross(add,13:30:12 13:30:10,1 10))");
        Entity matrixSecond= conn.run("take(13:30:10,4)$2:2");
        map.put("matrixSecondCross", matrixSecondCross);
        map.put("matrixSecond", matrixSecond);
        conn.upload(map);
        Entity matrixSecondRes = conn.run("matrixSecondCross+matrixSecond");
        assertEquals(2, matrixSecondRes.rows());
        assertEquals(2, matrixSecondRes.columns());
        assertTrue(((BasicIntMatrix) matrixSecondRes).get(0, 0).getString().equals("97223"));

        Entity matrixMonthCross = conn.run("month(cross(add,2016.06M 2015.06M 2015.07M ,0 1 -1))");
        Entity matrixMonth = conn.run("take(2017.06M,9)$3:3");
        map.put("matrixMonthCross", matrixMonthCross);
        map.put("matrixMonth", matrixMonth);
        conn.upload(map);
        Entity matrixMonthRes = conn.run("month(matrixMonthCross+matrixMonth)");
        assertEquals(3, matrixMonthRes.rows());
        assertEquals(3, matrixMonthRes.columns());
        assertTrue(((BasicMonthMatrix) matrixMonthRes).get(0, 0).getString().equals("4033.11M"));

        Entity matrixNanoTimeCross = conn.run("nanotime(cross(add,17:30:10.008007006 17:35:10.008007006,1 10))");
        Entity matrixNanoTime= conn.run("take(17:30:10.008007006,4)$2:2");
        map.put("matrixNanoTimeCross", matrixNanoTimeCross);
        map.put("matrixNanoTime", matrixNanoTime);
        conn.upload(map);
        Entity matrixNanoTimeCrossRes = conn.run("matrixNanoTimeCross");
        Entity matrixNanoTimeRes = conn.run("matrixNanoTime");
        assertEquals(2, matrixNanoTimeCross.rows());
        assertEquals(2, matrixNanoTimeCross.columns());
        assertEquals(2, matrixNanoTime.rows());
        assertEquals(2, matrixNanoTime.columns());
        assertTrue(((BasicNanoTimeMatrix) matrixNanoTimeRes).get(0, 0).getString().equals("17:30:10.008007006"));
        assertTrue(((BasicNanoTimeMatrix) matrixNanoTimeCrossRes).get(1, 1).getString().equals("17:35:10.008007016"));

        Entity matrixNanotsCross = conn.run("nanotimestamp(cross(add,2012.06.13T13:30:10.008007006 2012.06.14T13:30:10.008007006,100 102 63))");
        Entity matrixNanots = conn.run("take(2012.06.13T13:30:10.008007006,6)$2:3");
        map.put("matrixNanotsCross", matrixNanotsCross);
        map.put("matrixNanots", matrixNanots);
        conn.upload(map);
        Entity matrixNanotsRes = conn.run("nanotimestamp(matrixNanotsCross+matrixNanots)");
        assertEquals(2, matrixNanotsRes.rows());
        assertEquals(3, matrixNanotsRes.columns());
        assertTrue(((BasicNanoTimestampMatrix) matrixNanotsRes).get(0, 1).getString().equals("2054.11.25T03:00:20.016014114"));

        Entity matrixDateCross = conn.run("date(cross(add,2013.06.13 2015.03.13,2 -2 6))");
        Entity matrixDate = conn.run("take(1998.06.13,6)$2:3");
        map.put("matrixDateCross", matrixDateCross);
        map.put("matrixDate", matrixDate);
        conn.upload(map);
        Entity matrixDateRes = conn.run("date(matrixDate+matrixDateCross)");
        assertEquals(2, matrixDateRes.rows());
        assertEquals(3, matrixDateRes.columns());
        assertTrue(((BasicDateMatrix) matrixDateRes).get(0, 1).getString().equals("2041.11.21"));

        Entity matrixDateTimeCross = conn.run("datetime(cross(add,2012.06.13T13:30:10 2019.06.13T16:30:10,0 5 9 -9))");
        Entity matrixDateTime = conn.run("take(2012.06.13T13:30:10,8)$2:4");
        map.put("matrixDateTimeCross", matrixDateTimeCross);
        map.put("matrixDateTime", matrixDateTime);
        conn.upload(map);
        Entity matrixDateTimeRes = conn.run("datetime(matrixDateTimeCross+matrixDateTime)");
        assertEquals(2, matrixDateTimeRes.rows());
        assertEquals(4, matrixDateTimeRes.columns());
        assertTrue(((BasicDateTimeMatrix) matrixDateTimeRes).get(1, 1).getString().equals("1925.10.18T23:32:09"));

        Entity matrixTimeCross = conn.run("time(cross(add,13:31:10.008 12:30:10.008,1 2 3 -4))");
        Entity matrixTime= conn.run("take(1900.06.13T13:30:10,8)$4:2");
        map.put("matrixTimeCross", matrixTimeCross);
        map.put("matrixTime", matrixTime);
        conn.upload(map);
        Entity matrixTimeCrossRes = conn.run("matrixTimeCross");
        Entity matrixTimeRes = conn.run("matrixTime");
        assertEquals(2, matrixTimeCross.rows());
        assertEquals(4, matrixTimeCross.columns());
        assertEquals(4, matrixTime.rows());
        assertEquals(2, matrixTime.columns());
        assertTrue(((BasicTimeMatrix) matrixTimeCrossRes).get(1, 1).getString().equals("12:30:10.010"));

        Entity matrixTimeStampCross = conn.run("timestamp(cross(add,2012.06.13T13:30:10.008 2014.06.13T13:30:10.008,12 23 64))");
        Entity matrixTimeStamp = conn.run("take(2015.06.14T13:30:10.008,6)$2:3");
        map.put("matrixTimeStampCross", matrixTimeStampCross);
        map.put("matrixTimeStamp", matrixTimeStamp);
        conn.upload(map);
        Entity matrixTimeStampRes = conn.run("timestamp(matrixTimeStampCross+matrixTimeStamp)");
        assertEquals(2, matrixTimeStampRes.rows());
        assertEquals(3, matrixTimeStampRes.columns());
        assertTrue(((BasicTimestampMatrix) matrixTimeStampRes).get(1, 0).getString().equals("2059.11.25T03:00:20.028"));

    }

    @Test
    public void testStringMatrixUpload() throws IOException {
        HashMap<String, Entity> map = new HashMap<String, Entity>();
        Entity matrixString = conn.run("matrix(`SYMBOL,2,4, ,`T)");
        map.put("matrixString",matrixString);
        conn.upload(map);
        Entity matrixStringRes = conn.run("matrixString");
        assertEquals(2,matrixStringRes.rows());
        assertEquals(4,matrixStringRes.columns());
    }
@Test
public void testShortMatrixUpload() throws IOException {
    HashMap<String, Entity> map = new HashMap<String, Entity>();
    Entity matrixShort= conn.run("1h..36h$6:6");
    map.put("matrixShort", matrixShort);
    conn.upload(map);
    Entity matrixShortRes =  conn.run("matrixShort");
    assertEquals(6,matrixShortRes.rows());
    assertEquals(6,matrixShortRes.columns());
    map = new HashMap<String, Entity>();
    Entity matrixShortCross = conn.run("cross(+, 1h..6h, -6h..-1h)");
    map.put("matrixShort", matrixShort);
    map.put("matrixShortCross", matrixShortCross);
    conn.upload(map);
    assertEquals(6, matrixShortCross.rows());
    assertEquals(6, matrixShortCross.columns());
    assertTrue(((BasicIntMatrix) matrixShortCross).get(0, 0).getString().equals("-5"));

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
    public void TestShareTable() throws IOException {
        conn.run("share table(1 2 as id ,3 4 as val) as sharedT1");
        BasicTable tb = (BasicTable) conn.run("select count(*) from sharedT1");
        BasicInt res = (BasicInt) conn.run("exec count(*) from sharedT1");
        assertEquals(1,tb.rows());
        assertEquals(2,res.getInt());

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
    public void TestPartitionTable() throws IOException {
        //createPartitionTable
        StringBuilder sb = new StringBuilder();
        sb.append("n=10000;");
        sb.append("t = table(rand(1 2 3,n)as id,rand(1..10,n)as val);");
        sb.append("if(existsDatabase('dfs://db1')){ dropDatabase('dfs://db1')}");
        sb.append("db = database('dfs://db1',VALUE ,1 2);");
        sb.append("pt = db.createPartitionedTable(t,`pt,`id).append!(t);");
        conn.run(sb.toString());
        BasicLong res = (BasicLong) conn.run("exec count(*) from pt");
        assertEquals(true,res.getLong()>0);
        //addValuePartitions
        sb.append("addValuePartitions(db,3);");
        sb.append("pt.append!(t);");
        conn.run(sb.toString());
        BasicBoolean res3 = (BasicBoolean) conn.run("existsPartition('dfs://db1/3');");
        assertEquals(true,res3.getBoolean());
        //dropPartition
        conn.run("dropPartition(db,3);");
        res3 = (BasicBoolean) conn.run("existsPartition('dfs://db1/3');");
        assertEquals(false,res3.getBoolean());
        //addColumn
        sb.append("addColumn(pt,[\"x\", \"y\"],[INT, INT]);");
        sb.append("t1 = table(rand(1 2 3 4,n) as id,rand(1..10,n) as val,rand(1..5,n) as x,rand(1..10,n) as y );");
        sb.append("pt.append!(t1);");
        conn.run(sb.toString());
        BasicLong res_x = (BasicLong) conn.run("exec count(*) from pt where x=1");
        assertEquals(true,res_x.getLong()>0);
        //PartitionTableJoin
        sb.append("t2 = table(1 as id,2 as val);");
        sb.append("pt2 = db.createPartitionedTable(t2,`pt2,`id).append!(t2);");
        conn.run(sb.toString());
        BasicLong resej = (BasicLong) conn.run("exec count(*) from ej(pt,pt2,`id);");
        BasicLong respt = (BasicLong) conn.run("exec count(*) from pt;");
        assertEquals(true,resej.getLong()<respt.getLong());
    }


    @Test
    public  void  TestConnectSuccess() {
        conn = new DBConnection();
        try {
            if(! conn.connect(HOST, PORT, "admin", "123456")){
                throw new IOException("can not connect to dolphindb.");
            }
        } catch (IOException ex) {
           ex.printStackTrace();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public  void  TestConnectHostAndPortAreNull()  {
        DBConnection conn1 = new DBConnection();
        try {
            conn1.connect(null, -1, "admin", "123456");
        } catch ( IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void TestConnectErrorHostFormat() {
        DBConnection conn1 = new DBConnection();
        try {
            if(!conn1.connect("fee", PORT, "admin", "123456")) {
                throw new IOException("The host is error.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void TestConnectErrorHostValue() {
        DBConnection conn1 = new DBConnection();
        try {
            if(! conn1.connect("192.168.1.103", PORT, "admin", "123456")){
                throw new IOException("The host is error.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    @Test
    public  void TestConnectErrorPort() {
        DBConnection conn1 = new DBConnection();
        try {
            if(!conn1.connect(HOST, 44, "admin", "123456")){
                throw new IOException("The port is error.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    @Test
    public void TestConnectNullUserId() {
        DBConnection conn1 = new DBConnection();
        try {
            if(!conn1.connect(HOST, PORT, null, "123456")){
                throw new IOException("The user name or password is null.");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    @Test
    public void TestConnectNullUserIdAndPwd() {
        DBConnection conn1 = new DBConnection();
        try {
            if(!conn1.connect(HOST, PORT, null,"")){
                throw new IOException("The user name or password is null.");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    @Test
    public void TestConnectWrongPassWord(){
        DBConnection conn1 = new DBConnection();
        try {
            if (!conn1.connect(HOST,PORT,"admin","111")) {
                throw new IOException("The user name or password is incorrect.");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void TestCloseOnce() throws IOException {
        DBConnection connClose = new DBConnection();
        //连接一次
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
    @Test
    public void TestClose() throws IOException {
        //连接多次
        DBConnection connNew = new DBConnection();
        for(int i=0;i<10;i++){
            try{
                if(!connNew.connect(HOST,PORT,"admin","123456")){
                    throw new IOException("Failed to connect to  server");
                }
            }catch(IOException ex){
                ex.printStackTrace();
            }
            int connCount = getConnCount();
            connNew.close();
            int  connCount1 = getConnCount();
            assertEquals(connCount-1,connCount1);
        }
    }
}
