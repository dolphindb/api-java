package com.xxdb;


import com.alibaba.fastjson.support.odps.udf.CodecCheck;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MutithreadTableWriterTest implements Runnable{
    private Logger logger_=Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    public static String HOST="192.168.1.116" ;
    public static Integer PORT=8999;
    private final int id;
    private static MultithreadedTableWriter mutithreadedTableWriter_ =null;

    public MutithreadTableWriterTest(int i) {
        this.id=i;
    }

    public static void test() throws Exception{
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();

        sb.append("t = streamTable(1000:0, `bool`char`short`long," +
                "[BOOL,CHAR,SHORT,LONG]);" +
                "share t as t1;");
        conn.run(sb.toString());
        long starttime=System.currentTimeMillis();
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,10000, 10000,
                2, "bool");
        short s = 0;
        long l = 0;
        boolean b = mutithreadedTableWriter_.insert(pErrorInfo, false, 'c', s,l);
        assertEquals(true, b);
        System.out.println(pErrorInfo);
        mutithreadedTableWriter_.waitForThreadCompletion();
        System.out.println("time used "+(System.currentTimeMillis()-starttime));
        Thread.sleep(2000);
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        System.out.println(bt.rows());
    }
    @Override
    public void run() {
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        try {
            int lastid = 0;
            for (int i = 0; i < 5000; i++) {
                lastid ++;
                if (mutithreadedTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", lastid)==false) {
                    logger_.warning("mutithreadTableWriter_.insert error "+pErrorInfo);
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }finally {
        }
    }

    public static void testTransform() throws Exception{
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();

        sb.append("t = streamTable(1000:0, `nanotime`dates," +
                "[NANOTIMESTAMP,DATE[]]);" +
                "share t as t1;");

        conn.run(sb.toString());
        //Date DT=new Date(2021,12,24,10,24,49);

        //LocalTime LT = LocalTime.now();


        Calendar calendar=Calendar.getInstance();
        calendar.set(Calendar.YEAR,2021);
        calendar.set(Calendar.MONTH,0);
        calendar.set(Calendar.DAY_OF_MONTH,29);
        calendar.set(Calendar.HOUR_OF_DAY,10);
        calendar.set(Calendar.MINUTE,30);
        calendar.set(Calendar.SECOND,49);
        calendar.set(Calendar.MILLISECOND,851);

        LocalTime LT = LocalTime.of(10, 39, 21, 6743329);
        Date DT = new Date();
        Long ll = DT.getTime();
        LocalDate LD = LocalDate.of(2021, 12, 1);
        LocalDateTime LDT = LocalDateTime.of(2021, 12, 23, 11, 35, 44, 79846543);
        System.out.println(ll);
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
        Date[] dates = new Date[]{DT, DT, DT, DT};

        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,5, 1,
                1, "dates");
        Integer[] a = new Integer[]{1,2,3,4,5};
        Boolean[] bs = new Boolean[]{false, true, false, true};
        //Entity entity = BasicEntityFactory.createScalar(Entity.DATA_TYPE.DT_BOOL, boo);
        //System.out.println(entity);
        int rows = 8;
        for (int i=0;i<rows;i++){
            boolean b = mutithreadedTableWriter_.insert(pErrorInfo,ll,dates);
            assertTrue(b);
        }
        mutithreadedTableWriter_.waitForThreadCompletion();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(rows,bt.rows());
        for (int i=0;i<bt.rows();i++){
            // assertEquals(sdf.format(DT),bt.getColumn(0).get(i).getString());
            System.out.println(bt.getColumn(0).getString());
        }
    }


    public static void testxx() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect("localhost", 8900);
        BasicArrayVector obj = (BasicArrayVector) conn.run("a = array(INT[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100])\n" +
                "};a");

        System.out.println(obj.getVectorValue(0).getDataType());
    }

    public static void testBlob()throws Exception{
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String script="t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob," +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share t as t1;" +
                "tt = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,\n" +
                "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, int128,INT,BLOB]);" +
                "share tt as t2;";
        conn.run(script);
        mutithreadedTableWriter_ = new MultithreadedTableWriter("192.168.1.182", 8848, "admin", "123456",
                "t1", "", false, false, null, 10, 1,
                1, "short");

        Month mon=LocalDate.of(2022,2,2).getMonth();
        for (int i = 0; i < 10000; i++) {
            System.out.println("i1: " + i);
            boolean b=mutithreadedTableWriter_.insert(pErrorInfo,new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),new BasicFloat(2.312f),new BasicDouble(3.2),
                    new BasicString("sedf"+i),new BasicString("sedf"),new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),
                    new BasicInt(21),new BasicString("d"+i,true));
            assertTrue(b);
            System.out.println("i2: " + i);
            List<Entity> args = Arrays.asList(new BasicBoolean(true),new BasicByte((byte)'w'),new BasicShort((short)2),new BasicLong(4533l),
                    new BasicDate(LocalDate.of(2022,2,2)), new BasicMonth(2002,mon),new BasicSecond(LocalTime.of(2,2,2)),
                    new BasicDateTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTime(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicNanoTimestamp(LocalDateTime.of(2000,2,2,3,2,3,2)),
                    new BasicFloat(2.312f),new BasicDouble(3.2),new BasicString("sedf"+i),new BasicString("sedf"),
                    new BasicUuid(23424,4321423),new BasicIPAddr(23424,4321423),new BasicInt128(23424,4321423),new BasicInt(21),
                    new BasicString("d"+i,true));
//            List<Entity> args = Arrays.asList(new BasicString("d"+i,true));
            conn.run("tableInsert{t2}", args);
            System.out.println("i3: " + i);
        }
        Thread.sleep(2000);
        BasicTable ex = (BasicTable) conn.run("select * from t1 order by symbol");
        BasicTable  res= (BasicTable) conn.run("select * from t2 order by symbol");
        assertEquals(10000,ex.rows());
        mutithreadedTableWriter_.waitForThreadCompletion();
    }

    public static void testxxxxx() throws Exception{
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db1=database(\"\", RANGE, 2022.01.01+(0..10)*2)\n" +
                "\tdb2=database(\"\", HASH,[INT,3])\n" +
                "\tdb=database(dbName, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n";
        conn.run(script);
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 1000, 1,
                2, "volume");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 200000; i++) {
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("2"));
            row.add(new BasicNanoTimestamp(LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0)));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicDouble(i + 0.1));
            row.add(new BasicInt(i % 10));
            row.add(new BasicDouble(i + 0.1));
            tb.add(row);
            conn.run("tableInsert{t1}", row);
            if (i < 190000){
                boolean b = mutithreadedTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
                assertEquals(true, b);
            }
//            boolean b = mutithreadedTableWriter_.insert(pErrorInfo, "2", LocalDateTime.of(2022, 1, 1 + i % 10, 1, 1, 0, 0), i + 0.1, i + 0.1, i % 10, i + 0.1);
//            assertEquals(true, b);
            if (i %1000 == 0){
                System.out.println(i);
            }
        }
        mutithreadedTableWriter_.waitForThreadCompletion();
        BasicTable bt = (BasicTable) conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;");
        BasicInt bl = (BasicInt) conn.run("exec count(*) from t1") ;
        System.out.println("bl: " + bl.getInt());
        System.out.println("bt: " + bt.rows());
        MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
        mutithreadedTableWriter_.getStatus(status);
        status.hasError();
    }

    public static void testSymbol() throws Exception{
        String dbName = "dfs://test_MultithreadedTableWriter";
        String script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n" +
                "if(exists(dbName)){\n" +
                "\tdropDatabase(dbName)\t\n" +
                "}\n" +
                "db=database(dbName, LIST, [`IBM`ORCL`MSFT, `GOOG`FB])\n" +
                "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [STRING, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
                "\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"sym\"])\n";
        conn.run(script);
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                dbName, "pt", false, false, null, 10, 1,
                4, "sym");
        Integer threadTime = 10;
        ErrorCodeInfo pErrorInfo = new ErrorCodeInfo();
        List<List<Entity>> tb = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            boolean b = mutithreadedTableWriter_.insert(pErrorInfo, "IBM", LocalDateTime.of(2022, 1, (i % 10) + 1, (i % 10) + 1, (i % 10) + 10, 0), i + 0.1, i + 0.1, (i % 10) + 1, i + 0.1);
            System.out.println(i);
            assertEquals(true, b);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        conn = new DBConnection(false, false, false);
        try {
            //Properties props = new Properties();
            //FileInputStream in= new FileInputStream( "test/com/xxdb/setup/settings.properties");
            //props.load(in);
            //PORT =Integer.parseInt(props.getProperty ("PORT"));
            //HOST  =props.getProperty ("HOST");
            if (!conn.connect("192.168.1.116", 8999, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            testSymbol();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("test done");
    }
}
