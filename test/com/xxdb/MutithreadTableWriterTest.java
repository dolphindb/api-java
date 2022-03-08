package com.xxdb;


import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MutithreadTableWriterTest implements Runnable{
    private Logger logger_=Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    public static String HOST="127.0.0.1" ;
    public static Integer PORT=8900 ;
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
        mutithreadedTableWriter_.waitExit();
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

    public void test2() throws Exception{
        StringBuilder sb = new StringBuilder();
        sb.append("t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;");
        conn.run(sb.toString());
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,10000, 1,
                5, "date");
        for (int i = 0; i < 100; i++) {
            new Thread(new MutithreadTableWriterTest(i)).start();
        }
        Thread.sleep(5000);
        conn.close();
    }


    public static void arrayVectorSetuptest() throws Exception{
        Vector value = BasicEntityFactory.instance().createVectorWithDefaultValue(Entity.DATA_TYPE.DT_INT, 100000);
        for (int i = 0; i < 10; i++){
            value.set(1, new BasicInt(i+1));
        }
        int[] a = new int[]{2,5,8,10};
        BasicArrayVector t = new BasicArrayVector(a, value);
        //BasicArrayVector t2 = (BasicArrayVector)conn.run("a = take(1..10,10);res = arrayVector([2,5,8,10],a);res");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("value", t);
        conn.upload(map);
    }

    public static void newtest() throws Exception{
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, `int`double," +
                "[INT,DOUBLE]);" +
                "share t as t1;");
        conn.run(sb.toString());
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "int");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i=0;i<20;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicInt(2));
            tb.add(row);
        }
        for (int i=0;i<2;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicInt(1));
            row.add(new BasicString("1"));
            tb.add(row);
        }
        boolean b= mutithreadedTableWriter_.insert(tb,pErrorInfo);
        assertEquals(true, b);
    }

    public static void testTransform() throws Exception{
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        /*
        sb.append("t = streamTable(1000:0, `date`month`second`datetime`timestamp`minute`datehour," +
                "[DATE,MONTH,SECOND,DATETIME,TIMESTAMP,MINUTE,DATEHOUR]);" +
                "share t as t1;");
        */
        /*
        sb.append("t = streamTable(1000:0, `date`month`second`datetime`timestamp`minute`datehour," +
                "[DATE,MONTH,SECOND,DATETIME,TIMESTAMP,MINUTE,DATEHOUR]);" +
                "share t as t1;");
        */
        /*
        sb.append("t = streamTable(1000:0, `time`minute`second`nanotime," +
                "[TIME,MINUTE,SECOND,NANOTIME]);" +
                "share t as t1;");
        */
        /*
        sb.append("t = streamTable(1000:0, `date`month," +
                "[DATE,MONTH]);" +
                "share t as t1;");
        */

        /*
        sb.append("t = streamTable(1000:0, `datetime`datehour`timestamp`nanotime`nanotimestamp," +
                "[BOOL[],DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP]);" +
                "share t as t1;");
        */

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
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,DT,DT,DT,DT,DT,DT,DT);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,calendar,calendar,calendar,calendar,calendar, calendar,calendar);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,LT,LT,LT,LT);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,LD,LD);
            //boolean b = mutithreadTableWriter_.insert(pErrorInfo,bools,LDT,LDT,LDT,LDT);
            boolean b = mutithreadedTableWriter_.insert(pErrorInfo,ll,dates);
            assertTrue(b);
        }
        mutithreadedTableWriter_.waitExit();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(rows,bt.rows());
        for (int i=0;i<bt.rows();i++){
            // assertEquals(sdf.format(DT),bt.getColumn(0).get(i).getString());
            System.out.println(bt.getColumn(0).getString());
        }
    }

    public static void arrayVectortest() throws Exception{

        BasicArrayVector t = (BasicArrayVector) conn.run("a = array(LONG[])\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100])\n" +
                "};a");
        System.out.println(t.get(0).getDataType());
        //List<Entity> args = new ArrayList<Entity>();
        //args.add(t);
        //conn.run("c = array(INT[], 0, 20)");
        //BasicArrayVector t2 = (BasicArrayVector)conn.run("append!{c}", args);
        //for (int i=0;i<t2.rows();i++){
            //System.out.println(t.getString(i));
        //}
        conn.close();
    }

    public static void testDelta() throws Exception{
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, [`long]," +
                "[LONG]);" +
                "share t as t1;");
        /*
        BasicLongVector t = (BasicLongVector)conn.run("l = [long(0)]\n" +
                ";l");
        //conn.run("share table(1:0, [`id], [LONG]) as t1");

        t.setCompressedMethod(Vector.COMPRESS_DELTA);
        List<String> colNames = new ArrayList<>();
        colNames.add("dd");
        List<Vector> col = new ArrayList<>();
        col.add(t);
        BasicTable table = new BasicTable(colNames, col);
        List<Entity> args = new ArrayList<>();
        args.add(table);
        conn.run("append!{t1}", args);
        */

        conn.run(sb.toString());
        long starttime=System.currentTimeMillis();
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "t1", "",
                false, false,null,10000, 10000,2, "long",
                new int[]{Vector.COMPRESS_DELTA});
        long l = 0;
        boolean b = mutithreadedTableWriter_.insert(pErrorInfo, l);
        assertEquals(true, b);
        System.out.println(pErrorInfo);
        mutithreadedTableWriter_.waitExit();
        System.out.println("time used "+(System.currentTimeMillis()-starttime));

        //Thread.sleep(2000);
        // BasicTable bt= (BasicTable) conn.run("select * from t1;");
        // System.out.println(bt.rows());
    }

    public static void testxx() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect("localhost", 8900);
        BasicArrayVector obj = (BasicArrayVector) conn.run("a = array(INT[], 0, 20)\n" +
                "for(i in 1..20){\n" +
                "\ta.append!([1..100])\n" +
                "};a");

        System.out.println(obj.getValue(0, 2).getDataType());
    }

    public static void testBlob() throws Exception{
        ErrorCodeInfo pErrorInfo=new ErrorCodeInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("t = streamTable(1000:0, [`blob],[BLOB]);share t as t1;");
        conn.run(sb.toString());
        mutithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,1, 1,
                1, "blob");
        List<List<Entity>> tb = new ArrayList<>();
        for (int i=0;i<100;i++){
            List<Entity> row = new ArrayList<>();
            row.add(new BasicString("453g",true));
            tb.add(row);
        }
        //conn.run("table"
        //BasicStringVector t = (BasicStringVector)conn.run("t = take(blob('453g'), 100);t;");
        //List<Entity> args = new ArrayList<>();
        //args.add(t);
        //conn.run("append!{t}", args);
        boolean b=mutithreadedTableWriter_.insert(tb,pErrorInfo);
        assertEquals(true, b);
        mutithreadedTableWriter_.waitExit();
        MultithreadedTableWriter.Status status=new MultithreadedTableWriter.Status();
        mutithreadedTableWriter_.getStatus(status);
        // conn.run(String.format("insert into t1 values('%s',%s)",1,"1232"));
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(100,bt.rows());
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        conn = new DBConnection(false, false, false);
        try {
            //Properties props = new Properties();
            //FileInputStream in= new FileInputStream( "test/com/xxdb/setup/settings.properties");
            //props.load(in);
            //PORT =Integer.parseInt(props.getProperty ("PORT"));
            //HOST  =props.getProperty ("HOST");
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            testTransform();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("test done");
    }
}
