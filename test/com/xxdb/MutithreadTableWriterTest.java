package com.xxdb;


import com.sun.org.apache.bcel.internal.generic.NEW;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.multithreadtablewriter.MultithreadTableWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MutithreadTableWriterTest implements Runnable{
    private Logger logger_=Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    public static String HOST="127.0.0.1" ;
    public static Integer PORT=8900 ;
    private final int id;
    private static MultithreadTableWriter mutithreadTableWriter_=null;

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
        mutithreadTableWriter_ = new MultithreadTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,10000, 10000,
                2, "bool");
        short s = 0;
        long l = 0;
        boolean b = mutithreadTableWriter_.insert(pErrorInfo, false, 'c', s,l);
        assertEquals(true, b);
        System.out.println(pErrorInfo);
        mutithreadTableWriter_.waitExit();
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
                if (mutithreadTableWriter_.insert(pErrorInfo, System.currentTimeMillis(), "A", lastid)==false) {
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
        mutithreadTableWriter_ = new MultithreadTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,10000, 1,
                5, "date");
        for (int i = 0; i < 100; i++) {
            new Thread(new MutithreadTableWriterTest(i)).start();
        }
        Thread.sleep(5000);
        conn.close();
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

        sb.append("t = streamTable(1000:0, `bools`datetime," +
                "[BOOL[],DATETIME]);" +
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
        LocalDate LD = LocalDate.of(2021, 12, 1);
        LocalDateTime LDT = LocalDateTime.of(2021, 12, 23, 11, 35, 44, 79846543);
        //System.out.println(by);
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");


        mutithreadTableWriter_ = new MultithreadTableWriter(HOST, PORT, "admin", "123456",
                "t1", "", false, false,null,5, 1,
                1, "bools");
        Boolean[] bools = new Boolean[]{false, true, false};
        //Entity entity = BasicEntityFactory.createScalar(Entity.DATA_TYPE.DT_BOOL, boo);
        //System.out.println(entity);
        for (int i=0;i<8;i++){
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,DT,DT,DT,DT,DT,DT,DT);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,calendar,calendar,calendar,calendar,calendar, calendar,calendar);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,LT,LT,LT,LT);
            //boolean b=mutithreadTableWriter_.insert(pErrorInfo,LD,LD);
            //boolean b = mutithreadTableWriter_.insert(pErrorInfo,bools,LDT,LDT,LDT,LDT);
            boolean b = mutithreadTableWriter_.insert(pErrorInfo,bools,LDT);
            assertTrue(b);
        }
        mutithreadTableWriter_.waitExit();
        BasicTable bt= (BasicTable) conn.run("select * from t1;");
        assertEquals(8,bt.rows());
        for (int i=0;i<bt.rows();i++){
            // assertEquals(sdf.format(DT),bt.getColumn(0).get(i).getString());
            System.out.println(bt.getColumn(4).get(i).getString());
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        conn = new DBConnection();
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
