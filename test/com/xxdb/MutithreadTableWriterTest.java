package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.BasicLong;
import com.xxdb.multithreadtablewriter.MultithreadTableWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class MutithreadTableWriterTest implements Runnable{
    private Logger logger_=Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    public static String HOST="192.168.1.182" ;
    public static Integer PORT=8848 ;
    private final int id;
    private static MultithreadTableWriter mutithreadTableWriter_=null;

    public MutithreadTableWriterTest(int i) {
        this.id=i;
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
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
