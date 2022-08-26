package com.xxdb;


import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class MutithreadTableWriterTest implements Runnable{
    private Logger logger_=Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    private final int id;
    private static MultithreadedTableWriter mutithreadedTableWriter_ =null;

    public MutithreadTableWriterTest(int i) {
        this.id=i;
    }
    @Override
    public void run() {

    }




    public static void main(String[] args) throws InterruptedException, IOException, Exception {
        DBConnection connection = new DBConnection();
        connection.connect("192.168.1.116", 18999, "admin", "123456");
        BasicDecimal32Vector bsv = (BasicDecimal32Vector) connection.run("[23$DECIMAL32(4), 1233$DECIMAL32(4), 9876$DECIMAL32(4)]");
        BasicDecimal32 bs = (BasicDecimal32) bsv.get(0);
        System.out.println(bs);
        BasicDecimal32 Decimal32 = new BasicDecimal32(1233,4);
        System.out.println(Decimal32);
    }
}
