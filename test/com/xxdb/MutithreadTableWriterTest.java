package com.xxdb;


import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.io.ProgressListener;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
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
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect("192.168.1.116", 18999, "admin", "123456");
        connection.run("x=0.1*(1..100)\n" +
                "y=0.1*(100..1)\n" +
                "t=table(x,y)\n" +
                "plot(t,extras={multiYAxes: true})");
//        connection.run("t = table(1:0, [`b], [DECIMAL32(2)]);share t as TT");
//        connection.run("t.append!(table([4,5,6] as b))");
//
//        BasicTable bt = (BasicTable) connection.run("TT");
//        List<Entity> argss = new ArrayList<>();
//        argss.add(bt);
//        connection.run("tableInsert{TT}", argss);
//        connection.run("t = table(1:0, [`b], [INT]);share t as TT");
//        connection.run("t.append!(table([400,500,600] as b))");
//        BasicTable bt = (BasicTable) connection.run("TT");
//        List<Entity> argss = new ArrayList<>();
//        argss.add(bt);
//        connection.run("tableInsert{TT}", argss);

    }
}
