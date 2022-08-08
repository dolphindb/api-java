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

    public static void testArrayV() throws Exception{
        DBConnection conn = new DBConnection(false,false,false);
        conn.connect(HOST, PORT,"admin","123456");
        conn.run("share table(100:0,[`arryInt],[INT[]]) as trades");
        List<Vector> l = new ArrayList<Vector>();
        int time=5;
        l.add(0,new BasicDoubleVector(new double[]{}));
//        for (int i=1;i<time;i++){
//            Vector v=new BasicDoubleVector(i);
//            v= (Vector) conn.run("double(1.."+i+")");
//            l.add(i,v);
//        }
//        BasicArrayVector arrayDouble = new BasicArrayVector(l);

        l = new ArrayList<Vector>();
        l.add(0,new BasicIntVector(new int[]{}));
        for (int i=1;i<time;i++){
            Vector v=new BasicIntVector(i);
            v= (Vector) conn.run("int(1.."+i*100000+")");
            l.add(i,v);
        }
        BasicArrayVector arryInt = new BasicArrayVector(l);

//        l = new ArrayList<Vector>();
//        l.add(0,new BasicDateVector(new int[]{}));
//        for (int i=1;i<time;i++){
//            Vector v=new BasicDateVector(i);
//            v= (Vector) conn.run("date(1.."+i+")");
//
//            l.add(i,v);
//        }
//        BasicArrayVector arryDate = new BasicArrayVector(l);

        List<String> colNames=new ArrayList<>();
//        colNames.add(0,"id");
        colNames.add(0,"arryInt");
//        colNames.add(2,"arrayDouble");
//        colNames.add(3,"arrayDate");

        List<Vector> cols=new ArrayList<>();
//        cols.add(0, (BasicIntVector)conn.run("1.."+time+""));
        cols.add(0,arryInt);
//        cols.add(2,arrayDouble);
//        cols.add(3,arryDate);
        BasicTable bt =new BasicTable(colNames,cols);
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{trades}", args);
        BasicTable res = (BasicTable) conn.run("select * from trades");
//        for (int i=0;i<time;i++){
//            assertEquals(arryInt.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arryInt")).getVectorValue(i).getString());
//            assertEquals(arrayDouble.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arrayDouble")).getVectorValue(i).getString());
//            assertEquals(arryDate.getVectorValue(i).getString(),((BasicArrayVector)res.getColumn("arrayDate")).getVectorValue(i).getString());
//        }
        conn.close();
    }

    public static void prepareData(int size, DBConnection connss) throws Exception {
        try {
            connss.run("undef(`data)");
            System.out.println("undef data success");
        } catch (Exception e) {

        }
        connss.run("testRow =" + size);
        connss.run("boolVec = take([true, false, NULL].bool(), testRow)");
        connss.run("charVec = take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char()");
        connss.run("shortVec = take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short()");
        connss.run("intVec = take((rand(100, 100) - 50).join([pow(2,32)-1, NULL, 0]), testRow).int()");
        connss.run("longVec = take((rand(100, 100) - 50).join([pow(2,64)-1, NULL, 0]), testRow).long()");
        connss.run("dateVec = take((rand(100, 100) - 50).join([pow(2,32)-1, NULL, 0]), testRow).date()");
        connss.run("monthVec = take((rand(100, 100)).join([pow(2,32)-1, NULL]), testRow).month()");
        connss.run("timeVec = take((rand(100, 100)).join([pow(2,32)-1, NULL, 0]), testRow).time()");
        connss.run("minuteVec = take((rand(100, 100)).join([pow(2,32)-1, NULL, 0]), testRow).minute()");
        connss.run("secondVec = take((rand(100, 100)).join([pow(2,32)-1, NULL, 0]), testRow).second()");
        connss.run("datetimeVec = take((rand(100, 100) - 50).join([pow(2,32)-1, NULL, 0]), testRow).datetime()");
        connss.run("timestampVec = take((rand(100, 100) - 50).join([pow(2,64)-1, NULL, 0]), testRow).timestamp()");
        connss.run("nanotimeVec = take((rand(100, 100)).join([pow(2,64)-1, NULL, 0]), testRow).nanotime()");
        connss.run("nanotimestampVec = take((rand(100, 100) - 50).join([pow(2,64)-1, NULL, 0]), testRow).nanotimestamp()");
        connss.run("floatVec = take(rand(10000000.0, 100).float().join(NULL), testRow)");
        connss.run("doubleVec = take(rand(10000000.0, 100).double().join(NULL), testRow)");
        connss.run("symbolVec = take(`AAA`BBB`CCC`中文, testRow).symbol()");
        connss.run("stringVec = take(`AAA`BBB`CCC`中文, testRow).string()");
        connss.run("uuidVec = rand(uuid(), testRow).uuid()");
        connss.run("datehourVec = take((rand(100, 100) - 50).join([pow(2,32)-1, NULL, 0]), testRow).datehour()");
        connss.run("ipaddrVec = rand(ipaddr(), testRow).ipaddr()");
        connss.run("int128Vec = rand(int128(), testRow).int128()");
        connss.run("blobVec = rand(['AAA', 'BBB', 'CCC', '\\0\\1'].string().blob(), testRow)");
        connss.run("tmp = table(boolVec as cbool, charVec as cchar, shortVec as cshort, intVec as cint, longVec as clong, " +
                "dateVec as cdate, monthVec as cmonth, timeVec as ctime, minuteVec as cminute, secondVec as csecond, datetimeVec as cdatetime, " +
                "timestampVec as ctimestamp, nanotimeVec as cnanotime, nanotimestampVec as cnanotimestamp, " +
                "floatVec as cfloat, doubleVec as cdouble, symbolVec as csymbol, stringVec as cstring, " +
                "uuidVec as cuuid, datehourVec as cdatehour, ipaddrVec as cipaddr, int128Vec as cint128, blobVec as cblob)");
        connss.run("share tmp as data");
    }

    public static void main(String[] args) throws InterruptedException, IOException, Exception {
        DBConnection connection = new DBConnection();
        connection.connect("192.168.1.116", 18999, "admin", "123456");
        BasicTable bt = (BasicTable) connection.run("\n" +
                "loadText(\"/home/Data/data.csv\");\n");
        String ss = bt.getRowJson(20);
        String s2 = String.valueOf(bt.getColumn(2).get(20).getNumber().doubleValue());
        String s1 = bt.getColumn(2).get(20).getJsonString();
        double d = bt.getColumn(2).get(20).getNumber().doubleValue();
        int a = 0;
    }
}
