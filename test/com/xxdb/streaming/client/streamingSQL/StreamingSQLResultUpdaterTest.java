package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import static com.xxdb.Prepare.*;
import static com.xxdb.Prepare.checkData;
import static com.xxdb.streaming.client.streamingSQL.StreamingSQLClientTest.writer_data;
import static com.xxdb.streaming.client.streamingSQL.StreamingSQLClientTest.writerdata_array;

public class StreamingSQLResultUpdaterTest {
    public static DBConnection conn;
    public static DBConnection conn1;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @BeforeClass
    public static void setUp() throws IOException {
        try {clear_env_1();}catch (Exception e){}
    }

    @Before
    public void clear() throws IOException {
        conn = new DBConnection();
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try{conn.run("login(`admin, `123456)\n" +
                "    res = getStreamingSQLStatus()\n" +
                "    for(sqlStream in res){\n" +
                "        try{unsubscribeStreamingSQL(, sqlStream.queryId)}catch(ex){print ex}\n" +
                "        try{revokeStreamingSQL(sqlStream.queryId)}catch(ex){print ex}\n" +
                "    }\n" +
                "    go;\n" +
                "    try{revokeStreamingSQLTable(`t1)}catch(ex){print ex}\n" +
                "    try{revokeStreamingSQLTable(`t2)}catch(ex){print ex}\n" +
                "    try{undef(`t1,SHARED)}catch(ex){print ex}\n" +
                "    try{undef(`t2,SHARED)}catch(ex){print ex}");}catch (Exception ex){}
    }

    @After
    public void after() throws IOException, InterruptedException {
        try {clear_env();}catch (Exception e){}
        conn.close();
    }

    public static void Preparedata(String dataType) throws IOException {
        String script = "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, "+dataType +"]) as t1;\n" +
                "share keyedTable(`id, 1:0, `id`time`value, [SYMBOL, TIMESTAMP, "+dataType +"]) as t2;";
        conn.run(script);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
    }

    public static void wait(BasicTable bt) throws InterruptedException {
        int count = 0;
        int count1= 0;
        int count2= 1;
        Thread.sleep(1000);
        while (!(count1==count2)&&count<=100)
        {
            count=count++;
            count1 = bt.rows();
            System.out.println("count1:"+bt.rows());
            Thread.sleep(1000);
            count2 = bt.rows();
            System.out.println("count2:"+bt.rows());
        }
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(1000);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        //update
        conn.run("update t1 set value = value+10");
        conn.run("update t2 set value = value+10.4");
        Thread.sleep(1000);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex1, bt);
        //update
        conn.run("update t1 set value = NULL where id in(`A+string(1..10))");
        conn.run("update t2 set value = NULL where id in(`A+string(1..20))");
        Thread.sleep(500);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_allDateType() throws IOException, InterruptedException {
        Preparedata_keyTable(100);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.boolv, t1.charv, t1.shortv, t1.intv, t1.longv, t1.doublev, t1.floatv, t1.datev, t1.monthv, t1.timev, t1.minutev, t1.secondv, t1.datetimev, t1.timestampv, t1.nanotimev, t1.nanotimestampv, t1.stringv, t1.datehourv, t1.uuidv, t1.ippaddrv, t1.int128v, t1.blobv, t1.pointv, t1.complexv, t1.decimal32v, t1.decimal64v, t1.decimal128v, t1.intv+t2.intv as value  FROM t1 INNER JOIN t2 ON t1.tv = t2.tv order by id, boolv,charv,shortv,intv,longv";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("update t1 set boolv=true, charv='1', shortv=1, intv=1, longv=1, doublev=1.0, floatv=1.0, datev=date(now()), monthv=month(now()), timev=time(now()), minutev=minute(now()), secondv=second(now()), datetimev=datetime(now()), timestampv=timestamp(now()), nanotimev=nanotime(now()), nanotimestampv=nanotimestamp(now()), stringv=\"erere\", datehourv=datehour(now()), uuidv=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"), ippaddrv=ipaddr(\"192.168.100.10\"), int128v=int128(\"e1671797c52e15f763380b45e841ec32\"), blobv=\"blobbbbbbb@@@##$%^&*()!\", pointv=point(1,1), complexv=complex(1,1), decimal32v=decimal32(1,1), decimal64v=decimal64(1,1), decimal128v=decimal128 (1,1) where  id in(`A+string(1..30))");
        conn.run("update t2 set boolv=true, charv='1', shortv=1, intv=1, longv=1, doublev=1.0, floatv=1.0, datev=date(now()), monthv=month(now()), timev=time(now()), minutev=minute(now()), secondv=second(now()), datetimev=datetime(now()), timestampv=timestamp(now()), nanotimev=nanotime(now()), nanotimestampv=nanotimestamp(now()), stringv=\"erere\", datehourv=datehour(now()), uuidv=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"), ippaddrv=ipaddr(\"192.168.100.10\"), int128v=int128(\"e1671797c52e15f763380b45e841ec32\"), blobv=\"blobbbbbbb@@@##$%^&*()!\", pointv=point(1,1), complexv=complex(1,1), decimal32v=decimal32(1,1), decimal64v=decimal64(1,1), decimal128v=decimal128 (1,1) where  id in(`A+string(20..30))");
        Thread.sleep(500);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex1, bt);
        //update
        conn.run("update t1 set boolv=NULL, charv=NULL, shortv=NULL, intv=NULL, longv=NULL, doublev=NULL, floatv=NULL, datev=NULL, monthv=NULL, timev=NULL, minutev=NULL, secondv=NULL, datetimev=NULL, timestampv=NULL, nanotimev=NULL, nanotimestampv=NULL, stringv=NULL, datehourv=NULL, uuidv=NULL, ippaddrv=NULL, int128v=NULL, blobv=NULL, pointv=NULL, complexv=NULL, decimal32v=NULL, decimal64v=NULL, decimal128v=NULL where  id in(`A+string(1..10))");
        conn.run("update t2 set boolv=NULL, charv=NULL, shortv=NULL, intv=NULL, longv=NULL, doublev=NULL, floatv=NULL, datev=NULL, monthv=NULL, timev=NULL, minutev=NULL, secondv=NULL, datetimev=NULL, timestampv=NULL, nanotimev=NULL, nanotimestampv=NULL, stringv=NULL, datehourv=NULL, uuidv=NULL, ippaddrv=NULL, int128v=NULL, blobv=NULL, pointv=NULL, complexv=NULL, decimal32v=NULL, decimal64v=NULL, decimal128v=NULL where  id in(`A+string(1..20))");
        Thread.sleep(1000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);

        //update
        conn.run("update t1 set boolv=true, charv='1', shortv=1, intv=1, longv=1, doublev=1.0, floatv=1.0, datev=date(now()), monthv=month(now()), timev=time(now()), minutev=minute(now()), secondv=second(now()), datetimev=datetime(now()), timestampv=timestamp(now()), nanotimev=nanotime(now()), nanotimestampv=nanotimestamp(now()), stringv=\"erere\", datehourv=datehour(now()), uuidv=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"), ippaddrv=ipaddr(\"192.168.100.10\"), int128v=int128(\"e1671797c52e15f763380b45e841ec32\"), blobv=\"blobbbbbbb@@@##$%^&*()!\", pointv=point(1,1), complexv=complex(1,1), decimal32v=decimal32(1,1), decimal64v=decimal64(1,1), decimal128v=decimal128 (1,1) where  id in(`A+string(30..31))");
        conn.run("update t2 set boolv=true, charv='1', shortv=1, intv=1, longv=1, doublev=1.0, floatv=1.0, datev=date(now()), monthv=month(now()), timev=time(now()), minutev=minute(now()), secondv=second(now()), datetimev=datetime(now()), timestampv=timestamp(now()), nanotimev=nanotime(now()), nanotimestampv=nanotimestamp(now()), stringv=\"erere\", datehourv=datehour(now()), uuidv=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"), ippaddrv=ipaddr(\"192.168.100.10\"), int128v=int128(\"e1671797c52e15f763380b45e841ec32\"), blobv=\"blobbbbbbb@@@##$%^&*()!\", pointv=point(1,1), complexv=complex(1,1), decimal32v=decimal32(1,1), decimal64v=decimal64(1,1), decimal128v=decimal128 (1,1) where  id in(`A+string(32..33))");
        Thread.sleep(1000);
        BasicTable ex3 = (BasicTable)conn.run(sqlStr1);
        checkData(ex3, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_bool() throws IOException, InterruptedException {
        Preparedata("BOOL[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","bool");
        writerdata_array(100,5,"t2","bool");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_char() throws IOException, InterruptedException {
        Preparedata("CHAR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","char");
        writerdata_array(100,5,"t2","char");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_short() throws IOException, InterruptedException {
        Preparedata("SHORT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","short");
        writerdata_array(100,5,"t2","short");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_int() throws IOException, InterruptedException {
        Preparedata("INT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","int");
        writerdata_array(100,5,"t2","int");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_same_rows_many_times() throws IOException, InterruptedException {
        Preparedata("INT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","int");
        writerdata_array(100,5,"t2","int");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value ;\n" +
                "update t2 set  t1.value=tmptt.value ;");
        Thread.sleep(2000);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex1, bt);
        //update  如果整列更新成空值， server 也会有问题 ，先不管
//        conn.run("insert into t1 values (`A601,2025.08.26T12:36:23.639,);\n" +
//                "tmptt =(exec * FROM t1 where id = `A601 limit 1)[0];\n" +
//                "update t1 set  value=tmptt.value ;\n" +
//                "update t2 set  value=tmptt.value ;");
//        Thread.sleep(2000);
//        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
//        System.out.println(bt.getString());
//        checkData(ex2, bt);

        conn.run("tmptt =(exec * FROM t1 where id = `A20 limit 1)[0];\n" +
                "update t1 set  value=tmptt.value ;\n" +
                "update t2 set  value=tmptt.value ;");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);

        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_long() throws IOException, InterruptedException {
        Preparedata("LONG[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","long");
        writerdata_array(100,5,"t2","long");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_double() throws IOException, InterruptedException {
        Preparedata("DOUBLE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","double");
        writerdata_array(100,5,"t2","double");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_float() throws IOException, InterruptedException {
        Preparedata("FLOAT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","float");
        writerdata_array(100,5,"t2","float");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_date() throws IOException, InterruptedException {
        Preparedata("DATE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","date");
        writerdata_array(100,5,"t2","date");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_month() throws IOException, InterruptedException {
        Preparedata("MONTH[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","month");
        writerdata_array(100,5,"t2","month");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_time() throws IOException, InterruptedException {
        Preparedata("TIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","time");
        writerdata_array(100,5,"t2","time");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_minute() throws IOException, InterruptedException {
        Preparedata("MINUTE[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","minute");
        writerdata_array(100,5,"t2","minute");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_second() throws IOException, InterruptedException {
        Preparedata("SECOND[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","second");
        writerdata_array(100,5,"t2","second");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_datetime() throws IOException, InterruptedException {
        Preparedata("DATETIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","datetime");
        writerdata_array(100,5,"t2","datetime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_timestamp() throws IOException, InterruptedException {
        Preparedata("TIMESTAMP[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","timestamp1");
        writerdata_array(100,5,"t2","timestamp1");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_nanotime() throws IOException, InterruptedException {
        Preparedata("NANOTIME[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","nanotime");
        writerdata_array(100,5,"t2","nanotime");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_nanotimestamp() throws IOException, InterruptedException {
        Preparedata("NANOTIMESTAMP[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","nanotimestamp");
        writerdata_array(100,5,"t2","nanotimestamp");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_uuid() throws IOException, InterruptedException {
        Preparedata("UUID[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","uuid");
        writerdata_array(100,5,"t2","uuid");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_datehour() throws IOException, InterruptedException {
        Preparedata("DATEHOUR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","datehour");
        writerdata_array(100,5,"t2","datehour");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_ipaddr() throws IOException, InterruptedException {
        Preparedata("IPADDR[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","ipaddr");
        writerdata_array(100,5,"t2","ipaddr");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_int128() throws IOException, InterruptedException {
        Preparedata("INT128[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","int128");
        writerdata_array(100,5,"t2","int128");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_complex() throws IOException, InterruptedException {
        Preparedata("COMPLEX[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","complex");
        writerdata_array(100,5,"t2","complex");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_point() throws IOException, InterruptedException {
        Preparedata("POINT[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","point");
        writerdata_array(100,5,"t2","point");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_decimal32() throws IOException, InterruptedException {
        Preparedata("DECIMAL32(2)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        Assert.assertEquals(0, bt.rows());
        writerdata_array(100,5,"t1","decimal32");
        writerdata_array(100,5,"t2","decimal32");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_decimal64() throws IOException, InterruptedException {
        Preparedata("DECIMAL64(7)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","decimal64");
        writerdata_array(100,5,"t2","decimal64");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_array_decimal128() throws IOException, InterruptedException {
        Preparedata("DECIMAL128(19)[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 INNER JOIN  t2 ON t1.id=t2.id  order by id, time\n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","decimal128");
        writerdata_array(100,5,"t2","decimal128");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.value=tmptt.value where id in(`A+string(1..5));\n" +
                "update t2 set  t1.value=tmptt.value where id in(`A+string(2..10));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_allDateType_array() throws IOException, InterruptedException {
        Preparedata_array_keyTable(500,5);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, tv, t1.cbool, t1.cchar, t1.cshort, t1.cint, t1.clong, t1.cdouble, t1.cfloat, t1.cdate, t1.cmonth, t1.ctime, t1.cminute, t1.csecond, t1.cdatetime, t1.ctimestamp, t1.cnanotime, t1.cnanotimestamp, t1.cdatehour, t1.cuuid, t1.cipaddr, t1.cint128, t1.cpoint, t1.ccomplex, t1.cdecimal32, t1.cdecimal64, t1.cdecimal128, rowSum(t1.cint) as value FROM t1 INNER JOIN t2 ON t1.tv = t2.tv order by id, tv";

        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(1..25));\n" +
                "update t2 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(20..30));");
        Thread.sleep(1000);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
//        System.out.println(bt.getString());
//        System.out.println(ex1.getString());
        checkData(ex1, bt);
//        update
        conn.run("insert into t1 values (`A601,2025.08.26T12:36:23.639,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "tmptt =(exec * FROM t1  where id = `A601 limit 1)[0];\n" +
                "update t1 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(30..35));\n" +
                "update t2 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(40..45));");
        Thread.sleep(2000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        System.out.println(ex2.getString());
        checkData(ex2, bt);
//        update
        conn.run("tmptt =(exec * FROM t1 where id = `A100 limit 1)[0];\n" +
                "update t1 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(55..60));\n" +
                "update t2 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 where id in(`A+string(60..75));");
        Thread.sleep(1000);
        BasicTable ex3 = (BasicTable)conn.run(sqlStr1);
        checkData(ex3, bt);
        System.out.println(bt.getString());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    //@Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_update_allDateType_array_big_data() throws IOException, InterruptedException {
        Preparedata_array_keyTable(500,5);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, tv, t1.cbool, t1.cchar, t1.cshort, t1.cint, t1.clong, t1.cdouble, t1.cfloat, t1.cdate, t1.cmonth, t1.ctime, t1.cminute, t1.csecond, t1.cdatetime, t1.ctimestamp, t1.cnanotime, t1.cnanotimestamp, t1.cdatehour, t1.cuuid, t1.cipaddr, t1.cint128, t1.cpoint, t1.ccomplex, t1.cdecimal32, t1.cdecimal64, t1.cdecimal128, rowSum(t1.cint) as value FROM t1 INNER JOIN t2 ON t1.tv = t2.tv order by id, tv";

        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //update
        conn.run("tmptt =(exec * FROM t1 limit 1)[0];\n" +
                "update t1 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 ;\n" +
                "update t2 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 ;");
        Thread.sleep(20000);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println("------预期-----:"+ex1.rows());
        checkData(ex1, bt);
//        update
        conn.run("insert into t1 values (`A11601,2025.08.26T12:36:23.639,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "tmptt =(exec * FROM t1  where id = `A11601 limit 1)[0];\n" +
                "update t1 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 ;\n" +
                "update t2 set  t1.cbool=tmptt.cbool, cchar=tmptt.cchar, cshort=tmptt.cshort, cint=tmptt.cint, clong=tmptt.clong, cdouble=tmptt.cdouble, cfloat=tmptt.cfloat, cdate=tmptt.cdate, cmonth=tmptt.cmonth, ctime=tmptt.ctime, cminute=tmptt.cminute, csecond=tmptt.csecond, cdatetime=tmptt.cdatetime, ctimestamp=tmptt.ctimestamp, cnanotime=tmptt.cnanotime, cnanotimestamp=tmptt.cnanotimestamp, cdatehour=tmptt.cdatehour, cuuid=tmptt.cuuid, cipaddr=tmptt.cipaddr, cint128=tmptt.cint128, cpoint=tmptt.cpoint, ccomplex=tmptt.ccomplex, cdecimal32=tmptt.cdecimal32, cdecimal64=tmptt.cdecimal64, cdecimal128=tmptt.cdecimal128 ;");
        Thread.sleep(20000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(bt.getString());
        checkData(ex2, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }


    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_delete() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        //delete
        conn.run("delete from t1 where value<0");
        conn.run("delete from t2 where value<10");
        Thread.sleep(500);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex1, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_delete_allDateType() throws IOException, InterruptedException {
        Preparedata_keyTable(100);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.boolv, t1.charv, t1.shortv, t1.intv, t1.longv, t1.doublev, t1.floatv, t1.datev, t1.monthv, t1.timev, t1.minutev, t1.secondv, t1.datetimev, t1.timestampv, t1.nanotimev, t1.nanotimestampv, t1.stringv, t1.datehourv, t1.uuidv, t1.ippaddrv, t1.int128v, t1.blobv, t1.pointv, t1.complexv, t1.decimal32v, t1.decimal64v, t1.decimal128v, t1.intv+t2.intv as value  FROM t1 INNER JOIN t2 ON t1.tv = t2.tv order by id, boolv,charv,shortv,intv,longv";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //delete
        conn.run("delete from t1 where id in(`A+string(1..11))");
        conn.run("delete from t2 where id in(`A+string(12..20))");
        Thread.sleep(500);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex1, bt);
        //delete
        conn.run("delete from t1 where id in(`A+string(22..30))");
        conn.run("delete from t2 where id in(`A+string(22..35))");
        Thread.sleep(500);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex2, bt);

        conn.run("delete from t1 where id in(`A+string(30..50))");
        Thread.sleep(500);
        BasicTable ex3 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex3, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_delete_array() throws IOException, InterruptedException {
        Preparedata("BOOL[]");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id \n" ;
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1);
        writerdata_array(100,5,"t1","bool");
        writerdata_array(100,5,"t2","bool");
        Thread.sleep(1000);
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //delete
        conn.run("delete from t1 where id in(`A+string(1..5))");
        conn.run("delete from t2 where id in(`A+string(3..10))");
        Thread.sleep(1000);
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("bt_tmp", bt);
        conn.upload(map);
        Entity ex1 = conn.run("res1 = select * from bt_tmp order by id;\n" +
                "ex1 = SELECT id, time, t1.value, rowSum(value) FROM t1 FULL JOIN t2 ON t1.id=t2.id order by id; \n" +
                "assert 1, each(eqObj, res1.values(), ex1.values())");
        System.out.println(ex1.getString());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_delete_allDateType_array() throws IOException, InterruptedException {
        Preparedata_array_keyTable(500,5);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.cbool, t1.cchar, t1.cshort, t1.cint, t1.clong, t1.cdouble, t1.cfloat, t1.cdate, t1.cmonth, t1.ctime, t1.cminute, t1.csecond, t1.cdatetime, t1.ctimestamp, t1.cnanotime, t1.cnanotimestamp, t1.cdatehour, t1.cuuid, t1.cipaddr, t1.cint128, t1.cpoint, t1.ccomplex, t1.cdecimal32, t1.cdecimal64, t1.cdecimal128, rowSum(t1.cint) as value FROM t1 INNER JOIN t2 ON t1.tv = t2.tv order by id, tv";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        checkData(ex, bt);
        //delete
        conn.run("delete from t1 where id in(`A+string(1..11))");
        conn.run("delete from t2 where id in(`A+string(12..20))");
        Thread.sleep(1000);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex1, bt);
        //delete
        conn.run("delete from t1 where id in(`A+string(22..30))");
        conn.run("delete from t2 where id in(`A+string(22..35))");
        Thread.sleep(1000);
        BasicTable ex2 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex2, bt);

        conn.run("delete from t1 where id in(`A+string(30..50))");
        Thread.sleep(1000);
        BasicTable ex3 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex3, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
    @Test
    public void test_StreamingSQLClient_subscribeStreamingSQL_append() throws IOException, InterruptedException {
        Preparedata("DOUBLE");
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("t1");
        streamingSQLClient.declareStreamingSQLTable("t2");
        String sqlStr1 = "SELECT id, t1.value+t2.value as value FROM t1 INNER JOIN t2 ON t1.time = t2.time order by id, value";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        //append
        writer_data(100,"t1","double");
        writer_data(100,"t2","double");
        Thread.sleep(500);
        System.out.println(bt.getString());
        BasicTable ex = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex, bt);
        //append
        writer_data(1000,"t1","double");
        writer_data(1000,"t2","double");
        Thread.sleep(500);
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println(ex.rows());
        checkData(ex1, bt);
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }

    @Test//客户真实sql
    public void test_StreamingSQLClient_subscribeStreamingSQL_real() throws IOException, InterruptedException {
        String script1 = "\n" +
                "login(`admin, `123456)\n" +
                "def clearAll(){\n" +
                "    login(`admin, `123456)\n" +
                "    lastUser = `admin\n" +
                "    res = getStreamingSQLStatus()\n" +
                "    for(sqlStream in res){\n" +
                "        if(sqlStream.user == 'guest') \n" +
                "            try{logout(lastUser)}catch(ex){}\n" +
                "        else{\n" +
                "            login(sqlStream.user, `123456)\n" +
                "            lastUser = sqlStream.user\n" +
                "        }\n" +
                "        try{unsubscribeStreamingSQL(, sqlStream.queryId)}catch(ex){}\n" +
                "        revokeStreamingSQL(sqlStream.queryId)\n" +
                "    }\n" +
                "    assert \"clearAll 1\", size(getStreamingSQLStatus()) == 0\n" +
                "\n" +
                "    res = listStreamingSQLTables()\n" +
                "    for(table in res){\n" +
                "        if(table.users=='guest') try{logout(lastUser)}catch(ex){}\n" +
                "        else{\n" +
                "            login(table.users, `123456)\n" +
                "            lastUser = table.users\n" +
                "        }\n" +
                "        try{\n" +
                "            revokeStreamingSQLTable(table.tableName)\n" +
                "            undef(table.tableName,SHARED)\n" +
                "        }catch(ex){}\n" +
                "    }\n" +
                "    assert \"clearAll 2\", size(listStreamingSQLTables()) == 0\n" +
                "}\n" +
                "def writeDataMultiCols(mutable tb, colNames, colTypes, n=100, writeTimes=500, keyNum=10000){\n" +
                "    runs = 0\n" +
                "\tdo{\n" +
                "        res = table(n:n, colNames, colTypes)\n" +
                "        for(i in 0..(size(colNames)-1)){\n" +
                "            t = colTypes[i]\n" +
                "            name = colNames[i]\n" +
                "            date_time = datetime(now()) + 1..n\n" +
                "            if(t==INT){\n" +
                "                res[name] = rand(rand(1000,10) join NULL, n)\n" +
                "            }else if(t==SHORT){\n" +
                "                res[name] = short(rand(-100..1000,n))\n" +
                "            }else if(t==LONG){\n" +
                "                res[name] = long(rand(-1000..100000,n))\n" +
                "            }else if(t==FLOAT){\n" +
                "                res[name] = float(rand(10000.0,n))\n" +
                "            }else if(t==DOUBLE){\n" +
                "                res[name] = rand(rand(200.0,10) join NULL, n)\n" +
                "            }else if(t==STRING){\n" +
                "                res[name] = rand(take(`ID+string(1..keyNum), keyNum), n)\n" +
                "            }else if(t==SYMBOL){\n" +
                "                res[name] = rand(take(`ID+string(1..keyNum), keyNum), n)\n" +
                "            }else if(t==TIMESTAMP){\n" +
                "                res[name] = take(now(), n)\n" +
                "            }\n" +
                "        }\n" +
                "        tb.append!(res)\n" +
                "        sleep(1000)\n" +
                "        runs += 1\n" +
                "        print(runs)\n" +
                "\t}while(runs < writeTimes)\n" +
                "}\n" +
                "\n" +
                "def waitsubworker(){\n" +
                "    print(\"wait until stream log finish\")\n" +
                "    do{\n" +
                "        sleep(500)\n" +
                "        ct = exec count(*) as ct from getStreamingStat().subWorkers where queueDepth>0\n" +
                "        if(ct==0){\n" +
                "            break;\n" +
                "        }\n" +
                "    }while(true)\n" +
                "    print(\"stream log finished!\")\n" +
                "}\n" +
                "\n" +
                "clearAll()\n" +
                "sleep(1000)\n" +
                "go;" +
                "\n" +
                "// 最优报价表\n" +
                "bestBondQuotation_colnames = `SecurityID`DisplayListedMarket`BondName`ContributorID`ContributorName`BidVolume`BidYield`AskVolume`AskYield`MultiBidVolume`MultiAskVolume`BidPriceDesc`AskPriceDesc`MarketDataTime`SecurityCode`IC`TIMESTAMP\n" +
                "bestBondQuotation_coltypes = [SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,TIMESTAMP]\n" +
                "\n" +
                "// 债券属性表\n" +
                "bondFilter_colnames = `SEC_INNER_CODE`BOND_CODE`ISSUER_ID`PROVINCE_NAME`CMBOND_FLAG`SUBDT_DEBT_FLAG`PERPETUAL_FLAG`ISSUER_TYPE`CUSTODIAN_SITE`ISSUER_RATING`BOND_RATING`RESIDUAL_MATURITY`EXER_RESIDUAL_MATURITY`INT_RATE_TYPE`INDUS_PH_TYPE`DISCOUNT_FLAG`SUBDT_PERPETUAL_FLAG`LOWEST_ISSUER_RATING_1Y`BOND_TYPE`BOND_LV1_TYPE_NAME`BOND_LV2_TYPE_NAME`STATE_SHARE_FLAG`BANK_BUSS_TYPE`BOND_MATURITY_DATE`IN_LIB_FLAG`CSI_YTM`CSI_EXER_YIELD`CCDC_YTM`CCDC_EXER_YIELD`CCDC_RCMD_YIELD`CSI_RCMD_YIELD`RESD_MATURITY_DESC`RESD_MATURITY_D`BOND_EXTER_RATING`ISSUER_EXTER_RATING`CBRC_IMPLICIT_RATING`CCDC_EXER_DURATION`BOND_NAME`GOV_BOND_FLAG`CDB_BOND_FLAG`ADBC_BOND_FLAG`EXIMB_BOND_FLAG`LOCAL_GOV_BOND_FLAG`GEN_BOND_FLAG`SPCL_BOND_FLAG`RATE_BOND_FLAG`CREDIT_BOND_FLAG`ST_FINC_BILL_FLAG`SST_FINC_BILL_FLAG`MTN_FLAG`ENT_BOND_FLAG`CMCL_BOND_FLAG`CMCL_SUBDT_BOND_FLAG`CMCL_MIXED_BOND_FLAG`SEC_COMPANY_BOND_FLAG`SEC_ST_FINC_BILL_FLAG`SEC_SUBDT_BOND_FLAG`POLICY_SUBDT_BOND_FLAG`NBFI_BOND_FLAG`SPCL_FINC_BOND_FLAG`HUIJIN_BOND_FLAG`LARGE_PUB_BOND_FLAG`SMALL_PUB_BOND_FLAG`PVT_BOND_FLAG`PPN_FLAG`ABS_FLAG`RAILWAY_BOND_FLAG`OTHER_BOND_FLAG`IBNCD_FLAG`SOSB_IBNCD_FLAG`SOLCB_IBNCD_FLAG`AAA_ISSUER_IBNCD_FLAG`AA_PLUS_ISSUER_IBNCD_FLAG`AA_ISSUER_IBNCD_FLAG`OTH_FINC_INST_CRE_BOND_FLAG`TERM_DESC`ISSUER_INNER_RATING`BOND_INNER_RATING`CLOSE_PRICE`PUB_BOND_AVAIL_INVEST_QUOTA`PENS_BOND_AVAIL_INVEST_QUOTA`SB_BOND_AVAIL_INVEST_QUOTA`AVAIL_INVEST_QUOTA_TOTAL`T_MINUS1_CCDC_YTM`T_MINUS1_CCDC_EXER_YIELD`T_MINUS1_CSI_YTM`T_MINUS1_CSI_EXER_YIELD`LEAVE_DAYS`TIMESTAMP\n" +
                "bondFilter_coltypes = [SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,DATE,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,DOUBLE,SYMBOL,SYMBOL,SYMBOL,DOUBLE,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,INT,TIMESTAMP]\n" +
                "\n" +
                "// 最优报价表\n" +
                "share keyedTable(`SecurityCode,10000:0,bestBondQuotation_colnames,bestBondQuotation_coltypes) as bestBondQuotation\n" +
                "// 债券属性表\n" +
                "share keyedTable(`bond_Code,10000:0,bondFilter_colnames,bondFilter_coltypes) as bondFilter\n" +
                "go;" +
                "// init data\n" +
                "jid1 = submitJob(\"write1\", \"\", writeDataMultiCols, bestBondQuotation, bestBondQuotation_colnames, bestBondQuotation_coltypes, 50000, 10)\n" +
                "jid2 = submitJob(\"write2\", \"\", writeDataMultiCols, bondFilter, bondFilter_colnames, bondFilter_coltypes, 50000, 10)" +
                "go\n";

        conn.run(script1);
        StreamingSQLClient streamingSQLClient = new StreamingSQLClient(HOST, PORT, "admin","123456");
        streamingSQLClient.declareStreamingSQLTable("bestBondQuotation");
        streamingSQLClient.declareStreamingSQLTable("bondFilter");
        String sqlStr1 = "select t1.*,\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.csi_ytm) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCsiYtm',\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.csi_exer_yield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCsiExerYield',\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.csi_rcmd_yield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCsiRcmdYield',\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.ccdc_ytm) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCcdcYtm',\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.ccdc_exer_yield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCcdcExerYield',\n" +
                "    case\n" +
                "        when t1.BidYield != null then round((t1.BidYield - t2.ccdc_rcmd_yield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'bidSubCcdcRcmdYield',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.csi_ytm - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'csiYtmSubOfr',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.csi_exer_yield - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'csiExerYieldSubOfr',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.csi_rcmd_yield - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'csiRcmdYieldSubOfr',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.ccdc_ytm - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'ccdcYtmSubOfr',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.ccdc_exer_yield - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'ccdcExerYieldSubOfr',\n" +
                "    case\n" +
                "        when t1.AskYield != null then round((t2.ccdc_rcmd_yield - t1.AskYield) * 100, 2)\n" +
                "        else null\n" +
                "    end as 'ccdcRcmdYieldSubOfr',\n" +
                "    t2.csi_ytm as csiYtm,\n" +
                "    t2.csi_exer_yield as csiExerYield,\n" +
                "    t2.csi_rcmd_yield as csiRcmdYield,\n" +
                "    t2.ccdc_ytm as ccdcYtm,\n" +
                "    t2.ccdc_exer_yield as ccdcExerYield,\n" +
                "    t2.ccdc_rcmd_yield as ccdcRcmdYield,\n" +
                "    t2.resd_maturity_desc as resdMaturityDesc\n" +
                "    from\n" +
                "        bestBondQuotation t1\n" +
                "        left join bondFilter t2 on t2.bond_Code = t1.SecurityCode\n" +
                "    where\n" +
                "        t2.RESD_MATURITY_D > 90\n" +
                "    order by\n" +
                "         t1.SecurityID, t1.DisplayListedMarket, t1.ContributorName, t1.BidYield desc nulls last;";
        String id1 = streamingSQLClient.registerStreamingSQL(sqlStr1);
        System.out.println("id1:"+id1);
        BasicTable bt = streamingSQLClient.subscribeStreamingSQL(id1,1,1);
        conn.run("getJobReturn(jid1, true)\n" +
                "getJobReturn(jid2, true)\n" +
                "waitsubworker();\n" +
                "go;");
        int count = 0;
        int count1= 0;
        int count2= 1;
        while (!(count1==count2)&&count<=100)
        {
            count=count++;
            count1 = bt.rows();
            System.out.println("count1:"+bt.rows());
            Thread.sleep(1000);
            count2 = bt.rows();
            System.out.println("count2:"+bt.rows());
        }
        BasicTable ex1 = (BasicTable)conn.run(sqlStr1);
        System.out.println("------预期-----:"+ex1.rows());
        checkData(ex1, bt);
        System.out.println(bt.rows());
        streamingSQLClient.unsubscribeStreamingSQL(id1);
    }
}
