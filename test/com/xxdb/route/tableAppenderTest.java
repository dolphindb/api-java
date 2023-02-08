package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class tableAppenderTest {

    private static String dburl="dfs://tableAppenderTest";
    private static String tableName="testAppend";
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://tableAppenderTest\")){\n" +"\tdropDatabase(\"dfs://tableAppenderTest\")\n" +                "}");
        conn.close();
    }
    @Test
    public void TestAppend() throws IOException {
        int size=100000;
        int[] id=new int[size];
        double[] data=new double[size];
        BasicTimestampVector timeVector=new BasicTimestampVector(size);
        Random rand=new Random();
        for(int i=0;i<size;++i){
            LocalDateTime dt=LocalDateTime.now();
            timeVector.setTimestamp(i,dt);
            id[i]= rand.nextInt();
            data[i]=rand.nextDouble();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        BasicDoubleVector dataVector=new BasicDoubleVector(data);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(timeVector);
        cols.add(dataVector);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("time");
        colName.add("data");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(100:0,`id`time`data,[INT,TIME,DOUBLE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`testAppend,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","testAppend",conn);
        appender.append(insert);
        Entity assertRet= conn.run("exec count(*) from loadTable(\"dfs://tableAppenderTest\", \"testAppend\")");
        String ret=assertRet.getString();

        Assert.assertEquals(String.valueOf(size), assertRet.getString());

    }
    @Test
    public void TestDateTimeToDATE() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("2021.05.01 00:00:10 + 10..19");
//        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 00:00:10.005 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
//        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DTtoDATE");
//        colName.add("TStoDATE");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(10:0,`id`DTtoDATE,[INT,DATE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateTimeToDATE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateTimeToDATE",conn);
        appender.append(insert);
        BasicTable Btable=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Btable.rows();++ind) {
            BasicDateVector DATEvector = (BasicDateVector) Btable.getColumn("DTtoDATE");
            LocalDate expDATE = LocalDate.of(2021,5,1);
            Assert.assertEquals(expDATE,DATEvector.getDate(ind));
        }
    }
    @Test
    public void TestTimeStampToDATE() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 00:00:10.005 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStoDATE");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(10:0,`id`TimeStoDATE,[INT,DATE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToDATE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToDATE",conn);
        appender.append(insert);
        BasicTable Btable=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Btable.rows();++ind) {
            BasicDateVector DATEvector = (BasicDateVector) Btable.getColumn("TimeStoDATE");
            LocalDate expDATE = LocalDate.of(2021,5,1);
            Assert.assertEquals(expDATE,DATEvector.getDate(ind));
        }
    }
    @Test
    public void TestNanoTimeStampToDATE() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 00:00:10.005007009 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStoDATE");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(100:0,`id`NanoTimeStoDATE,[INT,DATE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToDATE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDATE",conn);
        appender.append(insert);
        BasicTable Btable=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Btable.rows();++ind) {
            BasicDateVector DATEvector = (BasicDateVector) Btable.getColumn("NanoTimeStoDATE");
            LocalDate expDATE = LocalDate.of(2021,5,1);
            Assert.assertEquals(expDATE,DATEvector.getDate(ind));
        }
    }
    @Test
    public void TestDateHourToDATE() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicDateHourVector DATEHOUR = (BasicDateHourVector) conn.run("datehour('2021.05.01T13')+1..10");
//        System.out.println(DATEHOUR.getNanoTimestamp(5));
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATEHOUR);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DATEHOURtoDATE");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(100:0,`id`DATEHOURtoDATE,[INT,DATE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateHourToDATE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateHourToDATE",conn);
        appender.append(insert);
        BasicTable Btable=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Btable.rows();++ind) {
            BasicDateVector DATEvector = (BasicDateVector) Btable.getColumn("DATEHOURtoDATE");
            LocalDate expDATE = LocalDate.of(2021,5,1);
            Assert.assertEquals(expDATE,DATEvector.getDate(ind));
        }
    }
    @Test
    public void TestDateToMON() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicDateVector DATE = (BasicDateVector) conn.run("2021.05.01 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATE);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DATEtoMON");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`DATEtoMON,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateToMON,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateToMON",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Monvector = (BasicMonthVector) Tappender.getColumn("DATEtoMON");
            YearMonth expMON = YearMonth.of(2021,5);
            Assert.assertEquals(expMON,Monvector.getMonth(ind));
        }
    }
    @Test
    public void TestDateTimeToMON() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("2021.05.01 00:00:10 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DATETIMEtoMON");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`DATETIMEtoMON,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateTimeToMON,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateTimeToMON",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Monvector = (BasicMonthVector) Tappender.getColumn("DATETIMEtoMON");
            YearMonth expMON = YearMonth.of(2021,5);
            Assert.assertEquals(expMON,Monvector.getMonth(ind));
        }
    }
    @Test
    public void TestDateHourToMON() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicDateHourVector DATEHOUR = (BasicDateHourVector) conn.run("datehour('2021.05.01T13')+1..10");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATEHOUR);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DATEHOURtoMON");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`DATEHOURtoMON,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateHourToMON,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateHourToMON",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Monvector = (BasicMonthVector) Tappender.getColumn("DATEHOURtoMON");
            YearMonth expMON = YearMonth.of(2021,5);
            Assert.assertEquals(expMON,Monvector.getMonth(ind));
        }
    }
    @Test
    public void TestTimeStampToMON() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 00:00:10.005 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStoMon");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeStoMon,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToMON,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToMON",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Monvector = (BasicMonthVector) Tappender.getColumn("TimeStoMon");
            YearMonth expMON = YearMonth.of(2021,5);
            Assert.assertEquals(expMON,Monvector.getMonth(ind));
        }
    }
    @Test
    public void TestNanoTimeStampToMON() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 00:00:10.005007009 + 10..19");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStoMon");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStoMon,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToMON,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToMON",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Monvector = (BasicMonthVector) Tappender.getColumn("NanoTimeStoMon");
            YearMonth expMON = YearMonth.of(2021,5);
            Assert.assertEquals(expMON,Monvector.getMonth(ind));
        }
    }
    @Test
    public void TestNanoTimeToTIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        Random rand=new Random();
        BasicNanoTimeVector NANOTIME = (BasicNanoTimeVector) conn.run("13:30:10.008007006 +1..10");
        for(int i=0;i<size;++i){
            id[i]= rand.nextInt();
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimetoTime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimetoTime,[INT,TIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeToTIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeToTIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimeVector Tvector = (BasicTimeVector) Tappender.getColumn("NanoTimetoTime");
            LocalTime expTime = LocalTime.of(13,30,10,8000000);
            Assert.assertEquals(expTime,Tvector.getTime(ind));
        }
    }
    @Test
    public void TestTimeStampToTIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 13:30:10.008 +1..10");
        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStampToTIME");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeStampToTIME,[INT,TIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToTIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToTIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimeVector Tvector = (BasicTimeVector) Tappender.getColumn("TimeStampToTIME");
            LocalTime expTime = LocalTime.of(13,30,10,8000000+(ind+1)*1000000);
            Assert.assertEquals(expTime,Tvector.getTime(ind));
        }
    }
    @Test
    public void TestNanoTimeStampToTIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008007009 +1..10");
        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStampToTIME");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStampToTIME,[INT,TIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToTIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToTIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimeVector Tvector = (BasicTimeVector) Tappender.getColumn("NanoTimeStampToTIME");
            LocalTime expTime = LocalTime.of(13,30,10,8000000);
            Assert.assertEquals(expTime,Tvector.getTime(ind));
        }
    }
    @Test
    public void TestTimeToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimeVector TIME = (BasicTimeVector) conn.run("13:30:10.008 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("TimeToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestSecondToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicSecondVector SECOND = (BasicSecondVector) conn.run("13:30:10 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(SECOND);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("SecondToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`SecondToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestSecondToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestSecondToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("SecondToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestDateTimeToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("2021.05.01 13:30:10 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DateTimeToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`DateTimeToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateTimeToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateTimeToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("DateTimeToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestNanoTimeToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimeVector NANOTIME = (BasicNanoTimeVector) conn.run("13:30:10.008005006 +1..10");
        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("NanoTimeToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestTimeStampToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 13:30:10.008 +1..10");
        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStampToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeStampToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("TimeStampToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestNanoTimeStampToMINUTE() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMES = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008005006 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMES);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeSToMinute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeSToMinute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToMINUTE,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToMINUTE",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("NanoTimeSToMinute");
            LocalTime expTime = LocalTime.of(13,30);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }
    @Test
    public void TestTimeToSECOND() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimeVector TIME = (BasicTimeVector) conn.run("13:30:10.008 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeToSecond");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeToSecond,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeToSECOND,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeToSECOND",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("TimeToSecond");
            LocalTime expTime = LocalTime.of(13,30,10);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }
    @Test
    public void TestDateTimeToSECOND() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("2021.05.01 13:30:10 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("DateTimeToSecond");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`DateTimeToSecond,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateTimeToSECOND,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateTimeToSECOND",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("DateTimeToSecond");
            LocalTime expTime = LocalTime.of(13,30,11+ind);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }
    @Test
    public void TestNanoTimeToSECOND() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimeVector NANOTIME = (BasicNanoTimeVector) conn.run("13:30:10.008003004 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeToSecond");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeToSecond,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeToSECOND,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeToSECOND",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("NanoTimeToSecond");
            LocalTime expTime = LocalTime.of(13,30,10);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }
    @Test
    public void TestTimeStampToSECOND() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector Timestamp = (BasicTimestampVector) conn.run("2021.05.01 13:30:10.008 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(Timestamp);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStampToSecond");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeStampToSecond,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToSECOND,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToSECOND",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("TimeStampToSecond");
            LocalTime expTime = LocalTime.of(13,30,10);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }
    @Test
    public void TestNanoTimeStampToSECOND() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008005006 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStampToSecond");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStampToSecond,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToSECOND,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToSECOND",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("NanoTimeStampToSecond");
            LocalTime expTime = LocalTime.of(13,30,10);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }
    @Test
    public void TestTimeStampToDATETIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("2021.05.01 13:30:10.008 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("TimeStampToDateTime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`TimeStampToDateTime,[INT,DATETIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToDATETIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToDATETIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateTimeVector vector = (BasicDateTimeVector) Tappender.getColumn("TimeStampToDateTime");
            LocalDateTime expTime = LocalDateTime.of(2021,05,01,13,30,10);
            Assert.assertEquals(expTime,vector.getDateTime(ind));

        }
    }
    @Test
    public void TestNanoTimeStampToDATETIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008005006 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStampToDateTime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStampToDateTime,[INT,DATETIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToDATETIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDATETIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateTimeVector vector = (BasicDateTimeVector) Tappender.getColumn("NanoTimeStampToDateTime");
            LocalDateTime expTime = LocalDateTime.of(2021,05,01,13,30,10);
            Assert.assertEquals(expTime,vector.getDateTime(ind));

        }
    }
    @Test
    public void TestNanoTimeStampToTIMESTAMP() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008005006 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStampToTimeStamp");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStampToTimeStamp,[INT,TIMESTAMP])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToTIMESTAMP,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToTIMESTAMP",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimestampVector vector = (BasicTimestampVector) Tappender.getColumn("NanoTimeStampToTimeStamp");
            LocalDateTime expTime = LocalDateTime.of(2021,05,01,13,30,10,8*1000000);
            Assert.assertEquals(expTime,vector.getTimestamp(ind));

        }
    }
    @Test
    public void TestNanoTimeStampToNANOTIME() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2021.05.01 13:30:10.008005006 +1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("NanoTimeStampToNanoTime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`NanoTimeStampToNanoTime,[INT,NANOTIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToNANOTIME,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToNANOTIME",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicNanoTimeVector vector = (BasicNanoTimeVector) Tappender.getColumn("NanoTimeStampToNanoTime");
            LocalTime expTime = LocalTime.of(13,30,10,8005007 +ind);
            Assert.assertEquals(expTime,vector.getNanoTime(ind));

        }
    }

    @Test
    public void testBasicDecimal() throws Exception {
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,4);
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(3,2);
        bd32v.set(0,new BasicDecimal32(31,4));
        bd32v.set(1,new BasicDecimal32(22,4));
        bd32v.set(2,new BasicDecimal32(17,4));
        bd64v.set(0,new BasicDecimal64(45,2));
        bd64v.set(1,new BasicDecimal64(9,2));
        bd64v.set(2,new BasicDecimal64(11,2));
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("id");
        colNames.add("a");
        colNames.add("b");
        BasicIntVector biv = new BasicIntVector(new int[]{1,2,3});
        cols.add(biv);
        cols.add(bd32v);
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`pt,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","pt",conn);
        appender.append(bt);
        BasicTable aa = (BasicTable) conn.run("select * from pt");
        Assert.assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,aa.getColumn("a").getDataType());
        Assert.assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,aa.getColumn("b").getDataType());
        BasicDecimal32 bd32 = (BasicDecimal32) aa.getColumn("a").get(1);
        Assert.assertEquals(2,bd32.getScale());
        BasicDecimal64 bd64 = (BasicDecimal64) aa.getColumn("b").get(1);
        Assert.assertEquals(4,bd64.getScale());
    }
    @Test
    public void test_tableAppender_ArrayVector_decimal() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        tableAppender appender=new tableAppender("dfs://testArrayVector","pt",conn);

        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal32Vector(3,0);
        v32.set(0,new BasicDecimal32(15645.00,0));
        v32.set(1,new BasicDecimal32(24635.00001,0));
        v32.set(2,new BasicDecimal32(24635.00001,0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal32Vector(3,4);
        v321.set(0,new BasicDecimal32(15645.00,4));
        v321.set(1,new BasicDecimal32(24635.00001,4));
        v321.set(2,new BasicDecimal32(24635.00001,4));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal64Vector(3,0);
        v640.set(0,new BasicDecimal64(15645.00,0));
        v640.set(1,new BasicDecimal64(24635.00001,0));
        v640.set(2,new BasicDecimal64(24635.00001,0));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v641=new BasicDecimal64Vector(3,4);
        v641.set(0,new BasicDecimal64(15645.00,4));
        v641.set(1,new BasicDecimal64(24635.00001,4));
        v641.set(2,new BasicDecimal64(24635.00001,4));
        bdvcol3.add(0,v641);
        bdvcol3.add(1,v641);
        bdvcol3.add(2,v641);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v642=new BasicDecimal64Vector(3,8);
        v642.set(0,new BasicDecimal64(15645.00,8));
        v642.set(1,new BasicDecimal64(24635.00001,8));
        v642.set(2,new BasicDecimal64(24635.00001,8));
        bdvcol4.add(0,v642);
        bdvcol4.add(1,v642);
        bdvcol4.add(2,v642);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void test_tableAppender_ArrayVector_decimal_compress_true() throws Exception {

        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect(HOST, PORT, "admin", "123456");
        conn.run(script);
        tableAppender appender=new tableAppender("dfs://testArrayVector","pt",conn);

        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        colNames.add("col3");
        colNames.add("col4");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal32Vector(3,0);
        v32.set(0,new BasicDecimal32(15645.00,0));
        v32.set(1,new BasicDecimal32(24635.00001,0));
        v32.set(2,new BasicDecimal32(24635.00001,0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal32Vector(3,4);
        v321.set(0,new BasicDecimal32(15645.00,4));
        v321.set(1,new BasicDecimal32(24635.00001,4));
        v321.set(2,new BasicDecimal32(24635.00001,4));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal64Vector(3,0);
        v640.set(0,new BasicDecimal64(15645.00,0));
        v640.set(1,new BasicDecimal64(24635.00001,0));
        v640.set(2,new BasicDecimal64(24635.00001,0));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v641=new BasicDecimal64Vector(3,4);
        v641.set(0,new BasicDecimal64(15645.00,4));
        v641.set(1,new BasicDecimal64(24635.00001,4));
        v641.set(2,new BasicDecimal64(24635.00001,4));
        bdvcol3.add(0,v641);
        bdvcol3.add(1,v641);
        bdvcol3.add(2,v641);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v642=new BasicDecimal64Vector(3,8);
        v642.set(0,new BasicDecimal64(15645.00,8));
        v642.set(1,new BasicDecimal64(24635.00001,8));
        v642.set(2,new BasicDecimal64(24635.00001,8));
        bdvcol4.add(0,v642);
        bdvcol4.add(1,v642);
        bdvcol4.add(2,v642);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void testgetDTString() throws IOException{
        DBConnection connection = new DBConnection();
        connection.connect("192.168.1.116", 18999, "admin", "123456");
        tableAppender tableAppender = new tableAppender("dfs://twapinfo", "pt", connection, com.xxdb.route.tableAppender.APPEND_ACTION.fitColumnType);
        Entity.DATA_TYPE[] data_types = new Entity.DATA_TYPE[]{Entity.DATA_TYPE.DT_ANY, Entity.DATA_TYPE.DT_BLOB,
                Entity.DATA_TYPE.DT_BOOL, Entity.DATA_TYPE.DT_BYTE, Entity.DATA_TYPE.DT_CODE, Entity.DATA_TYPE.DT_COMPRESS,
                Entity.DATA_TYPE.DT_DATASOURCE, Entity.DATA_TYPE.DT_DATE, Entity.DATA_TYPE.DT_DATEHOUR, Entity.DATA_TYPE.DT_DATEMINUTE,
                Entity.DATA_TYPE.DT_DATETIME, Entity.DATA_TYPE.DT_DICTIONARY, Entity.DATA_TYPE.DT_DOUBLE, Entity.DATA_TYPE.DT_FLOAT,
        Entity.DATA_TYPE.DT_FUNCTIONDEF, Entity.DATA_TYPE.DT_HANDLE, Entity.DATA_TYPE.DT_INT, Entity.DATA_TYPE.DT_INT128,
        Entity.DATA_TYPE.DT_IPADDR, Entity.DATA_TYPE.DT_LONG, Entity.DATA_TYPE.DT_MINUTE, Entity.DATA_TYPE.DT_MONTH, Entity.DATA_TYPE.DT_NANOTIME,
        Entity.DATA_TYPE.DT_NANOTIMESTAMP, Entity.DATA_TYPE.DT_OBJECT, Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_RESOURCE, Entity.DATA_TYPE.DT_SECOND,
        Entity.DATA_TYPE.DT_SHORT, Entity.DATA_TYPE.DT_SYMBOL, Entity.DATA_TYPE.DT_TIME, Entity.DATA_TYPE.DT_TIMESTAMP, Entity.DATA_TYPE.DT_UUID,
                Entity.DATA_TYPE.DT_VOID, Entity.DATA_TYPE.DT_BOOL_ARRAY};

        String[] data_type_string = new String[]{"ANY", "BLOB", "BOOL", "BYTE", "CODE", "COMPRESSED", "DATASOURCE", "DATE", "DATEHOUR", "DATEMINUTE",
                "DATETIME", "DICTIONARY", "DOUBLE", "FLOAT", "FUNCTIONDEF", "HANDLE", "INT", "INT128", "IPADDR", "LONG", "MINUTE", "MONTH", "NANOTIME", "NANOTIMESTAMP",
                "OBJECT", "STRING", "RESOURCE", "SECOND", "SHORT", "SYMBOL", "TIME", "TIMESTAMP", "UUID", "VOID", "Unrecognized type"};

        for (int i = 0; i < data_types.length; i++){
            String s = tableAppender.getDTString(data_types[i]);
            assertEquals(data_type_string[i], s);
        }
    }

    @Test
    public void TestMon() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicMonthVector MONTH = (BasicMonthVector) conn.run("2021.05M + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(MONTH);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("Mon");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`Mon,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestMon,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestMon",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMonthVector Tvector = (BasicMonthVector) Tappender.getColumn("Mon");
            YearMonth expTime = MONTH.getMonth(ind);
            Assert.assertEquals(expTime,Tvector.getMonth(ind));
        }
    }


    @Test
    public void TestDate() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateVector DATE = (BasicDateVector) conn.run("2021.05.04 + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATE);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("date");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`date,[INT,DATE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDate,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDate",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateVector Tvector = (BasicDateVector) Tappender.getColumn("date");
            LocalDate expTime = DATE.getDate(ind);
            Assert.assertEquals(expTime,Tvector.getDate(ind));
        }
    }


    @Test
    public void TestTime() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimeVector TIME = (BasicTimeVector) conn.run("12:32:56.356 + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("time");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`time,[INT,TIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTime,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTime",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimeVector Tvector = (BasicTimeVector) Tappender.getColumn("time");
            LocalTime expTime = TIME.getTime(ind);
            Assert.assertEquals(expTime,Tvector.getTime(ind));
        }
    }

    @Test
    public void TestMinute() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicMinuteVector MINUTE = (BasicMinuteVector) conn.run("minute(2012.12.03 01:22:01) + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(MINUTE);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("minute");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`minute,[INT,MINUTE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestMinute,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestMinute",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicMinuteVector Tvector = (BasicMinuteVector) Tappender.getColumn("minute");
            LocalTime expTime = MINUTE.getMinute(ind);
            Assert.assertEquals(expTime,Tvector.getMinute(ind));
        }
    }


    @Test
    public void TestSecond() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicSecondVector SECOND = (BasicSecondVector) conn.run("09:00:01 + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(SECOND);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("second");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`second,[INT,SECOND])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestSecond,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestSecond",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicSecondVector Tvector = (BasicSecondVector) Tappender.getColumn("second");
            LocalTime expTime = SECOND.getSecond(ind);
            Assert.assertEquals(expTime,Tvector.getSecond(ind));
        }
    }


    @Test
    public void TestDatetime() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("datetime(now()) + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("datetime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`datetime,[INT,DATETIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDatetime,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDatetime",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateTimeVector Tvector = (BasicDateTimeVector) Tappender.getColumn("datetime");
            LocalDateTime expTime = DATETIME.getDateTime(ind);
            Assert.assertEquals(expTime,Tvector.getDateTime(ind));
        }
    }

    @Test
    public void TestTimeStamp() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector TIMESTAMP = (BasicTimestampVector) conn.run("timestamp(now()) + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(TIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("timestamp");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`timestamp,[INT,TIMESTAMP])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStamp,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStamp",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicTimestampVector Tvector = (BasicTimestampVector) Tappender.getColumn("timestamp");
            LocalDateTime expTime = TIMESTAMP.getTimestamp(ind);
            Assert.assertEquals(expTime,Tvector.getTimestamp(ind));
        }
    }

    @Test
    public void TestNanoTime() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimeVector NANOTIME = (BasicNanoTimeVector) conn.run("13:30:10.008007006 + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("nanotime");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`nanotime,[INT,NANOTIME])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanotime,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanotime",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicNanoTimeVector Tvector = (BasicNanoTimeVector) Tappender.getColumn("nanotime");
            LocalTime expTime = NANOTIME.getNanoTime(ind);
            Assert.assertEquals(expTime,Tvector.getNanoTime(ind));
        }
    }


    @Test
    public void TestNanoTimeStamp() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector NANOTIMESTAMP = (BasicNanoTimestampVector) conn.run("2012.12.03 13:30:10.008007006 + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(NANOTIMESTAMP);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("nanotimestamp");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`nanotimestamp,[INT,NANOTIMESTAMP])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanotimestamp,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanotimestamp",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicNanoTimestampVector Tvector = (BasicNanoTimestampVector) Tappender.getColumn("nanotimestamp");
            LocalDateTime expTime = NANOTIMESTAMP.getNanoTimestamp(ind);
            Assert.assertEquals(expTime,Tvector.getNanoTimestamp(ind));
        }
    }

    @Test
    public void TestDateHour() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateHourVector DATEHOUR = (BasicDateHourVector) conn.run("datehour(2012.06.13 13:30:10) + 1..10");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATEHOUR);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("datehour");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`datehour,[INT,DATEHOUR])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanotimestamp,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanotimestamp",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateHourVector Tvector = (BasicDateHourVector) Tappender.getColumn("datehour");
            LocalDateTime expTime = DATEHOUR.getDateHour(ind);
            Assert.assertEquals(expTime,Tvector.getDateHour(ind));
        }
    }

    @Test
    public void TestDateTimeToDateHour() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicDateTimeVector DATETIME = (BasicDateTimeVector) conn.run("datetime(2012.06.13 13:30:10) + 1..10");
        BasicDateHour result = (BasicDateHour) conn.run("datehour(2012.06.13 13:30:10)");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("datehour");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`datehour,[INT,DATEHOUR])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestDateTimeToDateHour,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestDateTimeToDateHour",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateHourVector vector = (BasicDateHourVector)  Tappender.getColumn("datehour");
            Assert.assertEquals(result.getDateHour(),vector.getDateHour(ind));
        }
    }

    @Test
    public void TestTimeStampToDateHour() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicTimestampVector DATETIME = (BasicTimestampVector) conn.run("timestamp(2012.06.13 13:30:10) + 1..10");
        BasicDateHour result = (BasicDateHour) conn.run("datehour(2012.06.13 13:30:10)");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("datehour");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`datehour,[INT,DATEHOUR])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestTimeStampToDateHour,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestTimeStampToDateHour",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateHourVector vector = (BasicDateHourVector)  Tappender.getColumn("datehour");
            Assert.assertEquals(result.getDateHour(),vector.getDateHour(ind));
        }
    }


    @Test
    public void TestNanotimeTimeStampToDateHour() throws IOException {
        int size=10;
        int[] id=new int[size];
        BasicNanoTimestampVector DATETIME = (BasicNanoTimestampVector) conn.run("nanotimestamp('2012.12.13 13:30:10.008007006') + 1..10");
        BasicDateHour result = (BasicDateHour) conn.run("datehour(2012.12.13 13:30:10)");

        for(int i=0;i<size;++i){
            id[i]= i;
        }
        BasicIntVector idVector=new BasicIntVector(id);
        ArrayList<Vector> cols=new ArrayList<>();
        cols.add(idVector);
        cols.add(DATETIME);
        ArrayList<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("datehour");
        BasicTable insert=new BasicTable(colName, cols);
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`datehour,[INT,DATEHOUR])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestNanoTimeStampToDateHour,`id)");
        tableAppender appender=new tableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDateHour",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateHourVector vector = (BasicDateHourVector)  Tappender.getColumn("datehour");
            Assert.assertEquals(result.getDateHour(),vector.getDateHour(ind));
        }
    }
}