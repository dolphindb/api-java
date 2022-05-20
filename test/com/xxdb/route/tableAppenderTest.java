package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.DBConnectionPool;
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

public class tableAppenderTest {

    private static String dburl="dfs://tableAppenderTest";
    private static String tableName="testAppend";
    private static DBConnection conn;
    private static String HOST="localhost";
    private static int PORT = 8848;
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

}