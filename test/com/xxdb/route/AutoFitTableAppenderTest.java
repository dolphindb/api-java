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

public class AutoFitTableAppenderTest {

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
    public static void PrepareTable(String dataType) throws IOException {
        String script = "try{\nundef(`pt, SHARED)\n}\n catch(ex){\n}\n;share table(1:0, `permno`dateType, [INT,"+dataType+"]) as pt;\n";
        conn.run(script);
    }
    public void compareBasicTable(BasicTable table, BasicTable newTable) {
        Assert.assertEquals(table.rows(), newTable.rows());
        Assert.assertEquals(table.columns(), newTable.columns());
        int cols = table.columns();
        for (int i = 0; i < cols; i++) {
            AbstractVector v1 = (AbstractVector) table.getColumn(i);
            AbstractVector v2 = (AbstractVector) newTable.getColumn(i);
            if (!v1.equals(v2)) {
                for (int j = 0; j < table.rows(); j++) {
                    int failCase = 0;
                    AbstractScalar e1 = (AbstractScalar) table.getColumn(i).get(j);
                    AbstractScalar e2 = (AbstractScalar) newTable.getColumn(i).get(j);
                    if (!e1.toString().equals(e2.toString())) {
                        System.out.println("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                        failCase++;
                    }
                    Assert.assertEquals(0, failCase);
                }

            }
        }
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","testAppend",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateTimeToDATE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToDATE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDATE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateHourToDATE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateToMON",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateTimeToMON",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateHourToMON",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToMON",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToMON",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeToTIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToTIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToTIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestSecondToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateTimeToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToMINUTE",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeToSECOND",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateTimeToSECOND",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeToSECOND",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToSECOND",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToSECOND",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToDATETIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDATETIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToTIMESTAMP",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToNANOTIME",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","pt",conn);
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
    public void test_BasicDecimal128_AutoFitTableAppender_indexedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t=indexedTable(`sym,1:0,`sym`datetime`col1`col2,[STRING,DATETIME,DECIMAL128(0),DECIMAL128(37)])";
        conn.run(script);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("sym");
        colNames.add("datetime");
        colNames.add("col1");
        colNames.add("col2");
        BasicStringVector bsv = new BasicStringVector(new String[]{"Huya","Tonghuashun","maoyan"});
        cols.add(bsv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{17,989,9000});
        cols.add(bdtv);
        BasicDecimal128Vector bd32v = new BasicDecimal128Vector(3,0);
        bd32v.set(0,new BasicDecimal128("-11.011",0));
        bd32v.set(1,new BasicDecimal128("19.99",0));
        bd32v.set(2,new BasicDecimal128("23.0000000000000000000001",0));
        cols.add(bd32v);
        System.out.println(bd32v.getString());
        BasicDecimal128Vector bd64v = new BasicDecimal128Vector(3,37);
        bd64v.set(0,new BasicDecimal128("-2.0009",37));
        bd64v.set(1,new BasicDecimal128("4.99999999999999999999999999999999",37));
        bd64v.set(2,new BasicDecimal128("0.00000000000000000000000000000001",37));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        AutoFitTableAppender aftu = new AutoFitTableAppender("","t",conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        System.out.println(ua.getString());
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("col1").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("col2").get(i).getString());
        }
        conn.run("clear!(t)");
    }
    @Test
    public void test_AutoFitTableAppender_ArrayVector_decimal() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://testArrayVector","pt",conn);

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
    public void test_AutoFitTableAppender_ArrayVector_decimal128() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(10)[],DECIMAL128(37)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn.run(script);
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://testArrayVector","pt",conn);

        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal128Vector(3,0);
        v32.set(0,new BasicDecimal128("15645.00",0));
        v32.set(1,new BasicDecimal128("24635.00001",0));
        v32.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal128Vector(3,10);
        v321.set(0,new BasicDecimal128("15645.00",10));
        v321.set(1,new BasicDecimal128("24635.00001",10));
        v321.set(2,new BasicDecimal128("24635.00001",10));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal128Vector(3,37);
        v640.set(0,new BasicDecimal128("1.00",37));
        v640.set(1,new BasicDecimal128("1.00001",37));
        v640.set(2,new BasicDecimal128("0.00001",37));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        BasicTable bt = new BasicTable(colNames,cols);
        appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
    }
    @Test
    public void test_AutoFitTableAppender_ArrayVector_decimal_compress_true() throws Exception {

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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://testArrayVector","pt",conn);

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
    public void test_AutoFitTableAppender_ArrayVector_decimal128_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(10)[],DECIMAL128(37)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        DBConnection connection = new DBConnection(false, false, true);
        connection.connect(HOST, PORT, "admin", "123456");
        conn.run(script);
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://testArrayVector","pt",conn);

        List<String> colNames = new ArrayList<>();
        colNames.add("cint");
        colNames.add("col0");
        colNames.add("col1");
        colNames.add("col2");
        List<Vector> cols = new ArrayList<>();
        cols.add(new BasicIntVector(new int[]{12,29,31}));
        List<Vector> bdvcol0 = new ArrayList<Vector>();
        Vector v32=new BasicDecimal128Vector(3,0);
        v32.set(0,new BasicDecimal128("15645.00",0));
        v32.set(1,new BasicDecimal128("24635.00001",0));
        v32.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v32);
        bdvcol0.add(1,v32);
        bdvcol0.add(2,v32);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v321=new BasicDecimal128Vector(3,10);
        v321.set(0,new BasicDecimal128("15645.00",10));
        v321.set(1,new BasicDecimal128("24635.00001",10));
        v321.set(2,new BasicDecimal128("24635.00001",10));
        bdvcol1.add(0,v321);
        bdvcol1.add(1,v321);
        bdvcol1.add(2,v321);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v640=new BasicDecimal128Vector(3,37);
        v640.set(0,new BasicDecimal128("1.00",37));
        v640.set(1,new BasicDecimal128("1.00001",37));
        v640.set(2,new BasicDecimal128("0.00001",37));
        bdvcol2.add(0,v640);
        bdvcol2.add(1,v640);
        bdvcol2.add(2,v640);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        BasicTable bt = new BasicTable(colNames,cols);
        appender.append(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
    }
    @Test
    public void testgetDTString() throws IOException{
        DBConnection connection = new DBConnection();
        connection.connect(HOST, PORT, "admin", "123456");
        conn.run("\n" +
                "login(`admin,`123456)\n" +
                "dbPath = \"dfs://tableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(1000:0,`id`Mon,[INT,MONTH])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`TestMon,`id)");
        AutoFitTableAppender tableAppender = new AutoFitTableAppender("dfs://tableAppenderTest", "pt", connection, com.xxdb.route.AutoFitTableAppender.APPEND_ACTION.fitColumnType);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestMon",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDate",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTime",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestMinute",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestSecond",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDatetime",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStamp",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanotime",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanotimestamp",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanotimestamp",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestDateTimeToDateHour",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestTimeStampToDateHour",conn);
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
        AutoFitTableAppender appender=new AutoFitTableAppender("dfs://tableAppenderTest","TestNanoTimeStampToDateHour",conn);
        appender.append(insert);
        BasicTable Tappender=(BasicTable) conn.run("select * from pt");
        for (int ind=0;ind<Tappender.rows();++ind) {
            BasicDateHourVector vector = (BasicDateHourVector)  Tappender.getColumn("datehour");
            Assert.assertEquals(result.getDateHour(),vector.getDateHour(ind));
        }
    }
    @Test
    public void Test_AutoFitTableAppender_keyedTable_allDateType() throws IOException {
        String script = null;
        script = "cbool = true false false;\n";
        script += "cchar = 'a' 'b' 'c';\n";
        script += "cshort = 122h 32h 45h;\n";
        script += "cint = 1 4 9;\n";
        script += "clong = 17l 39l 72l;\n";
        script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        script += "cminute = 03:25m 08:12m 10:15m;\n";
        script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        script += "cfloat = 7.5f 0.79f 8.27f;\n";
        script += "cdouble = 5.7 7.2 3.9;\n";
        script += "cstring = \"hello\" \"hi\" \"here\";\n";
        script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
        script += "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
        script += "share t as st;";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "st", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from st;");
        Assert.assertEquals(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
        compareBasicTable(bt, act);
        conn.run("undef(`st, SHARED)");
        }
    @Test
    public void Test_AutoFitTableAppender_keyedTable_allDateType_1() throws IOException {
        String script = "n=100;\n";
        script += "intv = 1..100;\n";
        script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
        script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
        script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
        script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "t = keyedTable(`intv,intv,uuidv,ippaddrv,int128v,complexv,pointv)\n";
        script += "t1 = keyedTable(`intv,100:0,`intv`uuidv`ippaddrv`int128v`complexv`pointv,[INT,UUID,IPADDR,INT128,COMPLEX,POINT])\n";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("select * from t");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "t1", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from t1;");
        Assert.assertEquals(100, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from t");
        compareBasicTable(bt, act);
    }
    @Test
    public void Test_AutoFitTableAppender_memoryTable_allDateType() throws IOException {
        String script = null;
        script = "cbool = true false false;\n";
        script += "cchar = 'a' 'b' 'c';\n";
        script += "cshort = 122h 32h 45h;\n";
        script += "cint = 1 4 9;\n";
        script += "clong = 17l 39l 72l;\n";
        script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        script += "cminute = 03:25m 08:12m 10:15m;\n";
        script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        script += "cfloat = 7.5f 0.79f 8.27f;\n";
        script += "cdouble = 5.7 7.2 3.9;\n";
        script += "cstring = \"hello\" \"hi\" \"here\";\n";
        script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
        script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
        script += "share t as st;";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "st", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from st;");
        Assert.assertEquals(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
        compareBasicTable(bt, act);
        conn.run("undef(`st, SHARED)");
    }
    @Test
    public void Test_AutoFitTableAppender_memoryTable_allDateType_1() throws IOException {
        String script = "n=100;\n";
        script += "intv = 1..100;\n";
        script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
        script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
        script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
        script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "t = table(intv,uuidv,ippaddrv,int128v,complexv,pointv)\n";
        script += "t1 = table(100:0,`intv`uuidv`ippaddrv`int128v`complexv`pointv,[INT,UUID,IPADDR,INT128,COMPLEX,POINT])\n";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("select * from t");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "t1", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from t1;");
        Assert.assertEquals(100, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from t");
        compareBasicTable(bt, act);
    }
    @Test
    public void Test_AutoFitTableAppender_streamTable_allDateType() throws IOException {
        String script = null;
        script = "cbool = true false false;\n";
        script += "cchar = 'a' 'b' 'c';\n";
        script += "cshort = 122h 32h 45h;\n";
        script += "cint = 1 4 9;\n";
        script += "clong = 17l 39l 72l;\n";
        script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        script += "cminute = 03:25m 08:12m 10:15m;\n";
        script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        script += "cfloat = 7.5f 0.79f 8.27f;\n";
        script += "cdouble = 5.7 7.2 3.9;\n";
        script += "cstring = \"hello\" \"hi\" \"here\";\n";
        script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
        script += "t = streamTable(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
        script += "share t as st;";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
        long start = System.nanoTime();
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "st", conn);
        aftu.append(bt);
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        BasicTable ua = (BasicTable)conn.run("select * from st;");
        Assert.assertEquals(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
        compareBasicTable(bt, act);
        conn.run("undef(`st, SHARED)");
    }
    @Test
    public void Test_AutoFitTableAppender_streamTable_allDateType_1() throws IOException {
        String script = "n=5000000;\n";
        script += "intv = 1..5000000;\n";
        script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
        script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
        script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
        script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "t = streamTable(intv,uuidv,ippaddrv,int128v,complexv,pointv)\n";
        script += "t1 = table(100:0,`intv`uuidv`ippaddrv`int128v`complexv`pointv,[INT,UUID,IPADDR,INT128,COMPLEX,POINT])\n";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("select * from t");
        BasicTable bt1 = (BasicTable)conn.run("select top 10 * from t");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "t1", conn);
        long start = System.nanoTime();
        Entity re = aftu.append(bt);
        assertEquals("5000000",re.getString());
        long end = System.nanoTime();
        System.out.println((end - start) / 1000000);
        assertEquals(true,((end - start) / 1000000)<5000);
        for(int i=0;i<10;i++){
            Entity re1 = aftu.append(bt1);
            assertEquals("10",re1.getString());
        }
        long end1 = System.nanoTime();
        System.out.println((end1 - end)/ 10000000);
        assertEquals(true,((end1 - end)/ 10000000)<2);
        BasicTable ua = (BasicTable)conn.run("select * from t1;");
        assertEquals(5000100, ua.rows());
        BasicTable ua1 = (BasicTable)conn.run("select * from t1 limit 5000000;");
        compareBasicTable(bt, ua1);
    }
    @Test
    public void Test_AutoFitTableAppender_DfsTable_allDateType() throws IOException {
        String script = null;
        script = "cbool = true false false;\n";
        script += "cchar = 'a' 'b' 'c';\n";
        script += "cshort = 122h 32h 45h;\n";
        script += "cint = 1 4 9;\n";
        script += "clong = 17l 39l 72l;\n";
        script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        script += "cminute = 03:25m 08:12m 10:15m;\n";
        script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        script += "cfloat = 7.5f 0.79f 8.27f;\n";
        script += "cdouble = 5.7 7.2 3.9;\n";
        script += "cstring = \"hello\" \"hi\" \"here\";\n";
        script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
        script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
        script += "dbPath = \"dfs://tableAppenderTest\"\n" ;
        script += "if(existsDatabase(dbPath))\n";
        script += "dropDatabase(dbPath)\n";
        script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
        script += "pt = db.createPartitionedTable(t,`pt,`cint,,`cint`cdate)\n";
        script += "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://tableAppenderTest", "pt", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt);");
        Assert.assertEquals(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt) where cint = 10;");
        compareBasicTable(bt, act);
    }
    @Test
    public void Test_AutoFitTableAppender_DfsTable_allDateType_1() throws IOException {
        String script = "n=100;\n";
        script += "intv = 1..100;\n";
        script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
        script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
        script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
        script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "t = table(intv,uuidv,ippaddrv,int128v,complexv,pointv)\n";
        script += "dbPath = \"dfs://tableAppenderTest\"\n" ;
        script += "if(existsDatabase(dbPath))\n";
        script += "dropDatabase(dbPath)\n";
        script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
        script += "pt = db.createPartitionedTable(t,`pt,`intv,,`intv)\n";
        script += "pt1 = db.createPartitionedTable(t,`pt1,`intv,,`intv)\n";
        script += "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("select * from t");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://tableAppenderTest", "pt1", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt1);");
        Assert.assertEquals(100, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt) ");
        compareBasicTable(bt, act);
    }
    @Test
    public void Test_AutoFitTableAppender_DimensionTable_allDateType() throws IOException {
        String script = null;
        script = "cbool = true false false;\n";
        script += "cchar = 'a' 'b' 'c';\n";
        script += "cshort = 122h 32h 45h;\n";
        script += "cint = 1 4 9;\n";
        script += "clong = 17l 39l 72l;\n";
        script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        script += "cminute = 03:25m 08:12m 10:15m;\n";
        script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        script += "cfloat = 7.5f 0.79f 8.27f;\n";
        script += "cdouble = 5.7 7.2 3.9;\n";
        script += "cstring = \"hello\" \"hi\" \"here\";\n";
        script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
        script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
        script += "dbPath = \"dfs://tableAppenderTest\"\n" ;
        script += "if(existsDatabase(dbPath))\n";
        script += "dropDatabase(dbPath)\n";
        script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
        script += "pt = db.createTable(t,`pt,,`cint)\n";
        script += "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://tableAppenderTest", "pt", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt);");
        Assert.assertEquals(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt) where cint = 10;");
        compareBasicTable(bt, act);
    }
    @Test
    public void Test_AutoFitTableAppender_DimensionTable_allDateType_1() throws IOException {
        String script = "n=100;\n";
        script += "intv = 1..100;\n";
        script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
        script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
        script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
        script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
        script += "t = table(intv,uuidv,ippaddrv,int128v,complexv,pointv)\n";
        script += "dbPath = \"dfs://tableAppenderTest\"\n" ;
        script += "if(existsDatabase(dbPath))\n";
        script += "dropDatabase(dbPath)\n";
        script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
        script += "pt = db.createTable(t,`pt,,`intv)\n";
        script += "pt1 = db.createTable(t,`pt1,,`intv)\n";
        script += "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable)conn.run("select * from t");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://tableAppenderTest", "pt1", conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt1);");
        Assert.assertEquals(100, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt) ");
        compareBasicTable(bt, act);
    }
    @Test
    public void test_AutoFitTableAppender_illegal_string() throws Exception {
        conn.run("if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1'); db = database('dfs://db1', VALUE, 1..10,,'TSDB');t = table(1:0,`id`string1`symbol1`blob1,[INT,STRING,SYMBOL,BLOB]);db.createPartitionedTable(t,'t1', 'id',,'id')");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://db1", "t1", conn);
        List<String> colName=new ArrayList<>();
        colName.add("id");
        colName.add("string1");
        colName.add("symbol1");
        colName.add("blob1");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector trade_dt = new BasicIntVector(0);
        trade_dt.add(1);
        trade_dt.add(2);
        trade_dt.add(3);
        cols.add(trade_dt);
        BasicStringVector string1 = new BasicStringVector(0);
        string1.add("string1AM\000ZN/n0");
        string1.add("\000\00PL\0");
        string1.add("string1AM\0");
        cols.add(string1);
        BasicStringVector symbol1 = new BasicStringVector(0);
        symbol1.add("symbol1AM\0\0ZN");
        symbol1.add("\0symbol1AP\0PL\0");
        symbol1.add("symbol1AM\0");
        cols.add(symbol1);
        BasicStringVector blob1 = new BasicStringVector(0);
        blob1.add("blob1AM\0ZN");
        blob1.add("\0blob1AM\0ZN");
        blob1.add("blob1AMZN\0");
        cols.add(blob1);
        BasicTable tmpTable = new BasicTable(colName, cols);
        aftu.append(tmpTable);
        BasicTable table2 = (BasicTable) conn.run("select * from loadTable(\"dfs://db1\", `t1) ;\n");
        System.out.println(table2.getString());
        assertEquals("string1AM", table2.getColumn(1).get(0).getString());
        assertEquals("", table2.getColumn(1).get(1).getString());
        assertEquals("string1AM", table2.getColumn(1).get(2).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(0).getString());
        assertEquals("", table2.getColumn(2).get(1).getString());
        assertEquals("symbol1AM", table2.getColumn(2).get(2).getString());
        assertEquals("blob1AM", table2.getColumn(3).get(0).getString());
        assertEquals("", table2.getColumn(3).get(1).getString());
        assertEquals("blob1AMZN", table2.getColumn(3).get(2).getString());
    }
    @Test(timeout = 120000)
    public void test_AutoFitTableAppender_allDataType_null() throws Exception {
        List<String> colNames = new ArrayList<String>();
        colNames.add("boolv");
        colNames.add("charv");
        colNames.add("shortv");
        colNames.add("intv");
        colNames.add("longv");
        colNames.add("doublev");
        colNames.add("floatv");
        colNames.add("datev");
        colNames.add("monthv");
        colNames.add("timev");
        colNames.add("minutev");
        colNames.add("secondv");
        colNames.add("datetimev");
        colNames.add("timestampv");
        colNames.add("nanotimev");
        colNames.add("nanotimestampv");
        colNames.add("symbolv");
        colNames.add("stringv");
        colNames.add("uuidv");
        colNames.add("datehourv");
        colNames.add("ippaddrv");
        colNames.add("int128v");
        colNames.add("blobv");
        colNames.add("complexv");
        colNames.add("pointv");
        colNames.add("decimal32v");
        colNames.add("decimal64v");
        colNames.add("decimal128V");

        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicBooleanVector(0));
        cols.add(new BasicByteVector(0));
        cols.add(new BasicShortVector(0));
        cols.add(new BasicIntVector(0));
        cols.add(new BasicLongVector(0));
        cols.add(new BasicDoubleVector(0));
        cols.add(new BasicFloatVector(0));
        cols.add(new BasicDateVector(0));
        cols.add(new BasicMonthVector(0));
        cols.add(new BasicTimeVector(0));
        cols.add(new BasicMinuteVector(0));
        cols.add(new BasicSecondVector(0));
        cols.add(new BasicDateTimeVector(0));
        cols.add(new BasicTimestampVector(0));
        cols.add(new BasicNanoTimeVector(0));
        cols.add(new BasicNanoTimestampVector(0));
        cols.add(new BasicSymbolVector(0));
        cols.add(new BasicStringVector(0));
        cols.add(new BasicUuidVector(0));
        cols.add(new BasicDateHourVector(0));
        cols.add(new BasicIPAddrVector(0));
        cols.add(new BasicInt128Vector(0));
        cols.add(new BasicStringVector(new String[0],true));
        cols.add(new BasicComplexVector(0));
        cols.add(new BasicPointVector(0));
        cols.add(new BasicDecimal32Vector(0,0));
        cols.add(new BasicDecimal64Vector(0,0));
        cols.add(new BasicDecimal128Vector(0,0));
        conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n" +
                " db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n " +
                "t= table(100:0,`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`complexv`pointv`decimal32v`decimal64v`decimal128v, " +
                "[BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, complex, POINT, DECIMAL32(3), DECIMAL64(4),DECIMAL128(10) ]);\n" +
                " pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://empty_table", "pt", conn);
        aftu.append(new BasicTable(colNames, cols));
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        conn.close();
    }
    @Test(timeout = 120000)
    public void test_AutoFitTableAppender_allDataType_array_null() throws Exception {
        List<String> colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("boolv");
        colNames.add("charv");
        colNames.add("shortv");
        colNames.add("intv");
        colNames.add("longv");
        colNames.add("doublev");
        colNames.add("floatv");
        colNames.add("datev");
        colNames.add("monthv");
        colNames.add("timev");
        colNames.add("minutev");
        colNames.add("secondv");
        colNames.add("datetimev");
        colNames.add("timestampv");
        colNames.add("nanotimev");
        colNames.add("nanotimestampv");
        //colNames.add("symbolv");
        //colNames.add("stringv");
        colNames.add("uuidv");
        colNames.add("datehourv");
        colNames.add("ippaddrv");
        colNames.add("int128v");
        //colNames.add("blobv");
        colNames.add("complexv");
        colNames.add("pointv");
        colNames.add("decimal32v");
        colNames.add("decimal64v");
        colNames.add("decimal128V");

        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicIntVector(0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BOOL_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BYTE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SHORT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_LONG_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_FLOAT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MONTH_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MINUTE_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SECOND_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATETIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIME_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIMESTAMP_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_UUID_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_IPADDR_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT128_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_COMPLEX_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_POINT_ARRAY,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL32_ARRAY,0,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL64_ARRAY,0,0));
        cols.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL128_ARRAY,0,0));
        conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n" +
                " db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n " +
                "t= table(100:0,`id`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`complexv`pointv`decimal32v`decimal64v`decimal128v, " +
                "[INT, BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], COMPLEX[], POINT[], DECIMAL32(3)[], DECIMAL64(4)[],DECIMAL128(10)[] ]);\n" +
                " pt=db.createPartitionedTable(t,`pt,`id,,`id);");
        AutoFitTableAppender aftu = new AutoFitTableAppender("dfs://empty_table", "pt", conn);
        aftu.append(new BasicTable(colNames, cols));
        BasicTable bt = (BasicTable) conn.run("select * from loadTable(\"dfs://empty_table\",`pt);");
        assertEquals(0, bt.rows());
        conn.close();
    }
    @Test
    public void Test_AutoFitTableAppender_dateType_not_match() throws IOException {
        PrepareTable("DATE");
        AutoFitTableAppender aftu = new AutoFitTableAppender("", "pt", conn);
        List<String> colName=new ArrayList<>();
        colName.add("permno");
        colName.add("dateType");
        List<Vector> cols = new ArrayList<>();
        BasicIntVector permno = new BasicIntVector(0);
        permno.add(1);
        cols.add(permno);
        BasicIntVector dateType = new BasicIntVector(0);
        dateType.add(2);
        cols.add(dateType);
        BasicTable bt = new BasicTable(colName, cols);
        aftu.append(bt);
        BasicTable bt1 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt1.rows());

        PrepareTable("MONTH");
        aftu.append(bt);
        BasicTable bt2 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt2.rows());

        PrepareTable("TIME");
        aftu.append(bt);
        BasicTable bt3 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt3.rows());

        PrepareTable("MINUTE");
        aftu.append(bt);
        BasicTable bt4 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt4.rows());

        PrepareTable("SECOND");
        aftu.append(bt);
        BasicTable bt5 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt5.rows());

        PrepareTable("DATETIME");
        aftu.append(bt);
        BasicTable bt6 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt6.rows());

        PrepareTable("TIMESTAMP");
        aftu.append(bt);
        BasicTable bt7 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt7.rows());

        PrepareTable("NANOTIME");
        aftu.append(bt);
        BasicTable bt8 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt8.rows());

        PrepareTable("NANOTIMESTAMP");
        aftu.append(bt);
        BasicTable bt9 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt9.rows());

        PrepareTable("DATEHOUR");
        aftu.append(bt);
        BasicTable bt10 = (BasicTable) conn.run("select * from pt;");
        assertEquals(0, bt10.rows());
    }

}

