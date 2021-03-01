package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.DBConnectionPool;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class autoFitTableAppenderTest {

    private static String dburl="dfs://autoFitTableAppenderTest";
    private static String tableName="testAppend";
    private static DBConnection conn;
    private static String HOST="115.239.209.192";
    private static int PORT = 8961;
    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
    }
    @After
    public void tearDown() throws Exception {
        conn.run("if(existsDatabase(\"dfs://autoFitTableAppenderTest\")){\n" +"\tdropDatabase(\"dfs://autoFitTableAppenderTest\")\n" +                "}");
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
                "dbPath = \"dfs://autoFitTableAppenderTest\"\n" +
                "if(existsDatabase(dbPath))\n" +
                "dropDatabase(dbPath)\n" +
                "t = table(100:0,`id`time`data,[INT,TIME,DOUBLE])\n" +
                "db=database(dbPath,HASH, [INT,10])\n" +
                "pt = db.createPartitionedTable(t,`testAppend,`id)");
        autoFitTableAppender appender=new autoFitTableAppender("dfs://autoFitTableAppenderTest","testAppend",conn);
        appender.append(insert);
        Entity assertRet= conn.run("exec count(*) from loadTable(\"dfs://autoFitTableAppenderTest\", \"testAppend\")");
        String ret=assertRet.getString();
        Assert.assertEquals(String.valueOf(size), assertRet.getString());
    }
}
