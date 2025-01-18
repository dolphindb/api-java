package com.xxdb.restart;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.multithreadedtablewriter.Callback;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MTWCallbackTest {
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String CONTROLLER_HOST = bundle.getString("CONTROLLER_HOST");
    static int CONTROLLER_PORT = Integer.parseInt(bundle.getString("CONTROLLER_PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");

    public static Integer insertTime = 5000;
    public static ErrorCodeInfo pErrorInfo =new ErrorCodeInfo();;

    //private final int id;
    private static MultithreadedTableWriter mutithreadTableWriter_ = null;

    @BeforeClass
    public static void prepare1 () throws IOException {
        DBConnection controller_conn = new DBConnection();
        controller_conn.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        controller_conn.run("try{startDataNode('" + HOST + ":" + PORT + "')}catch(ex){}");
        controller_conn.run("sleep(8000)");
    }
    @Before
    public void prepare() throws IOException {
        conn = new DBConnection(false,false,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
    public void dropAllDB() throws IOException {
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("for(db in getClusterDFSDatabases()){\n" +
                "\tdropDatabase(db)\n" +
                "}");

        conn.run("try{undef(`t1,SHARED)}catch(ex){}");
        conn.run("try{undef(`t2,SHARED)}catch(ex){}");
        conn.run("try{undef(`st1,SHARED)}catch(ex){}");
        conn.run("try{undef(`st2,SHARED)}catch(ex){}");
        conn.run("try{undef(`st3,SHARED)}catch(ex){}");
        conn.run("try{undef(`st4,SHARED)}catch(ex){}");
        conn.run("try{undef(`st5,SHARED)}catch(ex){}");
        conn.run("try{undef(`ext1,SHARED)}catch(ex){}");
        conn.close();
    }

    @After
    public void close() throws IOException {
//        dropAllDB();
        String script = "obj =  exec name from objs(true) where shared=true;\n" +
                "for(s in obj)\n" +
                "{\n" +
                "undef (s, SHARED);\n" +
                "}";
        try{
            conn.run(script);
        }catch(Exception E){
            System.out.println(E.getMessage());
        }
        conn.close();
    }

    /**
     * Parameter check
     * @throws Exception
     */
    Callback callbackHandler = new Callback(){
        public void writeCompletion(Table callbackTable){
            List<String> failedIdList = new ArrayList<>();
            BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
            BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
            for (int i = 0; i < successV.rows(); i++){
                if (!successV.getBoolean(i)){
                    failedIdList.add(idV.getString(i));
                }
            }
        }
    };
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_memoryTable_single_thread_true()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("share table(100:0, [`col0], [INT]) as table1");
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable){
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                for (int i = 0; i < successV.rows(); i++){
                    //System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                    assertEquals(true, successV.getBoolean(i));
                    assertEquals("id"+i, idV.getString(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "", "table1", false,
                false, null, 10000, 1, 1, "", null, callbackHandler);

        for (int i = 0; i < 10; i++){
            ErrorCodeInfo pErrorInfo = mtw.insert("id"+i, i);
            assertEquals("code= info=",pErrorInfo.toString());
            //System.out.println(pErrorInfo.toString());
        }
        mtw.waitForThreadCompletion();
        BasicTable table1= (BasicTable) conn.run("select * from table1;");
        assertEquals(10,table1.rows());
        for (int i = 0; i < 10; i++){
            assertEquals(i, ((Scalar)table1.getColumn("col0").get(i)).getNumber());
        }
        conn.run("undef(`table1,SHARED)");
        conn.close();

    }
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_memoryTable_single_thread_false()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        conn.run("share table(100:0, [`col0], [INT]) as table1");
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable){
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                    assertEquals(false, successV.getBoolean(i));
                    assertEquals("id"+i, idV.getString(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "", "table1", false,
                false, null, 2, 1, 1, "", null, callbackHandler);
        for (int i = 0; i < 10; i++){
            if(i==5){
                try{
                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
                }
                catch(IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            conn1.run("sleep(5000)");
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert("id"+i, i);
            }
            catch(RuntimeException ex)
            {
                System.out.println(pErrorInfo.toString());
            }
        }
        //mtw.waitForThreadCompletion();
        try{
            conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        conn1.run("sleep(5000)");
        conn1.close();
    }
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_true()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                try {
                    callback.getColumn(0).Append(idV);
                    callback.getColumn(1).Append(successV);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 5, 1, 5, "id", null, callbackHandler);

        for (int i = 0; i < 300; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();

        System.out.println("callback rows"+callback.rows());
        assertEquals(300, callback.rows()-1);

        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn.upload(map);
        BasicTable act = (BasicTable) conn.run("select * from testUpload where issuccess = true order by id");
        BasicTable act1 = (BasicTable) conn.run("select * from testUpload order by id");

        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
        }
        conn.close();
    }
    @Test//(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_false()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = \"dfs://test_MultithreadedTableWriter\";\n" +
                "if(existsDatabase(dbName)){\n" +
                "dropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                try {
                    callback.getColumn(0).Append(idV);
                    callback.getColumn(1).Append(successV);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 5, 1, 5, "id", null, callbackHandler);

        for (int i = 0; i < 300; i++){
            if(i==100){
                try{
                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }

            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        System.out.println(mtw.getStatus().toString());
        mtw.waitForThreadCompletion();
        conn1.run("sleep(10000)");
        try{conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");

        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        conn1.run("sleep(10000)");
        DBConnection conn2= new DBConnection(false, false, false, false);
        conn2.connect(HOST, PORT, "admin", "123456");
        conn1.run("sleep(1000)");
        System.out.println("callback rows"+callback.rows());
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn2.upload(map);
        conn1.run("sleep(1000)");
        BasicTable act = (BasicTable) conn2.run("select * from testUpload where issuccess = true order by id");
        conn1.run("sleep(5000)");
        BasicTable ex = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
            System.out.println(ex.getColumn(0).get(i).getString());
        }
        conn.close();
        conn2.close();
    }
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_single_thread_true()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                try {
                    callback.getColumn(0).Append(idV);
                    callback.getColumn(1).Append(successV);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 5, 1, 1, "id", null, callbackHandler);

        for (int i = 0; i < 300; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();

        System.out.println("callback rows"+callback.rows());
        assertEquals(300, callback.rows()-1);

        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn.upload(map);
        BasicTable act = (BasicTable) conn.run("select * from testUpload where issuccess = true order by id");
        BasicTable act1 = (BasicTable) conn.run("select * from testUpload order by id");

        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
        }
        conn.close();
    }
    @Test(timeout = 200000)
    public  void test_MultithreadedTableWriter_Callback_dfs_single_thread_false()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                try {
                    callback.getColumn(0).Append(idV);
                    callback.getColumn(1).Append(successV);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 5, 1, 1, "id", null, callbackHandler);

        for (int i = 0; i < 300; i++){
            if(i==100){
                try{
                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        conn1.run("sleep(10000)");
        System.out.println(mtw.getStatus().toString());
        Assert.assertEquals(true,mtw.getStatus().sendFailedRows>0);
        //Assert.assertEquals(true,mtw.getStatus().unsentRows==0);
        mtw.waitForThreadCompletion();
        conn1.run("sleep(4000)");
        try{conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        conn1.run("sleep(8000)");
        DBConnection conn2= new DBConnection(false, false, false, false);
        conn2.connect(HOST, PORT, "admin", "123456");
        conn1.run("sleep(1000)");
        System.out.println("callback rows"+callback.rows());
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn2.upload(map);
        BasicTable act = (BasicTable) conn2.run("select * from testUpload where issuccess = true order by id");
        conn1.run("sleep(20000)");
        BasicTable ex = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
            System.out.println(ex.getColumn(0).get(i).getString());
        }
        conn.close();
        conn2.close();
    }

    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_true_bigData()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                synchronized (callback) {
                    try {
                        callback.getColumn(0).Append(idV);
                        callback.getColumn(1).Append(successV);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                for (int i = 0; i < successV.rows(); i++){
                    //System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 1000000, 1, 20, "id", null, callbackHandler);

        for (int i = 0; i < 3000000; i++){
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();

        System.out.println("callback rows"+callback.rows());
        //assertEquals(1000000000, callback.rows()-1);

        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn.upload(map);
        BasicTable act = (BasicTable) conn.run("select * from testUpload where issuccess = true order by id");
        BasicTable act1 = (BasicTable) conn.run("select * from testUpload order by id");

        BasicTable ex = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
        }
        conn.close();
    }
    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_false_bigData()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                synchronized (callback) {
                    try {
                        callback.getColumn(0).Append(idV);
                        callback.getColumn(1).Append(successV);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 1000, 1, 20, "id", null, callbackHandler);

        for (int i = 0; i < 3000000; i++){
            if(i==500000){
                try{
                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();
        conn1.run("sleep(10000)");
        try{conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");

        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        conn1.run("sleep(10000)");
        DBConnection conn2= new DBConnection(false, false, false, false);
        conn2.connect(HOST, PORT, "admin", "123456");
        conn1.run("sleep(20000)");
        System.out.println("callback rows"+callback.rows());
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn2.upload(map);
        BasicTable act = (BasicTable) conn2.run("select * from testUpload where issuccess = true order by id");
        BasicTable ex = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        System.out.println("ex rows"+ex.rows());

        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
            //System.out.println(ex.getColumn(0).get(i).getString());
            //System.out.println("callback rows"+callback.rows());
        }
        conn.close();
        conn2.close();
    }

    @Test(timeout = 120000)
    public  void test_MultithreadedTableWriter_Callback_dfs_single_thread_false_insertUnwrittenData()throws Exception {
        DBConnection conn= new DBConnection(false, false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        DBConnection conn1= new DBConnection(false, false, false, false);
        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
        StringBuilder sb = new StringBuilder();
        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName);\n" +
                "}\n" +
                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
                "dummy = table(100:0, [`id], [STRING]);\n" +
                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
        conn.run(sb.toString());
        List<Vector> cols = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        BasicStringVector bsv = new BasicStringVector(1);
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        colNames.add("id");
        colNames.add("issuccess");
        cols.add(bsv);
        cols.add(bbv);
        BasicTable callback = new BasicTable(colNames,cols);;
        Callback callbackHandler = new Callback(){
            public void writeCompletion(Table callbackTable) {
                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
                try {
                    callback.getColumn(0).Append(idV);
                    callback.getColumn(1).Append(successV);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                for (int i = 0; i < successV.rows(); i++){
                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
                }
            }
        };
        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
                false, null, 5, 1, 1, "id", null, callbackHandler);

        for (int i = 0; i < 300; i++){
            if(i==100){
                try{
                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
                }
                catch(IOException ex)
                {
                    System.out.println(ex.getMessage());
                }
            }
            try{
                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
            }
            catch(RuntimeException ex)
            {
                System.out.println(ex.getMessage());
            }
        }
        mtw.waitForThreadCompletion();
        conn1.run("sleep(10000)");
        try{conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");

        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }
        conn1.run("sleep(10000)");
        DBConnection conn2= new DBConnection(false, false, false, false);
        conn2.connect(HOST, PORT, "admin", "123456");
        conn1.run("sleep(1000)");
        System.out.println("callback rows"+callback.rows());
        int callbackrows = callback.rows()-1;
        Map<String,Entity> map = new HashMap<>();
        map.put("testUpload",callback);
        conn2.upload(map);
        conn1.run("sleep(1000)");
        BasicTable act = (BasicTable) conn2.run("select * from testUpload where issuccess = true order by id");
        conn1.run("sleep(20000)");
        BasicTable ex = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
        assertEquals(ex.rows(), act.rows());
        assertEquals(ex.rows(), act.rows());
        System.out.println("ex.rows()"+ex.rows());

        for (int i = 0; i < ex.rows(); i++){
            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
            System.out.println(ex.getColumn(0).get(i).getString());
        }
        try{
            List<List<Entity>> unwrite1 = mtw.getUnwrittenData();
        }
        catch(RuntimeException ex1){
            assertEquals("getUnwrittenData is disabled when callback is enabled.", ex1.getMessage());
        }
        conn.close();
        conn2.close();
    }

//    @Test
//    public  void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_false_insertUnwrittenData()throws Exception {
//        DBConnection conn= new DBConnection(false, false, false, false);
//        conn.connect(HOST, PORT, "admin", "123456");
//        DBConnection conn1= new DBConnection(false, false, false, false);
//        conn1.connect(CONTROLLER_HOST, CONTROLLER_PORT, "admin", "123456");
//        StringBuilder sb = new StringBuilder();
//        sb.append("dbName = 'dfs://test_MultithreadedTableWriter';\n" +
//                "if(existsDatabase(dbName)){\n" +
//                "\tdropDB(dbName);\n" +
//                "}\n" +
//                "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\");\n"+
//                "dummy = table(100:0, [`id], [STRING]);\n" +
//                "db.createPartitionedTable(dummy, `pt, `id, , `id);");
//        conn.run(sb.toString());
//        List<Vector> cols = new ArrayList<>();
//        List<String> colNames = new ArrayList<>();
//        BasicStringVector bsv = new BasicStringVector(1);
//        BasicBooleanVector bbv = new BasicBooleanVector(1);
//        colNames.add("id");
//        colNames.add("issuccess");
//        cols.add(bsv);
//        cols.add(bbv);
//        BasicTable callback = new BasicTable(colNames,cols);;
//        Callback callbackHandler = new Callback(){
//            public void writeCompletion(Table callbackTable) {
//                BasicStringVector idV = (BasicStringVector) callbackTable.getColumn(0);
//                BasicBooleanVector successV = (BasicBooleanVector) callbackTable.getColumn(1);
//                synchronized (callback){
//                    try {
//                        callback.getColumn(0).Append(idV);
//                        callback.getColumn(1).Append(successV);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//
//                for (int i = 0; i < successV.rows(); i++){
//                    System.out.println(idV.getString(i) + " " + successV.getBoolean(i));
//                }
//            }
//        };
//        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
//                false, null, 5, 1, 5, "id", null, callbackHandler);
//
//        for (int i = 0; i < 300; i++){
//            if(i==100){
//                try{
//                    conn1.run("stopDataNode([\""+HOST+":"+PORT+"\"])");
//                }
//                catch(IOException ex)
//                {
//                    System.out.println(ex.getMessage());
//                }
//            }
//            try{
//                ErrorCodeInfo pErrorInfo = mtw.insert(Integer.toString(i), Integer.toString(i));
//            }
//            catch(RuntimeException ex)
//            {
//                System.out.println(ex.getMessage());
//            }
//        }
//        mtw.waitForThreadCompletion();
//        try{conn1.run("startDataNode([\""+HOST+":"+PORT+"\"])");
//
//        }
//        catch(IOException ex) {
//            System.out.println(ex.getMessage());
//        }
//        conn1.run("sleep(1000)");
//        DBConnection conn2= new DBConnection(false, false, false, false);
//        conn2.connect(HOST, PORT, "admin", "123456");
//        conn1.run("sleep(1000)");
//        System.out.println("callback rows"+callback.rows());
//        int callbackrows = callback.rows()-1;
//        Map<String,Entity> map = new HashMap<>();
//        map.put("testUpload",callback);
//        conn2.upload(map);
//        BasicTable act = (BasicTable) conn2.run("select * from testUpload where issuccess = true order by id");
//        conn1.run("sleep(2000)");
//        BasicTable ex = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
//        assertEquals(ex.rows(), act.rows());
//        assertEquals(ex.rows(), act.rows());
//        System.out.println("ex.rows()"+ex.rows());
//        System.out.println("act.rows()"+act.rows());
//
//        for (int i = 0; i < ex.rows(); i++){
//            assertEquals(ex.getColumn(0).get(i).getString(), act.getColumn(0).get(i).getString());
//            System.out.println(ex.getColumn(0).get(i).getString());
//        }
//        List<List<Entity>> unwrite1 = mtw.getUnwrittenData();
//        assertTrue(unwrite1.size() >0);
//        System.out.println("unwrite1.size()"+unwrite1.size());
//
//        MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://test_MultithreadedTableWriter", "pt", false,
//                false, null, 5, 1, 1, "id", null, callbackHandler);
//        mtw1.insertUnwrittenData(unwrite1);
//        mtw1.waitForThreadCompletion();
//        BasicTable ex1 = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
//        System.out.println("ex1"+ex1.rows());
//
//        assertEquals(callbackrows,ex1.rows());
//        conn.close();
//        conn2.close();
//    }
}
