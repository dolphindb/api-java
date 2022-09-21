package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.route.AutoFitTableUpsert;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

public class AutoFitTableUpsertTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String[] ipports = bundle.getString("SITES").split(",");


    static String[] host_list= bundle.getString("HOSTS").split(",");
    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();
    //String[] highAvailabilitySites = {"192.168.0.57:9002","192.168.0.57:9003","192.168.0.57:9004","192.168.0.57:9005"};

    @Test
    public void test_tableUpsert_DP_baseNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType";
        String tableName = "pt";
        String script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType\"\n"+
                "if(exists(dbName)){\n"+
                "\tdropDatabase(dbName)\t\n"+
                "}\n"+
                "db  = database(dbName, RANGE,1 10000,,'TSDB')\n"+
                "t = table(1000:0, `id`value,[ INT, INT[]])\n"+
                "pt = db.createPartitionedTable(t,`pt,`id,,`id)";
        conn.run(script);
        BasicIntVector v1 = new BasicIntVector(3);
        v1.setInt(0, 1);
        v1.setInt(1, 100);
        v1.setInt(2, 9999);
        BasicArrayVector ba = new BasicArrayVector(Entity.DATA_TYPE.DT_INT_ARRAY, 1);
        ba.Append(v1);
        ba.Append(v1);
        ba.Append(v1);
        List<String> colNames = new ArrayList<>();
        colNames.add("id");
        colNames.add("value");
        List<Vector> cols = new ArrayList<>();
        cols.add(v1);
        cols.add(ba);
        BasicTable bt = new BasicTable(colNames, cols);
        String[] keyColName = new String[]{"id"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert(dbName, tableName, conn, false, keyColName, null);
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from pt;");
        assertEquals(3,res.rows());
        assertEquals("[[1,100,9999],[1,100,9999],[1,100,9999]]",res.getColumn(1).getString());
        assertEquals(Entity.DATA_TYPE.DT_INT,res.getColumn(0).getDataType());
    }

    @Test(expected = Exception.class)
    public void test_tableUpsert_DP_addNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType";
        String tableName = "pt";
        String script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType\"\n"+
                "if(exists(dbName)){\n"+
                "\tdropDatabase(dbName)\t\n"+
                "}\n"+
                "db  = database(dbName, RANGE,1 10000,,'TSDB')\n"+
                "t = table(1000:0, `id`value,[ INT, INT[]])\n"+
                "pt = db.createPartitionedTable(t,`pt,`id,,`id)";
        conn.run(script);
        String[] keyColName = new String[]{"id"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert(dbName, tableName, conn, false, keyColName, null);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        BasicTable bt = new BasicTable(colNames,cols);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_DP_updateFirst() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable ub = (BasicTable) conn.run("select * from pt");
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.09, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        assertEquals(ub.rows()+1,ua.rows());
        assertEquals(1,conn.run("select * from pt where sym = `A and price = 11.1;").rows());
        assertEquals(0,conn.run("select * from pt where price=8.3").rows());
        assertEquals(1,conn.run("select * from pt where price=4.5").rows());
        assertEquals(1,conn.run("select * from pt where price=7.6").rows());
        assertEquals(3,conn.run("select * from pt where sym = `A;").rows());
    }
    @Test
    public void test_tableUpsert_DP_sortColumns() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.09, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,psortColumns);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        float a = Float.parseFloat(ua.getColumn(2).get(0).toString());
        float b = Float.parseFloat(ua.getColumn(2).get(1).toString());
        assertTrue(a<b);
    }

    @Test
    public void test_tableUpsert_DP_sortColumnsNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.09, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        int a = Integer.parseInt(ua.getColumn(3).get(0).toString());
        int b = Integer.parseInt(ua.getColumn(3).get(1).toString());
        assertFalse(a<b);
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_DP_notMatchColumns() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( 1 2 as id2, 1 2 as id, 1 NULL as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_DP_notMatchDataType() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createPartitionedTable(t, `pt, `sym)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( `A`B`E as sym,1 2 3 as id2, 1 2 3 as id, 1 2 3 as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_DP_ignoreNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createPartitionedTable(t, \"pt\", `id).append!(t)\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1 NULL as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt where value = NULL;");
        assertEquals(0,ua.rows());
        AutoFitTableUpsert aftu2 = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,false,keyColName,null);
        aftu2.upsert(bt);
        BasicTable ua2 = (BasicTable) conn.run("select * from pt where value = NULL;");
        assertEquals(1,ua2.rows());
    }

    @Test(timeout=120000)
    public void test_tableUpsert_DP_BigData() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createPartitionedTable(t, \"pt\", `id).append!(t)\n";
        conn.run(script);
        String scripts = "id = take(100..2000,50080000)\n" +
                "id2 = 101..50080100\n" +
                "value = 50080100..101\n" +
                "t2 = table(id,id2,value);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(50080100,ua.rows());
    }

    @Test
    public void test_tableUpsert_DD_bigData() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createTable(t, \"pt\").append!(t)\n";
        conn.run(script);
        String scripts = "id = take(100..2000,50080000)\n" +
                "id2 = 101..50080100\n" +
                "value = 50080100..101\n" +
                "t2 = table(id,id2,value);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(50080100,ua.rows());
    }

    @Test
    public void test_tableUpsert_DD_ignoreNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")) {\n" +
                "dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "db = database(\"dfs://valuedemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n" +
                "pt = db.createTable(t, \"pt\").append!(t)\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1 NULL as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt where value = NULL;");
        assertEquals(0,ua.rows());
        AutoFitTableUpsert aftu2 = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,false,keyColName,null);
        aftu2.upsert(bt);
        BasicTable ua2 = (BasicTable) conn.run("select * from pt where value = NULL;");
        assertEquals(1,ua2.rows());
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_DD_notMatchColumns() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( 1 2 as id2, 1 2 as id, 1 NULL as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_DD_notMatchDataType() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table( `A`B`E as sym,1 2 3 as id2, 1 2 3 as id, 1 2 3 as value);t2;");
        String[] keyColName = new String[]{"id2"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,true,keyColName,null);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_DD_sortColumns() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.09, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,psortColumns);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        int a = Integer.parseInt(ua.getColumn(3).get(1).toString());
        int b = Integer.parseInt(ua.getColumn(3).get(2).toString());
        assertTrue(a<b);
    }

    @Test
    public void test_tableUpsert_DD_sortColumnsNull() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://upsert\")) {\n" +
                "dropDatabase(\"dfs://upsert\")\n" +
                "}\n" +
                "sym=`A`B`C`A`D`B`A\n" +
                "date=take(2021.12.10,3) join take(2021.12.09, 3) join 2021.12.10\n" +
                "price=8.3 7.2 3.7 4.5 6.3 8.4 7.6\n" +
                "val=10 19 13 9 19 16 10\n" +
                "t=table(sym, date, price, val)\n" +
                "db=database(\"dfs://upsert\", VALUE,  `A`B`C)\n" +
                "pt=db.createTable(t, `pt)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.09, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        int a = Integer.parseInt(ua.getColumn(3).get(0).toString());
        int b = Integer.parseInt(ua.getColumn(3).get(1).toString());
        assertFalse(a<b);
    }

    @Test
    public void test_tableUpsert_DP_objNull_sort() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")){\n" +
                "    dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "t = table(10000:0,`sym`date`price`val,[SYMBOL,DATE,FLOAT,INT])\n" +
                "db = database(\"dfs://valuedemo\",VALUE,`A`B`C)\n" +
                "pt = db.createPartitionedTable(t,`pt,`sym)";
        conn.run(script);
        String scripts = "sym = take(`A`B`C`D,200)\n" +
                "date = take(2016.06.15..2016.08.31,200)\n" +
                "price = rand(50.0,200)\n" +
                "val = take(50..100,200)\n" +
                "t2 = table(sym,date,price,val);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("t2;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,false,keyColName,psortColumns);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        System.out.println(ua.getString());
        int a = Integer.parseInt(ua.getColumn(3).get(0).toString());
        int b = Integer.parseInt(ua.getColumn(3).get(1).toString());
        assertTrue(a<b);
    }

    @Test
    public void test_tableUpsert_DD_objNull_sort() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://valuedemo\")){\n" +
                "    dropDatabase(\"dfs://valuedemo\")\n" +
                "}\n" +
                "t = table(10000:0,`sym`date`price`val,[SYMBOL,DATE,FLOAT,INT])\n" +
                "db = database(\"dfs://valuedemo\",VALUE,`A`B`C)\n" +
                "pt = db.createTable(t,`pt)";
        conn.run(script);
        String scripts = "sym = take(`A`B`C`D,200)\n" +
                "date = take(2016.06.15..2016.08.31,200)\n" +
                "price = rand(50.0,200)\n" +
                "val = take(50..100,200)\n" +
                "t2 = table(sym,date,price,val);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("t2;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://valuedemo","pt",conn,false,keyColName,psortColumns);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        System.out.println(ua.getString());
        int a = Integer.parseInt(ua.getColumn(3).get(0).toString());
        int b = Integer.parseInt(ua.getColumn(3).get(1).toString());
        assertTrue(a<b);
    }

    @Test
    public void test_tableUpsert_KeyedTable_Normal() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);";
        conn.run(script);
        String scripts = "newData = table(`A`B`C`D as sym1, take(2021.01.06, 4) as date1, NULL NULL 300 400 as x1, NULL 600 700 800 as y1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(4,ua.rows());
        assertEquals(1,conn.run("select * from t where y = 5").rows());
        assertEquals(0,conn.run("select * from t where y = 6").rows());
        assertEquals(0,conn.run("select * from t where y = 7").rows());
        assertEquals(1,conn.run("select * from t where x = 1").rows());
        conn.run("clear!(t)");
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_KeyTable_noMatchColOrder() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);";
        conn.run(script);
        String scripts = "newData = table(take(2021.01.06,4) as date1, `A`B`C`D as sym1, NULL NULL 300 400 as x1, NULL 600 700 800 as y1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_keyTable_updateSame() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);";
        conn.run(script);
        String scripts = "newData = table(`C`D`E as sym1, 2021.01.06 2022.07.11 2022.09.21 as date1, 400 550 720 as x1, 17 88 190 as y1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(5,ua.rows());
        assertEquals(0,conn.run("select * from t where x=3;").rows());
        conn.run("clear!(t);");
    }

    @Test(expected = IOException.class)
    public void test_tableUpsert_keyTable_sortCol() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=keyedTable(`sym`date, sym, date, x, y);";
        conn.run(script);
        String scripts = "newData = table(`C`D`E as sym1, 2021.01.06 2022.07.11 2022.09.21 as date1, 400 550 720 as x1, 17 88 190 as y1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        String[] pkeyColNames = new String[]{"sym","date"};
        String[] psortCols = new String[]{"date","x"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,pkeyColNames,psortCols);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_indexedTable_normal() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C`D`E\n" +
                "id=5 4 3 2 1\n" +
                "val=52 64 25 48 71\n" +
                "t=indexedTable(`sym`id,sym,id,val);";
        conn.run(script);
        String scripts = "newData = table(`F`G as sym1, 7 9 as id1, 36 11 as val1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(7,ua.rows());
        conn.run("clear!(t);");
    }

    @Test(expected = RuntimeException.class)
    public void test_tableUpsert_indexedTable_noMatchOrder() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C`D`E\n" +
                "id=5 4 3 2 1\n" +
                "val=52 64 25 48 71\n" +
                "t=indexedTable(`sym`id,sym,id,val);";
        conn.run(script);
        String scripts = "newData = table(7 9 as id1,`F`G as sym1, 36 11 as val1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
    }

    @Test
    public void test_tableUpsert_indexedTable_updateSame() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C`D`E\n" +
                "id=5 4 3 2 1\n" +
                "val=52 64 25 48 71\n" +
                "t=indexedTable(`sym`id,sym,id,val);";
        conn.run(script);
        String scripts = "newData = table(`C`D`E`F`G as sym1, 3 2 1 7 8 as id1, 36 77 94 66 82 as val1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        assertEquals(7,ua.rows());
        assertEquals(0,conn.run("select * from t where val = 25;").rows());
        assertEquals(1,conn.run("select * from t where val = 77;").rows());
        conn.run("clear!(t)");
    }

    @Test(expected = IOException.class)
    public void test_tableUpsert_indexedTable_sortCols() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "sym=`A`B`C\n" +
                "date=take(2021.01.06, 3)\n" +
                "x=1 2 3\n" +
                "y=5 6 7\n" +
                "t=indexedTable(`sym`date, sym, date, x, y);";
        conn.run(script);
        String scripts = "newData = table(`C`D`E as sym1, 2021.01.06 2022.07.11 2022.09.21 as date1, 400 550 720 as x1, 17 88 190 as y1);";
        conn.run(scripts);
        BasicTable bt = (BasicTable) conn.run("newData;");
        String[] pkeyColNames = new String[]{"sym","date"};
        String[] psortCols = new String[]{"date","x"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,pkeyColNames,psortCols);
        aftu.upsert(bt);
    }

}
