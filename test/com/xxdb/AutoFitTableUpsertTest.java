package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.route.AutoFitTableUpsert;
import com.xxdb.route.tableAppender;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

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
        String script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType\";\n"+
                "if(existsDatabase(dbName)){\n"+
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
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.11, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        String[] psortColumns = new String[]{"date","val"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,psortColumns);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt");
        assertEquals("2021.12.09",ua.getColumn(1).get(0).getString());
        assertEquals("2021.12.11",ua.getColumn(1).get(2).getString());
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
        BasicTable bt = (BasicTable) conn.run("t1=table(`A`B`E as sym, take(2021.12.11, 3) as date, 11.1 10.5 6.9 as price, 12 9 11 as val);t1;");
        String[] keyColName = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://upsert","pt",conn,false,keyColName,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt where sym = `A");
        assertEquals("2021.12.11",ua.getColumn(1).get(0).getString());
        assertEquals("2021.12.10",ua.getColumn(1).get(2).getString());
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

    @Test(timeout=120000)
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
        String[] psortColumns = new String[]{"price","val"};
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

    @Test
    public void test_BasicDecimal_AutoFitTableUpsert_indexedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t=indexedTable(`sym,1:0,`sym`datetime`price`qty,[STRING,DATETIME,DECIMAL32(2),DECIMAL64(4)])";
        conn.run(script);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("sym");
        colNames.add("datetime");
        colNames.add("price");
        colNames.add("qty");
        BasicStringVector bsv = new BasicStringVector(new String[]{"Huya","Tonghuashun","maoyan"});
        cols.add(bsv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{17,989,9000});
        cols.add(bdtv);
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,2);
        bd32v.set(0,new BasicDecimal32(11,2));
        bd32v.set(1,new BasicDecimal32(19,2));
        bd32v.set(2,new BasicDecimal32(23,2));
        cols.add(bd32v);
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(3,4);
        bd64v.set(0,new BasicDecimal64(25,4));
        bd64v.set(1,new BasicDecimal64(49,4));
        bd64v.set(2,new BasicDecimal64(14,4));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("price").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("qty").get(i).getString());
        }
        conn.run("clear!(t)");
    }

    @Test
    public void test_BasicDecimal_AutoFitTableUpsert_KeyTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t=keyedTable(`sym,1:0,`sym`datetime`price`qty,[STRING,DATETIME,DECIMAL32(2),DECIMAL64(4)])";
        conn.run(script);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("sym");
        colNames.add("datetime");
        colNames.add("price");
        colNames.add("qty");
        BasicStringVector bsv = new BasicStringVector(new String[]{"Huya","Tonghuashun","maoyan"});
        cols.add(bsv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{17,989,9000});
        cols.add(bdtv);
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,2);
        bd32v.set(0,new BasicDecimal32(11,2));
        bd32v.set(1,new BasicDecimal32(19,2));
        bd32v.set(2,new BasicDecimal32(23,2));
        cols.add(bd32v);
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(3,4);
        bd64v.set(0,new BasicDecimal64(25,4));
        bd64v.set(1,new BasicDecimal64(49,4));
        bd64v.set(2,new BasicDecimal64(14,4));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("price").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("qty").get(i).getString());
        }
        conn.run("clear!(t)");
    }

    @Test
    public void test_BasicDecimal_AutoFitTableUpsert_PartitionedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,\"Huya\" \"Tonghuashun\" \"maoyan\");" +
                "t=keyedTable(`sym,1000:0,`sym`datetime`price`qty,[STRING,DATETIME,DECIMAL32(2),DECIMAL64(4)]);" +
                "pt = db.createPartitionedTable(t,`pt,`sym);" +
                "pt.append!(t)";
        conn.run(script);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("sym");
        colNames.add("datetime");
        colNames.add("price");
        colNames.add("qty");
        BasicStringVector bsv = new BasicStringVector(new String[]{"Huya","Tonghuashun","maoyan"});
        cols.add(bsv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{17,989,9000});
        cols.add(bdtv);
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,2);
        bd32v.set(0,new BasicDecimal32(11,2));
        bd32v.set(1,new BasicDecimal32(19,2));
        bd32v.set(2,new BasicDecimal32(23,2));
        cols.add(bd32v);
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(3,4);
        bd64v.set(0,new BasicDecimal64(25,4));
        bd64v.set(1,new BasicDecimal64(49,4));
        bd64v.set(2,new BasicDecimal64(14,4));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        String[] keyCols = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,keyCols,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("price").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("qty").get(i).getString());
        }
        conn.run("clear!(t)");
    }

    @Test
    public void test_BasicDecimal_AutoFitTableUpsert_DimensionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,\"Huya\" \"Tonghuashun\" \"maoyan\");" +
                "t=keyedTable(`sym,1000:0,`sym`datetime`price`qty,[STRING,DATETIME,DECIMAL32(2),DECIMAL64(4)]);" +
                "pt = db.createTable(t,`pt);" +
                "pt.append!(t)";
        conn.run(script);
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("sym");
        colNames.add("datetime");
        colNames.add("price");
        colNames.add("qty");
        BasicStringVector bsv = new BasicStringVector(new String[]{"Huya","Tonghuashun","maoyan"});
        cols.add(bsv);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{17,989,9000});
        cols.add(bdtv);
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,2);
        bd32v.set(0,new BasicDecimal32(11,2));
        bd32v.set(1,new BasicDecimal32(19,2));
        bd32v.set(2,new BasicDecimal32(23,2));
        cols.add(bd32v);
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(3,4);
        bd64v.set(0,new BasicDecimal64(25,4));
        bd64v.set(1,new BasicDecimal64(49,4));
        bd64v.set(2,new BasicDecimal64(14,4));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        String[] keyCols = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,keyCols,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("price").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("qty").get(i).getString());
        }
        conn.run("clear!(t)");
    }
    @Test
    public void test_BasicDecimal128_AutoFitTableUpsert_indexedTable() throws Exception {
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
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
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
    public void test_BasicDecimal128_AutoFitTableUpsert_KeyTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t=keyedTable(`sym,1:0,`sym`datetime`col1`col2,[STRING,DATETIME,DECIMAL128(0),DECIMAL128(37)])";
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
        bd32v.set(0,new BasicDecimal128("1.0001",0));
        bd32v.set(1,new BasicDecimal128("1.09",0));
        bd32v.set(2,new BasicDecimal128("-23.22",0));
        cols.add(bd32v);
        BasicDecimal128Vector bd64v = new BasicDecimal128Vector(3,37);
        bd64v.set(0,new BasicDecimal128("-2.0009",37));
        bd64v.set(1,new BasicDecimal128("4.99999999999999999999999999999999",37));
        bd64v.set(2,new BasicDecimal128("0.00000000000000000000000000000001",37));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","t",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from t;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("col1").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("col2").get(i).getString());
        }
        conn.run("clear!(t)");
    }

    @Test
    public void test_BasicDecimal128_AutoFitTableUpsert_PartitionedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,\"Huya\" \"Tonghuashun\" \"maoyan\");" +
                "t=keyedTable(`sym,1000:0,`sym`datetime`col1`col2,[STRING,DATETIME,DECIMAL128(0),DECIMAL128(37)]);" +
                "pt = db.createPartitionedTable(t,`pt,`sym);" +
                "pt.append!(t)";
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
        bd32v.set(0,new BasicDecimal128("1.0001",0));
        bd32v.set(1,new BasicDecimal128("1.09",0));
        bd32v.set(2,new BasicDecimal128("-23.22",0));
        cols.add(bd32v);
        BasicDecimal128Vector bd64v = new BasicDecimal128Vector(3,37);
        bd64v.set(0,new BasicDecimal128("-2.0009",37));
        bd64v.set(1,new BasicDecimal128("4.99999999999999999999999999999999",37));
        bd64v.set(2,new BasicDecimal128("0.00000000000000000000000000000001",37));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        String[] keyCols = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,keyCols,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("col1").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("col2").get(i).getString());
        }
        conn.run("clear!(t)");
    }

    @Test
    public void test_BasicDecimal128_AutoFitTableUpsert_DimensionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,\"Huya\" \"Tonghuashun\" \"maoyan\");" +
                "t=keyedTable(`sym,1000:0,`sym`datetime`col1`col2,[STRING,DATETIME,DECIMAL128(0),DECIMAL128(37)]);" +
                "pt = db.createTable(t,`pt);" +
                "pt.append!(t)";
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
        bd32v.set(0,new BasicDecimal128("1.0001",0));
        bd32v.set(1,new BasicDecimal128("1.09",0));
        bd32v.set(2,new BasicDecimal128("-23.22",0));
        cols.add(bd32v);
        BasicDecimal128Vector bd64v = new BasicDecimal128Vector(3,37);
        bd64v.set(0,new BasicDecimal128("-2.0009",37));
        bd64v.set(1,new BasicDecimal128("4.99999999999999999999999999999999",37));
        bd64v.set(2,new BasicDecimal128("0.00000000000000000000000000000001",37));
        cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        String[] keyCols = new String[]{"sym"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,keyCols,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        for (int i = 0; i < 3; i++) {
            assertEquals(bsv.get(i),ua.getColumn("sym").get(i));
            assertEquals(bdtv.get(i),ua.getColumn("datetime").get(i));
            assertEquals(bd32v.get(i).getString(),ua.getColumn("col1").get(i).getString());
            assertEquals(bd64v.get(i).getString(),ua.getColumn("col2").get(i).getString());
        }
        conn.run("clear!(t)");
    }
    @Test
    public void test_AutoFitTableUpsert_ArrayVector_decimal() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector","pt",conn,true,new String[]{"cint"},null);

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
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void test_AutoFitTableUpsert_ArrayVector_decimal_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn = new DBConnection(false,false,true);
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector","pt",conn,true,new String[]{"cint"},null);

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
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void test_AutoFitTableUpsert_ArrayVector_decimal128() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector","pt",conn,true,new String[]{"cint"},null);

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
        Vector v128=new BasicDecimal128Vector(3,0);
        v128.set(0,new BasicDecimal128("151285.00",0));
        v128.set(1,new BasicDecimal128("24635.00001",0));
        v128.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v128);
        bdvcol0.add(1,v128);
        bdvcol0.add(2,v128);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v1281=new BasicDecimal128Vector(3,4);
        v1281.set(0,new BasicDecimal128("151285.00",4));
        v1281.set(1,new BasicDecimal128("24635.00001",4));
        v1281.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol1.add(0,v1281);
        bdvcol1.add(1,v1281);
        bdvcol1.add(2,v1281);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v1280=new BasicDecimal128Vector(3,0);
        v1280.set(0,new BasicDecimal128("151285.00",0));
        v1280.set(1,new BasicDecimal128("24635.00001",0));
        v1280.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol2.add(0,v1280);
        bdvcol2.add(1,v1280);
        bdvcol2.add(2,v1280);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v1282=new BasicDecimal128Vector(3,4);
        v1282.set(0,new BasicDecimal128("151285.00",4));
        v1282.set(1,new BasicDecimal128("24635.00001",4));
        v1282.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol3.add(0,v1282);
        bdvcol3.add(1,v1282);
        bdvcol3.add(2,v1282);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v1283=new BasicDecimal128Vector(3,8);
        v1283.set(0,new BasicDecimal128("151285.00",8));
        v1283.set(1,new BasicDecimal128("24635.00001",8));
        v1283.set(2,new BasicDecimal128("24635.00001",8));
        bdvcol4.add(0,v1283);
        bdvcol4.add(1,v1283);
        bdvcol4.add(2,v1283);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v1280.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v1282.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v1283.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void test_AutoFitTableUpsert_ArrayVector_decimal128_compress_true() throws Exception {
        String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                "    dropDatabase(\"dfs://testArrayVector\")\n" +
                "}\n" +
                "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(8)[]])\n" +
                "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
        conn = new DBConnection(false,false,true);
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector","pt",conn,true,new String[]{"cint"},null);

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
        Vector v128=new BasicDecimal128Vector(3,0);
        v128.set(0,new BasicDecimal128("151285.00",0));
        v128.set(1,new BasicDecimal128("24635.00001",0));
        v128.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol0.add(0,v128);
        bdvcol0.add(1,v128);
        bdvcol0.add(2,v128);
        BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0);
        cols.add(bavcol0);
        List<Vector> bdvcol1 = new ArrayList<Vector>();
        Vector v1281=new BasicDecimal128Vector(3,4);
        v1281.set(0,new BasicDecimal128("151285.00",4));
        v1281.set(1,new BasicDecimal128("24635.00001",4));
        v1281.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol1.add(0,v1281);
        bdvcol1.add(1,v1281);
        bdvcol1.add(2,v1281);
        BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1);
        cols.add(bavcol1);
        List<Vector> bdvcol2 = new ArrayList<Vector>();
        Vector v1280=new BasicDecimal128Vector(3,0);
        v1280.set(0,new BasicDecimal128("151285.00",0));
        v1280.set(1,new BasicDecimal128("24635.00001",0));
        v1280.set(2,new BasicDecimal128("24635.00001",0));
        bdvcol2.add(0,v1280);
        bdvcol2.add(1,v1280);
        bdvcol2.add(2,v1280);
        BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2);
        cols.add(bavcol2);
        List<Vector> bdvcol3 = new ArrayList<Vector>();
        Vector v1284=new BasicDecimal128Vector(3,4);
        v1284.set(0,new BasicDecimal128("151285.00",4));
        v1284.set(1,new BasicDecimal128("24635.00001",4));
        v1284.set(2,new BasicDecimal128("24635.00001",4));
        bdvcol3.add(0,v1284);
        bdvcol3.add(1,v1284);
        bdvcol3.add(2,v1284);
        BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3);
        cols.add(bavcol3);
        List<Vector> bdvcol4 = new ArrayList<Vector>();
        Vector v1282=new BasicDecimal128Vector(3,8);
        v1282.set(0,new BasicDecimal128("151285.00",8));
        v1282.set(1,new BasicDecimal128("24635.00001",8));
        v1282.set(2,new BasicDecimal128("24635.00001",8));
        bdvcol4.add(0,v1282);
        bdvcol4.add(1,v1282);
        bdvcol4.add(2,v1282);
        BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4);
        cols.add(bavcol4);

        BasicTable bt = new BasicTable(colNames,cols);
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
        assertEquals(3,res.rows());
        assertEquals(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getVectorValue(0).getString());
        assertEquals(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getVectorValue(0).getString());
        assertEquals(v1280.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getVectorValue(0).getString());
        assertEquals(v1281.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getVectorValue(0).getString());
        assertEquals(v1282.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getVectorValue(0).getString());
    }
    @Test
    public void test_AutoFitTableUpsert_AlmostAllDataType_indexedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,10)\n" +
                "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "share t as st;";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)\n" +
                " t2;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","st",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(3,ua.rows());
        assertEquals(0,conn.run("select * from st where cminute = 10:15m").rows());
        assertEquals(1,conn.run("select * from st where cminute = 15:27m").rows());
        BasicTable ua1 = (BasicTable) conn.run("select cdecimal128 from st where cint = 9");
        assertEquals("27.0000000000",ua1.getColumn(0).get(0).getString());
        conn.run("undef(`st.SHARED)");
        conn.run("clear!(t)");
    }

    @Test
    public void test_AutoFitTableUpsert_AlmostAllDataType_keyedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,10)\n" +
                "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "share t as st;";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)\n" +
                " t2;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("","st",conn,true,null,null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from st;");
        assertEquals(3,ua.rows());
        assertEquals(0,conn.run("select * from st where cminute = 10:15m").rows());
        assertEquals(1,conn.run("select * from st where cminute = 15:27m").rows());
        BasicTable ua1 = (BasicTable) conn.run("select cdecimal128 from st where cint = 9");
        assertEquals("27.0000000000",ua1.getColumn(0).get(0).getString());
        conn.run("undef(`st.SHARED)");
        conn.run("clear!(t)");
    }

    @Test
    public void test_AutoFitTableUpsert_AlmostAllDatatype_PartitionedTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,10)\n" +
                "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,1..10);" +
                "pt = db.createPartitionedTable(t,`pt,`cint);" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)\n" +
                " t2;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,new String[]{"cint"},null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(3,ua.rows());
        assertEquals(0,conn.run("select * from pt where cminute = 10:15m").rows());
        assertEquals(1,conn.run("select * from pt where cminute = 15:27m").rows());
        BasicTable ua1 = (BasicTable) conn.run("select cdecimal128 from pt where cint = 9");
        assertEquals("27.0000000000",ua1.getColumn(0).get(0).getString());
    }

    @Test
    public void test_AutoFitTableUpsert_AlmostAllDatatype_DimensionTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "cbool = true false false;\n" +
                "cchar = 'a' 'b' 'c';\n" +
                "cshort = 122h 32h 45h;\n" +
                "cint = 1 4 9;\n" +
                "clong = 17l 39l 72l;\n" +
                "cdate = 2013.06.13 2015.07.12 2019.08.15;\n" +
                "cmonth = 2011.08M 2014.02M 2019.07M;\n" +
                "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n" +
                "cminute = 03:25m 08:12m 10:15m;\n" +
                "csecond = 01:15:20 04:26:45 09:22:59;\n" +
                "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n" +
                "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n" +
                "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n" +
                "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n" +
                "cfloat = 7.5f 0.79f 8.27f;\n" +
                "cdouble = 5.7 7.2 3.9;\n" +
                "cstring = \"hello\" \"hi\" \"here\";\n" +
                "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n" +
                "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n" +
                "cdecimal32 = decimal32(12 17 135.2,2)\n" +
                "cdecimal64 = decimal64(18 24 33.878,4)\n" +
                "cdecimal128 = decimal128(18 24 33.878,10)\n" +
                "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute," +
                "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble," +
                "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);" +
                "if(existsDatabase(\"dfs://testDecimal\")){" +
                "dropDatabase(\"dfs://testDecimal\")}" +
                "db = database(\"dfs://testDecimal\",VALUE,1..10);" +
                "pt = db.createTable(t,`pt);" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t2 = table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)\n" +
                " t2;");
        AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testDecimal","pt",conn,true,new String[]{"cint"},null);
        aftu.upsert(bt);
        BasicTable ua = (BasicTable) conn.run("select * from pt;");
        assertEquals(3,ua.rows());
        assertEquals(0,conn.run("select * from pt where cminute = 10:15m").rows());
        assertEquals(1,conn.run("select * from pt where cminute = 15:27m").rows());
        BasicTable ua1 = (BasicTable) conn.run("select cdecimal128 from pt where cint = 9");
        assertEquals("27.0000000000",ua1.getColumn(0).get(0).getString());
    }
}
