package com.dolphindb.examples;

import com.xxdb.*;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.route.AutoFitTableUpsert;
import java.util.*;

/**
 * ATFU(upsert) allows you to insert data idempotently, but the cost is quite high.
 * DO NOT use it frequently.
 */
public class AutoFitTableUpsertDemo {
    public static void main(String[] args) throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect("localhost", 8848, "admin", "123456");
        String dbName ="dfs://upsertTable";
        String tableName = "pt";
        String script = "dbName = \"dfs://upsertTable\"\n"+
                "if(exists(dbName)){\n"+
                "\tdropDatabase(dbName)\t\n"+
                "}\n"+
                "db  = database(dbName, RANGE,1 10000,,'TSDB')\n"+
                "t = table(1000:0, `id`value,[ INT, DOUBLE])\n"+
                "pt = db.createPartitionedTable(t,`pt,`id,,`id)";
        conn.run(script);

        BasicIntVector v1 = new BasicIntVector(3);
        v1.setInt(0, 1);
        v1.setInt(1, 100);
        v1.setInt(2, 1000);

        BasicDoubleVector v2 = new BasicDoubleVector(3);
        v2.setDouble(0, 100.0);
        v2.setDouble(1, 110.0);
        v2.setDouble(2, 108.0);

        List<String> colNames = new ArrayList<>();
        colNames.add("id");
        colNames.add("value");
        List<Vector> cols = new ArrayList<>();
        cols.add(v1);
        cols.add(v2);
        BasicTable bt = new BasicTable(colNames, cols);
        String[] keyColName = new String[]{"id"};
        AutoFitTableUpsert aftu = new AutoFitTableUpsert(dbName, tableName, conn, false, keyColName, null);
        aftu.upsert(bt);
        BasicTable res = (BasicTable) conn.run("select * from pt;");
        System.out.println(res.getString());

        //第2次插入pt，会根据id去重，存在则更新，否则插入
        BasicIntVector v3 = new BasicIntVector(3);
        v3.setInt(0, 1);
        v3.setInt(1, 1001);
        v3.setInt(2, 1002);

        BasicDoubleVector v4 = new BasicDoubleVector(3);
        v4.setDouble(0, 120.0);
        v4.setDouble(1, 130.0);
        v4.setDouble(2, 140.0);

        cols.set(0, v3);
        cols.set(1, v4);
        bt.setColumns(cols);

        aftu.upsert(bt);
        BasicTable res2 = (BasicTable) conn.run("select * from pt;");
        System.out.println(res2.getString());
    }
}

