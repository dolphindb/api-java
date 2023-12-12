package examples;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 *  when use tableInsert, be caution that insertedRows are equals to your input's size.
 *
 */

public class TableInsertDemo {
    private static DBConnection conn;
    public static String host = "localhost";
    public static Integer port = 8848;

    public static void main(String[] args) throws Exception {
        conn = new DBConnection();
        conn.connect(host, port);
        new TableInsertDemo().writeDfsTable();
    }

    private BasicTable createBasicTable() {
        List<String> colNames = new ArrayList<String>();
        colNames.add("cint");
        colNames.add("cdate");
        colNames.add("cstring");
        List<Vector> cols = new ArrayList<Vector>() {
        };

        //cint
        int[] vint = new int[]{2147483647, 483647};
        BasicIntVector bintv = new BasicIntVector(vint);
        cols.add(bintv);

        //cdate
        int[] vdate = new int[]{Utils.countDays(LocalDate.of(2018, 2, 14)), Utils.countDays(LocalDate.of(2018, 8, 15))};
        BasicDateVector bdatev = new BasicDateVector(vdate);
        cols.add(bdatev);

        //cstring
        String[] vstring = new String[]{"", "test string"};
        BasicStringVector bstringv = new BasicStringVector(vstring);
        cols.add(bstringv);
        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }

    public void writeDfsTable() throws IOException {
        BasicTable table1 = createBasicTable();
        conn.login("admin", "123456", false);
        conn.run("t = table(10000:0,`cint`cdate`cstring,[INT,DATE,STRING])\n");
        conn.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
        conn.run("db = database('dfs://testDatabase',RANGE,2018.01.01..2018.12.31)");
        conn.run("db.createPartitionedTable(t,'tb1','cdate')");
        conn.run("def saveData(data){ loadTable('dfs://testDatabase','tb1').tableInsert(data)}");
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("saveData", args);
        BasicTable dt = (BasicTable) conn.run("select * from loadTable('dfs://testDatabase','tb1')");
        System.out.println(dt.getString());
        if (dt.rows() != 2) {
            System.out.println("failed");
        }
    }
}