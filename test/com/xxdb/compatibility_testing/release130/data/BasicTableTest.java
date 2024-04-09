package com.xxdb.compatibility_testing.release130.data;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;
import static org.junit.Assert.*;

public class BasicTableTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    public static String HOST = bundle.getString("HOST");
    public static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test(timeout = 70000)
    public void test_BasicTable_GetSubTable_DFS() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n=10000000;\n" +
                "bool = take(true false,n);\n" +
                "char = take('a'..'z',n);\n" +
                "short = take(1h..255h,n);\n" +
                "int = take(1..2000,n);\n" +
                "long = take(2000l..5000l,n);\n" +
                "date = take(2012.01.01..2016.12.31,n);\n" +
                "month = take(2012.01M..2021.12M,n);\n" +
                "time = take(01:01:01.001..23:59:59.999,n);\n" +
                "minute = take(01:01m..23:59m,n);\n" +
                "second = take(01:01:01..23:59:59,n);\n" +
                "datetime = take(2022.10.14 01:01:01..2022.10.14 23:59:59,n);\n" +
                "timestamp = take(2022.10.14 01:01:01.001..2022.10.14 23:59:59.999,n);\n" +
                "nanotime = take(14:00:00.000000001..14:00:00.199999999,n);\n" +
                "nanotimestamp = take(2022.10.14 13:39:51.000000001..2022.10.14 13:39:51.199999999,n);\n" +
                "float = rand(33.2f,n);\n" +
                "double = rand(53.1,n);\n" +
                "string = take(\"orcl\" \"APPL\" \"AMZON\" \"GOOG\",n)\n" +
                "datehour = take(datehour(2011.01.01 01:01:01..2011.12.31 23:59:59),n)\n" +
                "t = table(bool,char,short,int,long,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp,float,double,string,datehour);\n" +
                "if(existsDatabase(\"dfs://testSubTable\")){dropDatabase(\"dfs://testSubTable\")}\n" +
                "db = database(\"dfs://testSubTable\",VALUE,1..2000);\n" +
                "pt = db.createPartitionedTable(t,`pt,`int);\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from pt;");
        assertEquals(10000000,bt.rows());
        Table gs = bt.getSubTable(0,9999999);
        System.out.println(gs.getString());
    }
}
