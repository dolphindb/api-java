package com.dolphindb.examples;
import com.xxdb.*;
import com.xxdb.data.BasicTable;
import java.io.IOException;

/**
 * insert into can only be applied to in-memory table
 */
public class InsertIntoDemo {
    public static void main(String[] args) throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect("localhost", 8848, "admin", "123456");
        conn.run("t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])\n" +
                "share t as sharedTable");
        conn.run(String.format("insert into sharedTable values(['IBM', 100, 2012.06.13 13:30:10.008, 3.14])"));
        BasicTable basicTable = (BasicTable) conn.run("select * from sharedTable");
        System.out.println(basicTable.getString());
    }
}
