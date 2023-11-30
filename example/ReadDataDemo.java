package com.dolphindb.examples;

import com.xxdb.*;
import com.xxdb.data.*;
import com.xxdb.io.ProgressListener;
import java.io.IOException;

public class ReadDataDemo {

    private static DBConnection conn = new DBConnection();
    private static String hostname = "localhost";
    private static int port = 8848;
    private static String username = "admin";
    private static String password = "123456";

    static {
        try {
            conn.connect(hostname, port, username, password);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        basicTableRead();
        batchRead();
    }

    public static void basicTableRead() throws IOException {

        conn.run("t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])\n" +
                "share t as sharedTable");
        conn.run(String.format("insert into sharedTable values(['IBM', 100, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['GOOG', 200, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['YHOO', 300, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['MSFT', 400, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['FB', 500, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['HP', 600, 2012.06.13 13:30:10.008, 3.14])"));
        conn.run(String.format("insert into sharedTable values(['IBM', 700, 2012.06.13 13:30:10.008, 3.14])"));

        BasicTable table = (BasicTable) conn.run("select * from sharedTable");

        // get a column by index start at 0
        Vector firstColumn = table.getColumn(0);
        System.out.println("firstColumn: " + firstColumn.getString());
        Vector secondColumn = table.getColumn(1);
        System.out.println("secondColumn: " + secondColumn.getString());

        // get a column by column name
        Vector cstringColumn = table.getColumn("cstring");
        System.out.println("cstringColumn: " + cstringColumn.getString());

        // get the string
        String tableString = table.getString();
        System.out.println("tableString: \n" + tableString);

        // get the table slices
        Table subTableRange = table.getSubTable(0, 1);
        System.out.println("subTableRange: \n" + subTableRange.getString());

    }

    public static void batchRead() throws IOException {

        EntityBlockReader v = (EntityBlockReader)conn.run("table(1..22486 as id)",(ProgressListener) null,4,4,10000);
        BasicTable data = (BasicTable)v.read();
        while(v.hasNext()){
            BasicTable t = (BasicTable)v.read();
            data = data.combine(t);
        }
        System.out.println(data.getString());
    }

}
