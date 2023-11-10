package com.dolphindb.examples;
import com.xxdb.*;
import com.xxdb.data.BasicStringVector;
import java.io.IOException;

public class DBConnectDemo {

    private static String hostname = "localhost";
    private static int port = 8848;
    private static String username = "admin";
    private static String password = "123456";


    private static void simpleDBConnectionDemo() throws IOException {
        DBConnection conn = new DBConnection();
        boolean success = conn.connect("localhost", 8848);
        BasicStringVector vector = (BasicStringVector) conn.run("`IBM`GOOG`YHOO");
        int size = vector.rows();
        for (int i = 0; i < size; ++i) {
            System.out.println(vector.getString(i));
        }
    }

    private static void DBConnectionPoolDemo() throws IOException {
        ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(hostname, port, username, password, 10, false, false);
        BasicDBTask task = new BasicDBTask("1+1");
        pool.execute(task);
        System.out.println(task.getResult());
        pool.waitForThreadCompletion();
        pool.shutdown();
    }

    public static void main(String[] args) throws IOException {
        simpleDBConnectionDemo();
        DBConnectionPoolDemo();
    }
}
