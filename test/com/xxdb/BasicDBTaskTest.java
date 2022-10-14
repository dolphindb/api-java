package com.xxdb;

import com.xxdb.comm.ErrorCodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ResourceBundle;
import java.util.logging.Logger;

public class BasicDBTaskTest {
    private Logger logger_ = Logger.getLogger(getClass().getName());
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    public static Integer insertTime = 5000;
    public static ErrorCodeInfo pErrorInfo =new ErrorCodeInfo();

    @Test
    public void test_BasicDBTask()throws Exception{
        conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicDBTask bt = new BasicDBTask("nanotimestamp(\"jagjagjagajhg\")");
        bt.setDBConnection(conn);
        bt.call();
    }
}
