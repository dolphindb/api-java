package com.xxdb;

import com.xxdb.data.BasicDoubleVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class PythonParserTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static DBConnection conn_python_parser;

    @Before
    public void setUp() throws IOException {
        conn_python_parser = new DBConnection(false,false,false,true);
        try {
            if (!conn_python_parser.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server python parser");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn_python_parser.close();
    }

    @Test
    public void test_connect_support_python_parser_import() throws IOException {
        conn_python_parser.run("import pandas");
    }

    @Test
    public void test_connect_support_python_parser_upload() throws IOException {
        Map<String, Entity> vars = new HashMap<String, Entity>();
        BasicDoubleVector vec = new BasicDoubleVector(3);
        vec.setDouble(0, 1.5);
        vec.setDouble(1, 2.5);
        vec.setDouble(2, 7);
        vars.put("a",vec);
        conn_python_parser.upload(vars);
        Entity result = conn_python_parser.run("a");
        assertEquals("[1.5,2.5,7]",result.getString());
        Entity result2 = conn_python_parser.run("type(a)");
        assertEquals("dolphindb.VECTOR.DOUBLE",result2.getString());
    }

    @Test
    public void test_connect_support_python_parser_run() throws IOException {
        conn_python_parser.run("a = [1,2,3,4,5]");
        Entity result2 = conn_python_parser.run("type(a)");
        assertEquals("list",result2.getString());
        Entity result = conn_python_parser.run("a.pop()");
        assertEquals("5",result.getString());
    }

    @Test
    public void test_connect_support_python_parser_insert_into () throws IOException {
        conn_python_parser.run("import dolphindb as ddb\n" +
                "t = table(pair(10000,0),[\"cstring\",\"cint\",\"cdouble\"].toddb(),[ddb.STRING,ddb.INT,ddb.DOUBLE].toddb());\n"+
                "share t as sharedTable");
        conn_python_parser.run(String.format("insert into sharedTable values('%s',%s,%s)","a",1,1.1));
        BasicTable result = (BasicTable)conn_python_parser.run("select * from sharedTable");
        assertEquals("[a]",result.getColumn(0).getString());
        assertEquals("[1]",result.getColumn(1).getString());
        assertEquals("[1.1]",result.getColumn(2).getString());
    }

    @Test
    public void test_connect_support_python_parser_read_list() throws IOException {
        conn_python_parser.run("import pandas as pd\n" +
                "a = pd.DataFrame({\"int\": [1, 2, 3], \"char\": [1, 2, 3]}, [11,22,33], False)");
        Entity result =conn_python_parser.run("a");
        System.out.println(result);
    }

}