package com.xxdb.compatibility_testing.release130.data;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicTableSchemaTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/compatibility_testing/release130/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(HOST,PORT,"admin","123456")){
                throw new IOException("Failed to connect to 2xdb server");
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }

    @Test
    public void test_BasicTableSchemaTest_row() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        assertEquals(100,tableSchema.rows());
    }

    @Test
    public void test_BasicTableSchemaTest_column() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        assertEquals(2,tableSchema.columns());
    }

    @Test
    public void test_BasicTableSchemaTest_getDataForm() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        assertEquals("DF_TABLE",tableSchema.getDataForm().toString());
    }

    @Test
    public void test_BasicTableSchemaTest_getDataCategory() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        assertEquals(null,tableSchema.getDataCategory());
    }

    @Test
    public void test_BasicTableSchemaTest_getDataType() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        assertEquals(null,tableSchema.getDataType());
    }

    @Test
    public void test_BasicTableSchemaTest_toBasicTable() throws IOException {
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        BasicIntVector ex_0 = (BasicIntVector) conn.run("exec a from ts");
        for(int i = 0;i<100;i++) {
            assertEquals(ex_0.getString(i), tableSchema.toBasicTable().getColumn(0).getString(i));
        }
        BasicStringVector ex_1 = (BasicStringVector) conn.run("exec b from ts");
        for(int i = 0;i<100;i++) {
            assertEquals(ex_1.getString(i), tableSchema.toBasicTable().getColumn(1).getString(i));
        }
    }

    @Test(expected = ClassCastException.class)
    public void test_BasicTableSchemaTest_run_null_tableName() throws IOException {
        DBConnection conn2 = new DBConnection(false,false,false);
        conn2.connect(HOST,PORT,"admin","123456");
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts");
    }

    @Test
    public void test_BasicTableSchemaTest_run_tableName_not_same() throws IOException {
        DBConnection conn2 = new DBConnection(false,false,false);
        conn2.connect(HOST,PORT,"admin","123456");
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        conn.run("ts2 = table(take(`a`b`c,50) as a1)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts2","ts");

        assertEquals(50,tableSchema.rows());
        assertEquals(1,tableSchema.columns());
        BasicStringVector ex_1 = (BasicStringVector) conn.run("exec a1 from ts2");
        for(int i = 0;i<50;i++) {
            assertEquals(ex_1.getString(i), tableSchema.toBasicTable().getColumn(0).getString(i));
        }
    }

    @Test
    public void test_BasicTableSchemaTest_setDBConnection_getString() throws IOException {
        DBConnection conn2 = new DBConnection(false,false,false);
        conn2.connect(HOST,PORT,"admin","123456");
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");
        System.out.println(tableSchema.getString());
    }

    @Test
    public void test_BasicTableSchemaTest_get_partition_table() throws IOException {
        DBConnection conn2 = new DBConnection(false,false,false);
        conn2.connect(HOST,PORT,"admin","123456");
        conn.run("dbName = \"dfs://test_amdQuote\"\n" +
                "if(existsDatabase(dbName)){\n" +
                "\tdropDB(dbName)\t\n" +
                "}\n" +
                "db = database(\"dfs://test_amdQuote\",HASH,[INT,4])\n" +
                "tmp_table = table(1..10 as id,take(`a`b`c`d,10) as sym,double(1..10) as price)\n" +
                "pt = createPartitionedTable(db,tmp_table,`pt,`id)\n" +
                "pt.append!(tmp_table)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from pt", "ts");

        assertEquals(10,tableSchema.rows());
        assertEquals(3,tableSchema.columns());
        BasicIntVector ex_1 = (BasicIntVector) conn.run("exec id from pt");
        for(int i = 0;i<10;i++) {
            assertEquals(ex_1.getString(i), tableSchema.toBasicTable().getColumn(0).getString(i));
        }

        BasicStringVector ex_2 = (BasicStringVector) conn.run("exec sym from pt");
        for(int i = 0;i<10;i++) {
            assertEquals(ex_2.getString(i), tableSchema.toBasicTable().getColumn(1).getString(i));
        }

        BasicDoubleVector ex_3 = (BasicDoubleVector) conn.run("exec price from pt");
        for(int i = 0;i<10;i++) {
            assertEquals(ex_3.getString(i), tableSchema.toBasicTable().getColumn(2).getString(i));
        }
    }

    @Test
    public void test_BasicTableSchemaTest_setDBConnection_other_session2() throws IOException {
        DBConnection conn2 = new DBConnection(false,false,false);
        conn2.connect(HOST,PORT,"admin","123456");
        conn2.run("ts = table(1 2 3 as a1)");
        conn.run("ts = table(1..100 as a,take(`a`b`c,100) as b)");
        BasicTableSchema tableSchema = (BasicTableSchema) conn.run("select * from ts", "ts");

        BasicIntVector ex_3 = (BasicIntVector) conn2.run("exec a1 from ts");
        for(int i = 0;i<3;i++) {
            assertEquals(ex_3.getString(i), tableSchema.toBasicTable().getColumn(0).getString(i));
        }
        tableSchema.write(null);
    }


}
