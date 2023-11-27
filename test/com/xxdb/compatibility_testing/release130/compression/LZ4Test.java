package com.xxdb.compatibility_testing.release130.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class LZ4Test {

    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\tstopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)\n" +
                "}");
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
    }

    @Before
    public void setUp() {
        conn = new DBConnection(false, false, true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
            clear_env();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testString() {
        BasicTable obj = null;
        try {
            obj = (BasicTable) conn.run("table(second(1..10240) as id, take(`hi`hello`hola, 10240) as value)");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < obj.rows(); i++) {
//            System.out.println(obj.getRowJson(i));
        }
    }

    @Test
    public void testLZ4(){
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteBuffer buffer = ByteBuffer.allocate(100000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 1; i <= 10000; ++i){
            buffer.putInt(i);
        }
        byte[] ret = compressor.compress(buffer.array(), 0, 4*10240);
        int a= 1;
    }

    @Test
    public void testcompressTable() throws IOException {
        BasicTable obj = null;
        try {
            obj = (BasicTable) conn.run("table(1..10000 as id)");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < obj.rows(); i++) {
//            System.out.println(obj.getRowJson(i));
        }
        conn.run("t = table(100000:0,[`id],[INT])" +
                "share t as st");
        List<String> colNames = new ArrayList<>();
        colNames.add("id");
        List<Entity> args = Arrays.asList(obj);
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        BasicTable newT = (BasicTable) conn.run("select * from st");
    }

    @Test
    public void testCompressVector() throws IOException {
        conn.run("t = table(100000:0,[`id],[INT])" +
                "share t as st");
        conn.run("t = array(INT, 0)");
        List<Entity> args = new ArrayList<>();
        BasicIntVector obj = (BasicIntVector)conn.run("1..10000");
        args.add(obj);
        BasicIntVector count = (BasicIntVector) conn.run("append!{t}", args);
        BasicTable newT = (BasicTable) conn.run("select * from st");
    }

    @Test
    public void testReadBigData1() throws IOException {
        conn.run("n = 100000000;" +
                "t = table(1..n as a,take(`aaaaaaa,n) as b,take(2021.01.01,n) as c)"+
                "share t as st");
        BasicTable newT = (BasicTable) conn.run("select * from st");
        assertEquals(100000000,newT.rows());
        clear_env();
    }

    @Test
    public void testReadBigData2() throws IOException {
        conn.run("n = 140000000;" +
                "t = table(1..n as a,take(`aaaaaaa,n) as b,take(2021.01.01,n) as c)"+
                "share t as st");
        BasicTable newT = (BasicTable) conn.run("select * from st");
        assertEquals(140000000,newT.rows());
        clear_env();
    }


    @After
    public void tearDown() throws Exception {
        conn.close();
    }

}
