package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.PollingClient;
import com.xxdb.streaming.client.TopicPoller;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

public class BasicDecimalTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    private static DBConnection conn;

    public void clear_env() throws IOException {
        conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
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
    @Test
    public void test_BasicDecimal_readMemoryTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "t = table(1:0,`a`b`c,[INT,DECIMAL32(2),DECIMAL64(4)]);\n"+
                "t.append!(table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c));\n";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("t;");
        BasicDecimal32 bd32 = new BasicDecimal32(0,4);
        //BasicDecimal64 bd64 = new BasicDecimal64(0,7L);
        System.out.println(bd32.getDataType());
        //assertEquals(bd64,bt.getColumn(2).get(0));
        //assertEquals(bd32,bt.getColumn(1).get(0));
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn(1).get(0).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn(2).get(0).getDataType());
    }

    @Test
    public void test_BasicDecimal_readPartitionTable() throws IOException{
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script1 = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "pt = db.createPartitionedTable(t,`pt,`id);\n" +
                "pt.append!(t);\n" +
                "pt = loadTable(\"dfs://testDecimal\",\"pt\");";
        conn.run(script1);
        BasicTable bt = (BasicTable) conn.run("select top 100 * from pt;");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
        assertEquals(100,bt.rows());
    }

    @Test
    public void test_BasicDecimal_readDimensionTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))\n" +
                "if(existsDatabase(\"dfs://testDecimal\")){dropDatabase(\"dfs://testDecimal\")}\n" +
                "db=database(\"dfs://testDecimal\",VALUE,1..100);\n" +
                "dt = db.createTable(t,`dt);\n" +
                "dt.append!(t);" +
                "dt = loadTable(\"dfs://testDecimal\",\"dt\");";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select top 50 * from dt;");
        assertEquals(50,bt.rows());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,bt.getColumn("a").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,bt.getColumn("b").getDataType());
    }

    @Test
    public void test_BasicDecimal_readStreamingTable() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        clear_env();
        PollingClient client = new PollingClient(HOST,10086);
        conn.run("st2 = streamTable(5000:0,`tag`ts`data,[INT,DECIMAL32(2),DECIMAL64(4)])\n" +
                "enableTableShareAndPersistence(table=st2, tableName=`Trades, asynWrite=true, compress=true, " +
                "cacheSize=2000, retentionMinutes=180)\t\n");
        conn.run("n = 1000;\n" +
                "t = table(n:0,`id`a`b,[INT,DECIMAL32(2),DECIMAL64(4)]);\n" +
                "t.append!(table(take(1..100,n) as id,rand(100,n) as a,take(101..500,n) as b))");
        BasicTable bt = (BasicTable) conn.run("t;");
        System.out.println(bt.getString());
        conn.run("Trades.append!(t);");
        TopicPoller poller = client.subscribe(HOST,PORT,"Trades","subTrades");
        ArrayList<IMessage> msg;
        conn.run("n=1000;" +
                "t=table(take(1..100,n) as tag,rand(100,n) as ts,take(101..200,n) as data);" +
                "Trades.append!(t);");
        msg = poller.poll(100,5000);
        assertEquals(1000,msg.size());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,msg.get(1).getEntity(1).getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,msg.get(1).getEntity(2).getDataType());
        client.unsubscribe(HOST,PORT,"Trades","subTrades");
        conn.run("dropStreamTable(`Trades);");
        clear_env();
        conn.close();
        client.close();
    }

    @Test
    public void test_BasicDecimal_normalWriteMemoryTable() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("it=table([1,2,3] as a,[4,5,6] as b,[7,8,9] as c)");
        BasicTable bt = (BasicTable) conn.run("it");
        conn.run("t = table(3:0,`a`b`c,[INT,DECIMAL32(2),DECIMAL64(4)]);" +
                "share t as st");
        List<Entity> args = Arrays.asList(bt);
        conn.run("tableInsert{st}",args);
        BasicTable res = (BasicTable) conn.run("t;");
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32,res.getColumn("b").getDataType());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64,res.getColumn("c").getDataType());
        assertEquals(3,res.rows());
    }
}
