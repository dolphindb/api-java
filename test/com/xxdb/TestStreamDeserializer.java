package com.xxdb;

import com.xxdb.data.Vector;
import com.xxdb.data.*;
import com.xxdb.streaming.client.*;

import org.javatuples.Pair;
import org.junit.Assert;

import java.util.*;

import static com.xxdb.TestStreamDeserializer.count;
import static com.xxdb.TestStreamDeserializer.streamConn;

public class TestStreamDeserializer {
    public static String SERVER = "192.168.1.116";
    public static int PORT = 18999;
    public static String LOCALHOST = "localhost";
    public static int LOCALPORT = 8676;
    public static DBConnection streamConn = new DBConnection();
    public static int count = 0;

    public static void testStreamDeserializer() throws Exception{
        DBConnection conn = new DBConnection();
        conn.connect(SERVER, PORT, "admin", "123456");
        streamConn = new DBConnection();
        streamConn.connect(SERVER, PORT, "admin", "123456");
        try
        {
            conn.run("dropStreamTable(`outTables)");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                "enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
        conn.run(script);

        String replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);

        BasicTable table1 = (BasicTable)conn.run("table1");
        BasicTable table2 = (BasicTable)conn.run("table2");


        //tablename
        Map<String, Pair<String, String>> tables = new HashMap<>();
        tables.put("msg1", new Pair<>("table1", ""));
        tables.put("msg2", new Pair<>("table2", ""));
        StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);


        Handler8 handler = new Handler8(streamFilter);
//        Handler7 handler7 = new Handler7();
        ThreadedClient client = new ThreadedClient(LOCALHOST, LOCALPORT);
        client.subscribe(SERVER, PORT, "outTables", "mutiSchema", handler, 0, true, null, false, 1000, 1, "admin", "123456");

        Thread.sleep(5000);
        List<BasicMessage> msg1 = handler.getMsg1();
        List<BasicMessage> msg2 = handler.getMsg2();
        client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema");
        Assert.assertEquals(table1.rows(), msg1.size());
        Assert.assertEquals(table2.rows(), msg2.size());
        for (int i = 0; i < table1.columns(); ++i)
        {
            Vector tableCol = table1.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg1.get(j).getEntity(i));
            }
        }
        for (int i = 0; i < table2.columns(); ++i)
        {
            Vector tableCol = table2.getColumn(i);
            for (int j = 0; j < 10000; ++j)
            {
                Assert.assertEquals(tableCol.get(j), msg2.get(j).getEntity(i));
            }
        }

        client.close();
        conn.close();
        streamConn.close();
    }

    public static void main(String[] args) throws Exception{
        testStreamDeserializer();
    }
}

class Handler1 implements MessageHandler
{

    public void batchHandler(List<IMessage> msgs)
    {
        throw new RuntimeException();
    }

    public void doEvent(IMessage msg)
    {
        try {
            msg.getEntity(0);
            String script = String.format("insert into sub1 values({%s},{%s},\"{%s}\",{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s} )",
                    msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(),
                    msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(),
                    msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(),
                    msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(),
                    msg.getEntity(12).getString());
            streamConn.run(script);
        }
        catch (Exception e) {
            System.out.println("Error 3");
            e.printStackTrace();
        }
    }
}

class Handler2 implements MessageHandler {
    public void batchHandler(List<IMessage> msgs) {
        throw new RuntimeException();
    }

    public void doEvent(IMessage msg) {
        try {
            msg.getEntity(0);
            String script = String.format("insert into sub2 values({%s},{%s},\"{%s}\",{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s} )",
                    msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(),
                    msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(),
                    msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(),
                    msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(),
                    msg.getEntity(12).getString());
        streamConn.run(script);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Handler3 implements MessageHandler {
    public void batchHandler(List<IMessage> msgs) {
        throw new RuntimeException();
    }

    public void doEvent(IMessage msg) {
        try {
            msg.getEntity(0);
            String script = String.format("insert into sub3 values({%s},{%s},\"{%s}\",{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s},{%s} )",
                    msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(),
                    msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(),
                    msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(),
                    msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(),
                    msg.getEntity(12).getString());
            streamConn.run(script);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

class Handler4 implements MessageHandler {
    private List<BasicMessage> msg1 = new ArrayList<>();
    private List<BasicMessage> msg2 = new ArrayList<>();


    public void batchHandler(List<IMessage> msgs)
    {
        throw new RuntimeException();
    }

    public void doEvent(IMessage msg)
    {
        try
        {
            if (((BasicMessage)msg).getSym().equals("msg1"))
            {
                msg1.add((BasicMessage)msg);
            }
            else if (((BasicMessage)msg).getSym().equals("msg2"))
            {
                msg2.add((BasicMessage)msg);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public List<BasicMessage> getMsg1()
    {
        return msg1;
    }

    public List<BasicMessage> getMsg2()
    {
        return msg2;
    }
}

class Handler5 implements MessageHandler {
    public void batchHandler(List<IMessage> msgs) {
        try {
            int rows = msgs.size();
            BasicIntVector col1v = new BasicIntVector(rows);
            BasicTimestampVector col2v = new BasicTimestampVector(rows);
            BasicSymbolVector col3v = new BasicSymbolVector(rows);
            BasicDoubleVector col4v = new BasicDoubleVector(rows);
            BasicDoubleVector col5v = new BasicDoubleVector(rows);
            BasicDoubleVector col6v = new BasicDoubleVector(rows);
            BasicDoubleVector col7v = new BasicDoubleVector(rows);
            BasicDoubleVector col8v = new BasicDoubleVector(rows);
            BasicIntVector col9v = new BasicIntVector(rows);
            BasicIntVector col10v = new BasicIntVector(rows);
            BasicIntVector col11v = new BasicIntVector(rows);
            BasicIntVector col12v = new BasicIntVector(rows);
            BasicIntVector col13v = new BasicIntVector(rows);

            for (int i = 0; i < rows; ++i)
            {
                col1v.set(i, (Scalar)msgs.get(i).getEntity(0));
                col2v.set(i, (Scalar)msgs.get(i).getEntity(1));
                col3v.set(i, (Scalar)msgs.get(i).getEntity(2));
                col4v.set(i, (Scalar)msgs.get(i).getEntity(3));
                col5v.set(i, (Scalar)msgs.get(i).getEntity(4));
                col6v.set(i, (Scalar)msgs.get(i).getEntity(5));
                col7v.set(i, (Scalar)msgs.get(i).getEntity(6));
                col8v.set(i, (Scalar)msgs.get(i).getEntity(7));
                col9v.set(i, (Scalar)msgs.get(i).getEntity(8));
                col10v.set(i, (Scalar)msgs.get(i).getEntity(9));
                col11v.set(i, (Scalar)msgs.get(i).getEntity(10));
                col12v.set(i, (Scalar)msgs.get(i).getEntity(11));
                col13v.set(i, (Scalar)msgs.get(i).getEntity(12));
            }
            List<String> colNames = Arrays.asList("permno", "timestamp", "ticker", "price1", "price2", "price3", "price4",
                    "price5", "vol1", "vol2", "vol3", "vol4", "vol5");
            List<Vector> cols = Arrays.asList(col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v);
            BasicTable tmp = new BasicTable(colNames, cols);
            List<Entity> value = new ArrayList<Entity>(){{add(tmp);}};
            streamConn.run("append!{sub1}", value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doEvent(IMessage msg) {
        throw new RuntimeException();
    }
}

class Handler6 implements MessageHandler {
    private StreamDeserializer deserializer_;
    private List<BasicMessage> msg1 = new ArrayList<>();
    private List<BasicMessage> msg2 = new ArrayList<>();

    public Handler6(StreamDeserializer deserializer) {
        deserializer_ = deserializer;
    }

    public void batchHandler(List<IMessage> msgs) {
        System.out.println(count++);
        for (int i = 0; i < msgs.size(); i++){
            try {
                BasicMessage message = deserializer_.parse(msgs.get(i));
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void doEvent(IMessage msg) {
        try {
            System.out.println(count++);
            BasicMessage message = deserializer_.parse(msg);
            if (message.getSym().equals("msg1")) {
                msg1.add(message);
            } else if (message.getSym().equals("msg2")) {
                msg2.add(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<BasicMessage> getMsg1() {
        return msg1;
    }

    public List<BasicMessage> getMsg2() {
        return msg2;
    }
}

class Handler8 implements BatchMessageHandler {
    private StreamDeserializer deserializer_;
    private List<BasicMessage> msg1 = new ArrayList<>();
    private List<BasicMessage> msg2 = new ArrayList<>();

    public Handler8(StreamDeserializer deserializer) {
        deserializer_ = deserializer;
    }

    public void batchHandler(List<IMessage> msgs) {
        System.out.println(count++);
        for (int i = 0; i < msgs.size(); i++) {
            try {
                BasicMessage message = deserializer_.parse(msgs.get(i));
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void doEvent(IMessage msg) {
        try {
            System.out.println(count++);
            BasicMessage message = deserializer_.parse(msg);
            if (message.getSym().equals("msg1")) {
                msg1.add(message);
            } else if (message.getSym().equals("msg2")) {
                msg2.add(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<BasicMessage> getMsg1() {
        return msg1;
    }

    public List<BasicMessage> getMsg2() {
        return msg2;
    }
}

class Handler7 implements MessageHandler {
    private List<BasicMessage> msg1 = new ArrayList<>();
    private List<BasicMessage> msg2 = new ArrayList<>();


    public void batchHandler(List<IMessage> msgs) {
        throw new RuntimeException();
    }

    public void doEvent(IMessage msg) {
        try {
            if (((BasicMessage)msg).getSym() == "msg1") {
                msg1.add((BasicMessage)msg);
            } else if (((BasicMessage)msg).getSym() == "msg2") {
                msg2.add((BasicMessage)msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<BasicMessage> getMsg1() {
        return msg1;
    }

    public List<BasicMessage> getMsg2() {
        return msg2;
    }
}


