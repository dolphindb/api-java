package com.xxdb.streaming.client.cep;

import com.xxdb.DBConnection;
import com.xxdb.Prepare;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.xxdb.Prepare.*;
import static com.xxdb.data.Entity.DATA_FORM.*;
import static com.xxdb.data.Entity.DATA_TYPE.*;
import static com.xxdb.streaming.client.cep.EventSenderTest.*;

public class EventClientTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    static int GROUP_ID = Integer.parseInt(bundle.getString("GROUP_ID"));
    static EventClient client = null;
    static EventSender sender = null;


    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        clear_env();
    }

    @After
    public  void after() throws IOException, InterruptedException {
        conn.close();
        try{client.unsubscribe(HOST, PORT, "inputTable", "test1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "intput", "test1");}catch (Exception ex){}
    }

    public static  EventMessageHandler handler = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attribute) {
            System.out.println("eventType: ");
            System.out.println("eventType: " + eventType);
            System.out.println(attribute.toString());
            try {
                conn.run("tableInsert{outputTable}", attribute);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    public static  EventMessageHandler handler1 = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attribute) {
            System.out.println("eventType: " + eventType);
            System.out.println(attribute.toString());
            System.out.println(eventType.equals("MarketData"));
            if(eventType.equals("MarketData") ){
                try {
                    conn.run("tableInsert{outputTable}", attribute);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }else{
                try {
                    conn.run("tableInsert{outputTable1}", attribute);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };
    @Test
    public  void test_EventClient_EventScheme_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient  client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventClient_EventType_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient  client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventClient_EventType_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventType must be non-empty.",re);
    }
    @Test
    public  void test_EventClient_EventType_repetition() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market");
        scheme1.setFieldNames(Arrays.asList("market1", "time1", "decimal32", "decimal64", "decimal128"));
        scheme1.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Collections.singletonList("market");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventType must be unique.",re);
    }

    @Test
    public  void test_EventClient_AttrKeys_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        //scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventKey in eventSchema must be non-empty.",re);
    }

    @Test
    public  void test_EventClient_AttrKeys_repetition() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("time","time"));
        scheme.setFieldTypes(Arrays.asList(DT_TIME,DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR,DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("time");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventSchema cannot has duplicated attrKey in attrKeys.",re);
    }

    @Test
    public  void test_EventClient_AttrKeys_one_colume() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, [`timestamp], [TIMESTAMP]) as outputTable;\n"+
                "class MarketData{\n"+
                "timestamp :: TIMESTAMP\n"+
                "def MarketData(t){\n"+
                "timestamp = t\n"+
                "}\n"+
                "}\n"+
                "class MainMonitor{\n"+
                "def MainMonitor(){}\n"+
                "def updateMarketData(event)\n"+
                "def onload(){addEventListener(updateMarketData,'MarketData',,'all')}\n"+
                "def updateMarketData(event){emitEvent(event)}\n"+
                "}\n"+
                "dummy = table(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs);\n"+
                "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput\n"+
                "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n"+
                "insert into schema values(\"MarketData\", \"timestamp\", \"TIMESTAMP\", 12, 0)\n"+
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\")\n";
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp"));
        scheme.setFieldTypes(Arrays.asList(DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("timestamp");
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "intput", "test1", handler, -1, true, "admin", "123456");
        conn.run("marketData1 = MarketData(now());\n appendEvent(inputSerializer, [marketData1])");
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
        BasicTable re1 = (BasicTable)conn.run("select timestamp from intput");
        checkData(re1, re);
        client.unsubscribe(HOST, PORT, "intput", "test1");
    }

    @Test
    public  void test_EventClient_AttrTypes_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        //scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }

    @Test
    public  void test_EventClient_AttrForms_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }

    @Test
    public  void test_EventClient_attrExtraParams_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    }

    @Test
    public  void test_EventClient_attrExtraParams_set_not_true() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList( DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList( 10, 19, 39));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale 10 is out of bounds, it must be in [0,9].",re);

        scheme.setFieldExtraParams(Arrays.asList( 1, 19, 39));
        String re1 = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale 19 is out of bounds, it must be in [0,18].",re1);

        scheme.setFieldExtraParams(Arrays.asList( 1, 18, 39));
        String re2 = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re2 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL128 scale 39 is out of bounds, it must be in [0,38].",re2);

        scheme.setFieldExtraParams(Arrays.asList( -1, 10, 10));
        String re3 = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re3 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale -1 is out of bounds, it must be in [0,9].",re3);

        scheme.setFieldExtraParams(Arrays.asList( 1, -1, 0));
        String re4 = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re4 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale -1 is out of bounds, it must be in [0,18].",re4);

        scheme.setFieldExtraParams(Arrays.asList( 0, 0, -1));
        String re5 = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re5 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL128 scale -1 is out of bounds, it must be in [0,38].",re5);
    }

    @Test
    public  void test_EventClient_eventTimeKeys_not_exist() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Event market doesn't contain eventTimeKey datetimev.",re);
    }
    @Test
    public  void test_EventClient_eventTimeKeys_two_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        String script = "share streamTable(1:0, `timestamp`time, [TIMESTAMP,TIME]) as outputTable;\n"+
                "share streamTable(1:0, `string`timestamp, [STRING,TIMESTAMP]) as outputTable1;\n"+
                "class MarketData{\n"+
                "timestamp :: TIMESTAMP\n"+
                "time :: TIME\n"+
                "def MarketData(t,t1){\n"+
                "timestamp = t\n"+
                "time = t1\n"+
                "}\n"+
                "}\n"+
                "class MarketData1{\n"+
                "string :: STRING\n"+
                "timestamp :: TIMESTAMP\n"+
                "def MarketData1(s,t){\n"+
                "string = s\n"+
                "timestamp = t\n"+
                "}\n"+
                "}\n"+
                "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput\n"+
                "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n"+
                "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n"+
                "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n"+
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\")\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp", "time"));
        scheme.setFieldTypes(Arrays.asList( DT_TIMESTAMP,DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("MarketData1");
        scheme1.setFieldNames(Arrays.asList("string", "timestamp"));
        scheme1.setFieldTypes(Arrays.asList(DT_STRING, DT_TIMESTAMP));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "timestamp"});
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        client.subscribe(HOST, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
        conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
        BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

        Assert.assertEquals(1,re.rows());
        Assert.assertEquals(1,re1.rows());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re.getColumn(0).get(0).getString());
        Assert.assertEquals("00:00:00.001",re.getColumn(1).get(0).getString());
        Assert.assertEquals("tesrtttt",re1.getColumn(0).get(0).getString());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re1.getColumn(1).get(0).getString());
        client.unsubscribe(HOST, PORT, "intput", "test1");
    }
    @Test
    public  void test_EventClient_commonKeys_one_column() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `timestamp`time`commonKey, [TIMESTAMP,TIME,TIMESTAMP]) as outputTable;\n"+
                "share streamTable(1:0, `string`timestamp`commonKey, [STRING,TIMESTAMP,TIMESTAMP]) as outputTable1;\n"+
                "class MarketData{\n"+
                "timestamp :: TIMESTAMP\n"+
                "time :: TIME\n"+
                "def MarketData(t,t1){\n"+
                "timestamp = t\n"+
                "time = t1\n"+
                "}\n"+
                "}\n"+
                "class MarketData1{\n"+
                "string :: STRING\n"+
                "timestamp :: TIMESTAMP\n"+
                "def MarketData1(s,t){\n"+
                "string = s\n"+
                "timestamp = t\n"+
                "}\n"+
                "}\n"+
                "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(TIMESTAMP, 0) as commonKey) as intput\n"+
                "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n"+
                "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n"+
                "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n"+
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\", commonField = \"timestamp\")\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp", "time"));
        scheme.setFieldTypes(Arrays.asList( DT_TIMESTAMP,DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("MarketData1");
        scheme1.setFieldNames(Arrays.asList("string", "timestamp"));
        scheme1.setFieldTypes(Arrays.asList(DT_STRING, DT_TIMESTAMP));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"timestamp"});
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        client.subscribe(HOST, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
        conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
        BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

        Assert.assertEquals(1,re.rows());
        Assert.assertEquals(1,re1.rows());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re.getColumn(0).get(0).getString());
        Assert.assertEquals("00:00:00.001",re.getColumn(1).get(0).getString());
        Assert.assertEquals("tesrtttt",re1.getColumn(0).get(0).getString());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re1.getColumn(1).get(0).getString());
        client.unsubscribe(HOST, PORT, "intput", "test1");
    }

    @Test
    public  void test_EventClient_commonKeys_two_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("market", "time"));
        scheme1.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeKeys, commonKeys);
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("123456"));
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market1", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(2,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(2).get(0).getString());
        Assert.assertEquals("12:45:03.100",re.getColumn(2).get(1).getString());
        Assert.assertEquals("123456",re.getColumn(3).get(0).getString());
        Assert.assertEquals("tesrtrrr",re.getColumn(3).get(1).getString());
    }
    public static void subscribePrepare() throws IOException {
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp", "comment1"));
        scheme.setFieldTypes(Arrays.asList( DT_TIMESTAMP,DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"comment1"});
        sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
        client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    }
    @Test
    public  void test_EventClient_subscribe_host_null() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(null, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
    }
    @Test
    public  void test_EventClient_subscribe_host_not_true() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe("erer", PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
    }
    @Test
    public  void test_EventClient_subscribe_port_0() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, 0, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
    }

    @Test
    public  void test_EventClient_subscribe_port_not_true() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, 18888, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
    }

    @Test
    public  void test_EventClient_subscribe_tableName_not_exist() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable111", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("The shared table inputTable111 doesn't exist."));
    }

    @Test
    public  void test_EventClient_subscribe_tableName_null() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, PORT, null, "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventClient subscribe 'tableName' param cannot be null or empty.",re);
        String re1 = null;
        try{
            client.subscribe(HOST, PORT, "", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("EventClient subscribe 'tableName' param cannot be null or empty.",re1);
    }

    @Test
    public  void test_EventClient_subscribe_actionName_exist() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable1;");
        String re = null;
        client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler1, -1, true, "admin", "123456");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable1", "test1");
    }
    @Test
    public  void test_EventClient_subscribe_actionName_null() throws IOException, InterruptedException {
        subscribePrepare();
        client.subscribe(HOST, PORT, "inputTable", null, handler1, -1, true, "admin", "123456");
        client.unsubscribe(HOST, PORT, "inputTable",null);
    }
    @Test
    public  void test_EventClient_subscribe_handler_null() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        client.subscribe(HOST, PORT, "inputTable", "test1", null, -1, true, "admin", "123456");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test
    public  void test_EventClient_subscribe_offset_negative_1() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test
    public  void test_EventClient_subscribe_offset_negative_2() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -2, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test
    public  void test_EventClient_subscribe_offset_0() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, 0, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(2,re.rows());
    }

    @Test
    public  void test_EventClient_subscribe_offset_1() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, 1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_offset_not_match() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable", "test1", handler, 2, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("Can't find the message with offset"));
    }
    @Test
    public  void test_EventClient_subscribe_reconnect_true() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, false, "admin", "123456");
        Thread.sleep(1000);
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_subscribe_reconnect_false() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        Thread.sleep(1000);
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_subscribe_user_error() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin123", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("The user name or password is incorrect"));
    }


    @Test
    public  void test_EventClient_subscribe_password_error() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456WWW");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("The user name or password is incorrect"));
    }

    @Test
    public  void test_EventClient_subscribe_admin() throws IOException, InterruptedException {
        PrepareUser("user1","123456");
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT,"user1","123456");
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
        conn.run("addAccessControl(`inputTable)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        subscribePrepare();

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_other_user() throws IOException, InterruptedException {
        PrepareUser("user1","123456");
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT,"user1","123456");
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
        conn.run("addAccessControl(`inputTable)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        subscribePrepare();

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_other_user_unallow() throws IOException, InterruptedException {
        PrepareUser("user1","123456");
        PrepareUser("user2","123456");
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT,"user1","123456");
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
        conn.run("addAccessControl(`inputTable)");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp", "comment1"));
        scheme.setFieldTypes(Arrays.asList( DT_TIMESTAMP,DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"comment1"});
        sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
        client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "user2", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("No access to shared table [inputTable]"));
    }
    @Test
    public  void test_EventClient_subscribe_unsubscribe_resubscribe() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        for(int i=0;i<10;i++) {
            client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            client.unsubscribe(HOST, PORT, "inputTable", "test1");
            client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            client.unsubscribe(HOST, PORT, "inputTable", "test1");
        }
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(20,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_duplicated() throws IOException, InterruptedException {
        subscribePrepare();
        client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("already be subscribed"));
    }
    @Test(expected = NullPointerException.class)
    public  void test_EventClient_not_subscribe_unsubscribe() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_unsubscribe_duplicated() throws IOException, InterruptedException {
        subscribePrepare();
        client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_subscribe_haStreamTable() throws IOException, InterruptedException {
        conn.run("table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
        conn.run("haStreamTable(11, table, `inputTable, 100000)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        subscribePrepare();

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_haStreamTable_leader() throws IOException, InterruptedException {
        BasicString StreamLeaderTmp = (BasicString)conn.run(String.format("getStreamingLeader(%d)", GROUP_ID));
        String StreamLeader = StreamLeaderTmp.getString();
        BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"%s\")[0]", StreamLeader));
        String StreamLeaderHost = StreamLeaderHostTmp.getString();
        BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.format("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and  name=\"%s\")[0]", StreamLeader));
        int StreamLeaderPort = StreamLeaderPortTmp.getInt();
        System.out.println(StreamLeaderHost);
        System.out.println(StreamLeaderPort);
        DBConnection conn1 = new DBConnection();
        conn1.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
        String script = "try{\ndropStreamTable(`inputTable)\n}catch(ex){\n}\n"+
            "table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]);\n"+
            "haStreamTable("+GROUP_ID+", table, `inputTable, 100000);\n"+
            "share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;;\n";
        conn1.run(script);
        subscribePrepare();

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(StreamLeaderHost, StreamLeaderPort, "inputTable", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn1.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("2024.03.22T10:45:03.100",re.getColumn(0).get(0).getString());
        Assert.assertEquals("123456",re.getColumn(1).get(0).getString());
        client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "inputTable", "test1");
    }

    @Test//not support
    public  void test_EventClient_subscribe_haStreamTable_follower() throws IOException, InterruptedException {
        String script0 ="leader = getStreamingLeader("+GROUP_ID+");\n" +
                "groupSitesStr = (exec sites from getStreamingRaftGroups() where id =="+GROUP_ID+")[0];\n"+
                "groupSites = split(groupSitesStr, \",\");\n"+
                "followerInfo = exec top 1 *  from rpc(getControllerAlias(), getClusterPerf) where site in groupSites and name!=leader;";
        conn.run(script0);
        BasicString StreamFollowerHostTmp = (BasicString)conn.run("(exec host from followerInfo)[0]");
        String StreamFollowerHost = StreamFollowerHostTmp.getString();
        BasicInt StreamFollowerPortTmp = (BasicInt)conn.run("(exec port from followerInfo)[0]");
        int StreamFollowerPort = StreamFollowerPortTmp.getInt();
        System.out.println(StreamFollowerHost);
        System.out.println(StreamFollowerPort);
        DBConnection conn1 = new DBConnection();
        conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
        String script = "try{\ndropStreamTable(`inputTable)\n}catch(ex){\n}\n"+
                "table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]);\n"+
                "haStreamTable("+GROUP_ID+", table, `inputTable, 100000);\n"+
                "share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;\n";
        conn1.run(script);

        subscribePrepare();
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(StreamFollowerHost, StreamFollowerPort, "inputTable", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn1.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("2024.03.22T10:45:03.100",re.getColumn(0).get(0).getString());
        Assert.assertEquals("123456",re.getColumn(1).get(0).getString());
        client.unsubscribe(StreamFollowerHost, StreamFollowerPort, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_subscribe_all_dateType_1() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `eventTime`eventType`blobs, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);
        String script1 = "class event_all_dateType{\n" +
                "\tboolv :: BOOL\n" +
                "\tcharv :: CHAR\n" +
                "\tshortv :: SHORT\n" +
                "\tintv :: INT\n" +
                "\tlongv :: LONG\n" +
                "\tdoublev :: DOUBLE \n" +
                "\tfloatv :: FLOAT\n" +
                "\tdatev :: DATE\n" +
                "\tmonthv :: MONTH\n" +
                "\ttimev :: TIME\n" +
                "\tminutev :: MINUTE\n" +
                "\tsecondv :: SECOND\n" +
                "\tdatetimev :: DATETIME \n" +
                "\ttimestampv :: TIMESTAMP\n" +
                "\tnanotimev :: NANOTIME\n" +
                "\tnanotimestampv :: NANOTIMESTAMP\n" +
                //"\tsymbolv :: SYMBOL\n" +
                "\tstringv :: STRING\n" +
                "\tdatehourv :: DATEHOUR\n" +
                "\tuuidv :: UUID\n" +
                "\tippaddrv :: IPADDR \n" +
                "\tint128v :: INT128\n" +
                "\tblobv :: BLOB\n" +
                "\tpointv :: POINT\n" +
                "\tcomplexv :: COMPLEX\n" +
                "\tdecimal32v :: DECIMAL32(3)\n" +
                "\tdecimal64v :: DECIMAL64(8)\n" +
                "\tdecimal128v :: DECIMAL128(10) \n" +
                "  def event_all_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp,  string, datehour, uuid, ippaddr, int128, blob,point, complex, decimal32, decimal64, decimal128){\n" +
                "\tboolv = bool\n" +
                "\tcharv = char\n" +
                "\tshortv = short\n" +
                "\tintv = int\n" +
                "\tlongv = long\n" +
                "\tdoublev = double\n" +
                "\tfloatv = float\n" +
                "\tdatev = date\n" +
                "\tmonthv = month\n" +
                "\ttimev = time\n" +
                "\tminutev = minute\n" +
                "\tsecondv = second\n" +
                "\tdatetimev = datetime\n" +
                "\ttimestampv = timestamp\n" +
                "\tnanotimev = nanotime\n" +
                "\tnanotimestampv = nanotimestamp\n" +
                //"\tsymbolv = symbol\n" +
                "\tstringv = string\n" +
                "\tdatehourv = datehour\n" +
                "\tuuidv = uuid\n" +
                "\tippaddrv = ippaddr\n" +
                "\tint128v = int128\n" +
                "\tblobv = blob\n" +
                "\tpointv = point\n" +
                "\tcomplexv = complex\n" +
                "\tdecimal32v = decimal32\n" +
                "\tdecimal64v = decimal64\n" +
                "\tdecimal128v = decimal128\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_dateType'\n" +
                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,stringv,datehourv,uuidv,ippaddrv,int128v,blobv,pointv,complexv,decimal32v,decimal64v,decimal128v';\n" +
                "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)];\n" +
                "formV = [SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");" ;
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv",  "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR,  DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "intput", "test1", handler, -1, true, "admin", "123456");

        Preparedata(1);
        String script2 = "data1=select * from data;\n" +
                "i = 0\n" +
                "\tall_data_type1=event_all_dateType(data1.row(i)[`boolv], data1.row(i)[`charv], data1.row(i)[`shortv], data1.row(i)[`intv],data1.row(i)[`longv], data1.row(i)[`doublev], data1.row(i)[`floatv], data1.row(i)[`datev],data1.row(i)[`monthv], data1.row(i)[`timev], data1.row(i)[`minutev], data1.row(i)[`secondv],data1.row(i)[`datetimev], data1.row(i)[`timestampv], data1.row(i)[`nanotimev], data1.row(i)[`nanotimestampv], data1.row(i)[`stringv], data1.row(i)[`datehourv], data1.row(i)[`uuidv], data1.row(i)[`ippaddrv],data1.row(i)[`int128v], blob(data1.row(i)[`blobv]), data1.row(i)[`pointv], data1.row(i)[`complexv], data1.row(i)[`decimal32v], data1.row(i)[`decimal64v], data1.row(i)[`decimal128v])\n" +
                "\tappendEvent(inputSerializer, all_data_type1)\n" ;
        conn.run(script2);
        Thread.sleep(10000);
        BasicTable bt1 = (BasicTable)conn.run("select * from data;");
        Assert.assertEquals(1,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(1,bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventClient_subscribe_all_dateType_100() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `eventTime`eventType`blobs, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);
        String script1 = "class event_all_dateType{\n" +
                "\tboolv :: BOOL\n" +
                "\tcharv :: CHAR\n" +
                "\tshortv :: SHORT\n" +
                "\tintv :: INT\n" +
                "\tlongv :: LONG\n" +
                "\tdoublev :: DOUBLE \n" +
                "\tfloatv :: FLOAT\n" +
                "\tdatev :: DATE\n" +
                "\tmonthv :: MONTH\n" +
                "\ttimev :: TIME\n" +
                "\tminutev :: MINUTE\n" +
                "\tsecondv :: SECOND\n" +
                "\tdatetimev :: DATETIME \n" +
                "\ttimestampv :: TIMESTAMP\n" +
                "\tnanotimev :: NANOTIME\n" +
                "\tnanotimestampv :: NANOTIMESTAMP\n" +
                //"\tsymbolv :: SYMBOL\n" +
                "\tstringv :: STRING\n" +
                "\tdatehourv :: DATEHOUR\n" +
                "\tuuidv :: UUID\n" +
                "\tippaddrv :: IPADDR \n" +
                "\tint128v :: INT128\n" +
                "\tblobv :: BLOB\n" +
                "\tpointv :: POINT\n" +
                "\tcomplexv :: COMPLEX\n" +
                "\tdecimal32v :: DECIMAL32(3)\n" +
                "\tdecimal64v :: DECIMAL64(8)\n" +
                "\tdecimal128v :: DECIMAL128(10) \n" +
                "  def event_all_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp,  string, datehour, uuid, ippaddr, int128, blob,point, complex, decimal32, decimal64, decimal128){\n" +
                "\tboolv = bool\n" +
                "\tcharv = char\n" +
                "\tshortv = short\n" +
                "\tintv = int\n" +
                "\tlongv = long\n" +
                "\tdoublev = double\n" +
                "\tfloatv = float\n" +
                "\tdatev = date\n" +
                "\tmonthv = month\n" +
                "\ttimev = time\n" +
                "\tminutev = minute\n" +
                "\tsecondv = second\n" +
                "\tdatetimev = datetime\n" +
                "\ttimestampv = timestamp\n" +
                "\tnanotimev = nanotime\n" +
                "\tnanotimestampv = nanotimestamp\n" +
                //"\tsymbolv = symbol\n" +
                "\tstringv = string\n" +
                "\tdatehourv = datehour\n" +
                "\tuuidv = uuid\n" +
                "\tippaddrv = ippaddr\n" +
                "\tint128v = int128\n" +
                "\tblobv = blob\n" +
                "\tpointv = point\n" +
                "\tcomplexv = complex\n" +
                "\tdecimal32v = decimal32\n" +
                "\tdecimal64v = decimal64\n" +
                "\tdecimal128v = decimal128\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_dateType'\n" +
                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,stringv,datehourv,uuidv,ippaddrv,int128v,blobv,pointv,complexv,decimal32v,decimal64v,decimal128v';\n" +
                "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)];\n" +
                "formV = [SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");" ;
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR,  DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,  3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "intput", "test1", handler, -1, true, "admin", "123456");

        Preparedata(100);
        String script2 = "data1=select * from data;\n" +
                "for(i in 0..99){\n" +
                "\tall_data_type1=event_all_dateType(data1.row(i)[`boolv], data1.row(i)[`charv], data1.row(i)[`shortv], data1.row(i)[`intv],data1.row(i)[`longv], data1.row(i)[`doublev], data1.row(i)[`floatv], data1.row(i)[`datev],data1.row(i)[`monthv], data1.row(i)[`timev], data1.row(i)[`minutev], data1.row(i)[`secondv],data1.row(i)[`datetimev], data1.row(i)[`timestampv], data1.row(i)[`nanotimev], data1.row(i)[`nanotimestampv],data1.row(i)[`stringv], data1.row(i)[`datehourv], data1.row(i)[`uuidv], data1.row(i)[`ippaddrv],data1.row(i)[`int128v], blob(data1.row(i)[`blobv]), data1.row(i)[`pointv], data1.row(i)[`complexv], data1.row(i)[`decimal32v], data1.row(i)[`decimal64v], data1.row(i)[`decimal128v])\n" +
                "\tappendEvent(inputSerializer, all_data_type1)\n" +
                "\t}" ;
                conn.run(script2);
        Thread.sleep(10000);
        BasicTable bt1 = (BasicTable)conn.run("select * from data;");
        Assert.assertEquals(100,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(100,bt2.rows());
        checkData(bt1,bt2);
    }

//    @Test
//    public  void test_EventClient_all_dateType_vector() throws IOException, InterruptedException {
//        Preparedata_array(100,10);
//        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
//                "colNames=\"col\"+string(1..25);\n" +
//                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
//                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
//        conn.run(script);
//        String script1 ="class event_all_array_dateType{\n" +
//                "\tboolv :: BOOL VECTOR\n" +
//                "\tcharv :: CHAR VECTOR\n" +
//                "\tshortv :: SHORT VECTOR\n" +
//                "\tintv :: INT VECTOR\n" +
//                "\tlongv :: LONG VECTOR\n" +
//                "\tdoublev :: DOUBLE  VECTOR\n" +
//                "\tfloatv :: FLOAT VECTOR\n" +
//                "\tdatev :: DATE VECTOR\n" +
//                "\tmonthv :: MONTH VECTOR\n" +
//                "\ttimev :: TIME VECTOR\n" +
//                "\tminutev :: MINUTE VECTOR\n" +
//                "\tsecondv :: SECOND VECTOR\n" +
//                "\tdatetimev :: DATETIME VECTOR \n" +
//                "\ttimestampv :: TIMESTAMP VECTOR\n" +
//                "\tnanotimev :: NANOTIME VECTOR\n" +
//                "\tnanotimestampv :: NANOTIMESTAMP VECTOR\n" +
//                "\t//stringv :: STRING VECTOR\n" +
//                "\tdatehourv :: DATEHOUR VECTOR\n" +
//                "\tuuidv :: UUID VECTOR\n" +
//                "\tippaddrv :: IPADDR  VECTOR\n" +
//                "\tint128v :: INT128 VECTOR\n" +
//                "\t//blobv :: BLOB VECTOR\n" +
//                "\tpointv :: POINT VECTOR\n" +
//                "\tcomplexv :: COMPLEX VECTOR\n" +
//                "\tdecimal32v :: DECIMAL32(3)  VECTOR\n" +
//                "\tdecimal64v :: DECIMAL64(8) VECTOR\n" +
//                "\tdecimal128v :: DECIMAL128(10) VECTOR \n" +
//                "  def event_all_array_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, datehour, uuid, ippaddr, int128,point, complex, decimal32, decimal64, decimal128){\n" +
//                "\tboolv = bool\n" +
//                "\tcharv = char\n" +
//                "\tshortv = short\n" +
//                "\tintv = int\n" +
//                "\tlongv = long\n" +
//                "\tdoublev = double\n" +
//                "\tfloatv = float\n" +
//                "\tdatev = date\n" +
//                "\tmonthv = month\n" +
//                "\ttimev = time\n" +
//                "\tminutev = minute\n" +
//                "\tsecondv = second\n" +
//                "\tdatetimev = datetime\n" +
//                "\ttimestampv = timestamp\n" +
//                "\tnanotimev = nanotime\n" +
//                "\tnanotimestampv = nanotimestamp\n" +
//                "\t//stringv = string\n" +
//                "\tdatehourv = datehour\n" +
//                "\tuuidv = uuid\n" +
//                "\tippaddrv = ippaddr\n" +
//                "\tint128v = int128\n" +
//                "\t//blobv = blob\n" +
//                "\tpointv = point\n" +
//                "\tcomplexv = complex\n" +
//                "\tdecimal32v = decimal32\n" +
//                "\tdecimal64v = decimal64\n" +
//                "\tdecimal128v = decimal128\n" +
//                "  \t}\n" +
//                "}   \n" +
//                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
//                "eventType = 'event_all_dateType'\n" +
//                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,datehourv,uuidv,ippaddrv,int128v,pointv,complexv,decimal32v,decimal64v,decimal128v';\n" +
//                "typeV = [BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[],MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], UUID[], IPADDR[], INT128[], POINT[], COMPLEX[], DECIMAL32(3)[], DECIMAL64(8)[], DECIMAL128(10)[]];\n" +
//                "formV = [VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR];\n" +
//                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
//                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
//                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
//                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
//        conn.run(script1);
//        EventSchema scheme = new EventSchema();
//        scheme.setEventType("event_all_array_dateType");
//        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv",  "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
//        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
//        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
//        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
//        List<String> eventTimeKeys = new ArrayList<>();
//        List<String> commonKeys = new ArrayList<>();
//        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
//        sender.connect(conn,"inputTable");
//
//        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
//        client.subscribe(HOST, PORT, "intput1", "test1", handler, -1, true, "admin", "123456");
//
//        Preparedata_array(100,10);
//        BasicTable bt = (BasicTable)conn.run("select * from data");
//        String script2 = "data1=select * from data;\n" +
//                "for(i in 0..9){\n" +
//                "\tevent_all_array_dateType1=event_all_array_dateType(data1.row(i)[`cbool], data1.row(i)[`cchar], data1.row(i)[`cshort], data1.row(i)[`cint],data1.row(i)[`clong], data1.row(i)[`cdouble], data1.row(i)[`cfloat], data1.row(i)[`cdate], data1.row(i)[`cmonth], data1.row(i)[`ctime], data1.row(i)[`cminute], data1.row(i)[`csecond], data1.row(i)[`cdatetime], data1.row(i)[`ctimestamp], data1.row(i)[`cnanotime], data1.row(i)[`cnanotimestamp], data1.row(i)[`cdatehour], data1.row(i)[`cuuid], data1.row(i)[`cipaddr], data1.row(i)[`cint128],data1.row(i)[`cpoint], data1.row(i)[`ccomplex], data1.row(i)[`cdecimal32], data1.row(i)[`cdecimal64], data1.row(i)[`cdecimal128])\n" +
//                "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
//                "\t}" ;
//        conn.run(script2);
//        Thread.sleep(10000);
//        for(int i=0;i<bt.rows();i++){
//            List<Entity> attributes = new ArrayList<>();
//            for(int j=0;j<bt.columns();j++){
//                Entity pt = bt.getColumn(j).get(i);
//                System.out.println(pt.getDataType());
//                System.out.println(i + "行， " + j + "列：" + pt.getString());
//                attributes.add(pt);
//            }
//            sender.sendEvent("event_all_array_dateType",attributes);
//        }
//        Thread.sleep(1000);
//        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
//        Assert.assertEquals(10,bt1.rows());
//        BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
//        Assert.assertEquals(10,bt2.rows());
//        checkData(bt,bt2);
//        Thread.sleep(10000);
//        BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
//        Assert.assertEquals(10,bt3.rows());
//        checkData(bt,bt3);
//    }
    @Test
    public  void test_EventClient_all_dateType_vector_no_decimal() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..22);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);
        String script1 ="class event_all_array_dateType{\n" +
                "\tboolv :: BOOL VECTOR\n" +
                "\tcharv :: CHAR VECTOR\n" +
                "\tshortv :: SHORT VECTOR\n" +
                "\tintv :: INT VECTOR\n" +
                "\tlongv :: LONG VECTOR\n" +
                "\tdoublev :: DOUBLE  VECTOR\n" +
                "\tfloatv :: FLOAT VECTOR\n" +
                "\tdatev :: DATE VECTOR\n" +
                "\tmonthv :: MONTH VECTOR\n" +
                "\ttimev :: TIME VECTOR\n" +
                "\tminutev :: MINUTE VECTOR\n" +
                "\tsecondv :: SECOND VECTOR\n" +
                "\tdatetimev :: DATETIME VECTOR \n" +
                "\ttimestampv :: TIMESTAMP VECTOR\n" +
                "\tnanotimev :: NANOTIME VECTOR\n" +
                "\tnanotimestampv :: NANOTIMESTAMP VECTOR\n" +
                "\t//stringv :: STRING VECTOR\n" +
                "\tdatehourv :: DATEHOUR VECTOR\n" +
                "\tuuidv :: UUID VECTOR\n" +
                "\tippaddrv :: IPADDR  VECTOR\n" +
                "\tint128v :: INT128 VECTOR\n" +
                "\t//blobv :: BLOB VECTOR\n" +
                "\tpointv :: POINT VECTOR\n" +
                "\tcomplexv :: COMPLEX VECTOR\n" +
                "  def event_all_array_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, datehour, uuid, ippaddr, int128,point, complex){\n" +
                "\tboolv = bool\n" +
                "\tcharv = char\n" +
                "\tshortv = short\n" +
                "\tintv = int\n" +
                "\tlongv = long\n" +
                "\tdoublev = double\n" +
                "\tfloatv = float\n" +
                "\tdatev = date\n" +
                "\tmonthv = month\n" +
                "\ttimev = time\n" +
                "\tminutev = minute\n" +
                "\tsecondv = second\n" +
                "\tdatetimev = datetime\n" +
                "\ttimestampv = timestamp\n" +
                "\tnanotimev = nanotime\n" +
                "\tnanotimestampv = nanotimestamp\n" +
                "\t//stringv = string\n" +
                "\tdatehourv = datehour\n" +
                "\tuuidv = uuid\n" +
                "\tippaddrv = ippaddr\n" +
                "\tint128v = int128\n" +
                "\t//blobv = blob\n" +
                "\tpointv = point\n" +
                "\tcomplexv = complex\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_array_dateType'\n" +
                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,datehourv,uuidv,ippaddrv,int128v,pointv,complexv';\n" +
                "typeV = [BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[],MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], UUID[], IPADDR[], INT128[], POINT[], COMPLEX[]];\n" +
                "formV = [VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv",  "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable", eventSchemes, eventTimeKeys, commonKeys);

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "intput1", "test1", handler_array_no_decimal, -1, true, "admin", "123456");

        Preparedata_array_1(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        String script2 = "data1=select * from data;\n" +
                "for(i in 0..9){\n" +
                "\tevent_all_array_dateType1=event_all_array_dateType(data1.row(i)[`cbool], data1.row(i)[`cchar], data1.row(i)[`cshort], data1.row(i)[`cint],data1.row(i)[`clong], data1.row(i)[`cdouble], data1.row(i)[`cfloat], data1.row(i)[`cdate], data1.row(i)[`cmonth], data1.row(i)[`ctime], data1.row(i)[`cminute], data1.row(i)[`csecond], data1.row(i)[`cdatetime], data1.row(i)[`ctimestamp], data1.row(i)[`cnanotime], data1.row(i)[`cnanotimestamp], data1.row(i)[`cdatehour], data1.row(i)[`cuuid], data1.row(i)[`cipaddr], data1.row(i)[`cint128],data1.row(i)[`cpoint], data1.row(i)[`ccomplex])\n" +
                "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
                "\t}" ;
        conn.run(script2);
        Thread.sleep(5000);
        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                System.out.println(pt.getDataType());
                System.out.println(i + "行， " + j + "列：" + pt.getString());
                attributes.add(pt);
            }
            sender.sendEvent("event_all_array_dateType",attributes);
        }
        Thread.sleep(1000);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(10,bt1.rows());
        BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt1,bt2);
        Thread.sleep(10000);
        BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt3.rows());
        checkData(bt,bt3);
    }

    //@Test//AJ-659
    public  void test_EventClient_all_dateType_vector_decimal() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..3);\n" +
                "colTypes=[DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);
        String script1 ="class event_all_array_dateType{\n" +
                "\tdecimal32v :: DECIMAL32(2)  VECTOR\n" +
                "\tdecimal64v :: DECIMAL64(7) VECTOR\n" +
                "\tdecimal128v :: DECIMAL128(19) VECTOR \n" +
                "  def event_all_array_dateType(decimal32, decimal64, decimal128){\n" +
                "\tdecimal32v = decimal32\n" +
                "\tdecimal64v = decimal64\n" +
                "\tdecimal128v = decimal128\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_array_dateType'\n" +
                "eventKeys = 'decimal32v,decimal64v,decimal128v';\n" +
                "typeV = [ DECIMAL32(2)[], DECIMAL64(7)[], DECIMAL128(19)[]];\n" +
                "formV = [ VECTOR, VECTOR, VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList( DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(  DF_VECTOR, DF_VECTOR, DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList( 2, 7, 19));

        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable", eventSchemes, eventTimeKeys, commonKeys);

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "intput1", "test1", handler_array_decimal, -1, true, "admin", "123456");

        Preparedata_array_decimal(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        String script2 = "data1=select * from data;\n" +
                "for(i in 0..9){\n" +
                "\tevent_all_array_dateType1=event_all_array_dateType( data1.row(i)[`cdecimal32], data1.row(i)[`cdecimal64], data1.row(i)[`cdecimal128])\n" +
                "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
                "\t}" ;
        conn.run(script2);
        Thread.sleep(5000);
        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                System.out.println(pt.getDataType());
                System.out.println(i + "行， " + j + "列：" + pt.getString());
                attributes.add(pt);
            }
            sender.sendEvent("event_all_array_dateType",attributes);
        }
        Thread.sleep(1000);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(10,bt1.rows());
        BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt1,bt2);
        Thread.sleep(10000);
        BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt3.rows());
        checkData(bt,bt3);
    }

    @Test//not support
    public  void test_EventClient_all_dateType_array() throws IOException, InterruptedException {
        String script0 = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..25);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script0);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, 2, 7,19));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
        conn.run(script);
        EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler_array, -1, true, "admin", "123456");
        Preparedata_array(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        List<Entity> attributes = new ArrayList<>();
        for(int j=0;j<bt.columns();j++){
            Entity pt = (bt.getColumn(j));
            System.out.println(pt.getDataType());
            System.out.println(  j + "列：" + pt.getString());
            attributes.add(pt);
        }
        sender.sendEvent("event_all_array_dateType",attributes);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,bt1.rows());
        Assert.assertEquals(1,bt1.rows());
        Thread.sleep(2000);
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test
    public  void test_EventClient_reconnect() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `timestamp`time`commonKey, [TIMESTAMP,TIME,TIMESTAMP]) as outputTable;\n"+
                "share streamTable(1:0, `string`timestamp`commonKey, [STRING,TIMESTAMP,TIMESTAMP]) as outputTable1;\n"+
                "class MarketData{\n"+
                "timestamp :: TIMESTAMP\n"+
                "time :: TIME\n"+
                "def MarketData(t,t1){\n"+
                "timestamp = t\n"+
                "time = t1\n"+
                "}\n"+
                "}\n"+
                "class MarketData1{\n"+
                "string :: STRING\n"+
                "timestamp :: TIMESTAMP\n"+
                "def MarketData1(s,t){\n"+
                "string = s\n"+
                "timestamp = t\n"+
                "}\n"+
                "}\n"+
                "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(TIMESTAMP, 0) as commonKey) as intput\n"+
                "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n"+
                "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n"+
                "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n"+
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\", commonField = \"timestamp\")\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("MarketData");
        scheme.setFieldNames(Arrays.asList("timestamp", "time"));
        scheme.setFieldTypes(Arrays.asList( DT_TIMESTAMP,DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("MarketData1");
        scheme1.setFieldNames(Arrays.asList("string", "timestamp"));
        scheme1.setFieldTypes(Arrays.asList(DT_STRING, DT_TIMESTAMP));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"timestamp"});
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

        client.subscribe(HOST, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
        conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
        BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

        Assert.assertEquals(1,re.rows());
        Assert.assertEquals(1,re1.rows());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re.getColumn(0).get(0).getString());
        Assert.assertEquals("00:00:00.001",re.getColumn(1).get(0).getString());
        Assert.assertEquals("tesrtttt",re1.getColumn(0).get(0).getString());
        Assert.assertEquals(re2.getColumn(0).get(0).getString(),re1.getColumn(1).get(0).getString());
        client.unsubscribe(HOST, PORT, "intput", "test1");
    }
}
