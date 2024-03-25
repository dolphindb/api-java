package com.xxdb.streaming.cep;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.xxdb.MultithreadedTableWriterTest.checkData;
import static com.xxdb.data.Entity.DATA_FORM.DF_SCALAR;
import static com.xxdb.data.Entity.DATA_FORM.DF_VECTOR;
import static com.xxdb.data.Entity.DATA_TYPE.*;

public class EventClientTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static EventClient client = null;
    static EventSender sender = null;

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
        conn.run("try{dropStreamEngine(\"serInput\");\n}catch(ex){\n}\n");
    }

    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        clear_env();
    }

    @After
    public  void after() throws IOException, InterruptedException {
        conn.close();
    }

    public static void Preparedata(long count) throws IOException {
        String script = "login(`admin, `123456); \n" +
                "n="+count+";\n" +
                "boolv = bool(rand([true, false, NULL], n));\n" +
                "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                "intv = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                "share table(boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v) as data;\n";
        conn.run(script);
    }
    public static void Preparedata_array(long count1,long count2) throws IOException {
        String script1 = "login(`admin, `123456); \n"+
                "n="+count1+";\n" +
                "m="+count2+";\n" +
                "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], n), m))\n" +
                "cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), n), m))\n" +
                "cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), n), m))\n" +
                "cint = array(INT[]).append!(cut(take(-100..100 join NULL, n), m))\n" +
                "clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), n), m))\n" +
                "cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, n) + 0.254, m))\n" +
                "cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, n) + 0.254f, m))\n" +
                "cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, n), m))\n" +
                "cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, n), m))\n" +
                "ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, n), m))\n" +
                "cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, n), m))\n" +
                "csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, n), m))\n" +
                "cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, n), m))\n" +
                "ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, n), m))\n" +
                "cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), n), m))\n" +
                "cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), n), m))\n" +
                "cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), n), m))\n" +
                "cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), n), m))\n" +
                "ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, n) + 0.254, 3), m))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25467, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25467, 5), m))\n" +
                "data = table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex,  cdecimal32, cdecimal64)\n" ;
        conn.run(script1);
    }
    public static void PrepareUser(String userName,String password) throws IOException {
        conn.run("def create_user(){try{deleteUser(`"+userName+")}catch(ex){};createUser(`"+userName+", '"+password+"');};"+
                "rpc(getControllerAlias(),create_user);" );
    }
    public static  EventMessageHandler handler = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attribute) {
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
        EventScheme scheme = new EventScheme();
        List<EventScheme> eventSchemes = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty for the EventClient Constructor.",re);
    }

    @Test
    public  void test_EventClient_EventType_null() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient  client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty for the EventClient Constructor.",re);
    }

    @Test
    public  void test_EventClient_EventType_null_1() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("");
        scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventClient eventSchemes must not be empty",re);
    }

    @Test//到时候修改成覆盖所有数据类型的
    public  void test_EventClient_EventType_special_character() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `market`code`price`qty`eventTime, [STRING, STRING, DOUBLE, INT, TIMESTAMP]) as outputTable;\n";
        //conn.run(script);
        EventScheme scheme = new EventScheme();
        scheme.setEventType("MarketData");
        scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("eventTime");
        List<String> commonKeys = new ArrayList<>();
        EventClient  client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "output", "test1", handler, -1, true, "admin", "123456");
        Thread.sleep(200000);
        BasicTable bt1 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(1,bt1.rows());
    }

    @Test
    public  void test_EventClient_EventType_repetition() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "time", "decimal32", "decimal64", "decimal128"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        EventScheme scheme1 = new EventScheme();
        scheme1.setEventType("market");
        scheme1.setAttrKeys(Arrays.asList("market1", "time1", "decimal32", "decimal64", "decimal128"));
        scheme1.setAttrTypes(Arrays.asList(DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme1.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        eventSchemes.add(scheme);
        eventSchemes.add(scheme1);
        List<String> eventTimeKeys = Collections.singletonList("market");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("？ eventType must be unique",re);
    }

    @Test
    public  void test_EventClient_AttrKeys_null() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        //scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("？ the eventKey in eventScheme must not be empty",re);
    }

    @Test
    public  void test_EventClient_AttrKeys_repetition() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("time","time"));
        scheme.setAttrTypes(Arrays.asList(DT_TIME,DT_TIME));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR,DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("time");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("提示待确认",re);
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
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchemes=schema, outputTable=intput, eventTimeKey = \"timestamp\")\n";
        conn.run(script);
        EventScheme scheme = new EventScheme();
        scheme.setEventType("MarketData");
        scheme.setAttrKeys(Arrays.asList("timestamp"));
        scheme.setAttrTypes(Arrays.asList(DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("timestamp");
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
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
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        //scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventClient the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }

    @Test
    public  void test_EventClient_AttrForms_null() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventClient the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }

    @Test
    public  void test_EventClient_attrExtraParams_null() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
    }

    @Test
    public  void test_EventClient_attrExtraParams_set_not_true() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("decimal32", "decimal64", "decimal128"));
        scheme.setAttrTypes(Arrays.asList( DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setAttrExtraParams(Arrays.asList( 10, 19, 39));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("报错提示待确认",re);

        scheme.setAttrExtraParams(Arrays.asList( -1, -1, -1));
        String re1 = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("报错提示待确认",re1);
    }

    @Test
    public  void test_EventClient_eventTimeKeys_not_exist() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventClient event market doesn't contain eventTimeKey datetimev",re);
    }
    @Test
    public  void test_EventClient_eventTimeKeys_not_time_column() throws IOException, InterruptedException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("market");
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("待确认",re);
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
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchemes=schema, outputTable=intput, eventTimeKey = \"timestamp\")\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("MarketData");
        scheme.setAttrKeys(Arrays.asList("timestamp", "time"));
        scheme.setAttrTypes(Arrays.asList( DT_TIMESTAMP,DT_TIME));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventScheme scheme1 = new EventScheme();
        scheme1.setEventType("MarketData1");
        scheme1.setAttrKeys(Arrays.asList("string", "timestamp"));
        scheme1.setAttrTypes(Arrays.asList(DT_STRING, DT_TIMESTAMP));
        scheme1.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        eventSchemes.add(scheme);
        eventSchemes.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "timestamp"});
        List<String> commonKeys = new ArrayList<>();
        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

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
                "class MainMonitor{\n"+
                "def MainMonitor(){}\n"+
                "def updateMarketData(event)\n"+
                "def onload(){addEventListener(updateMarketData,'MarketData',,'all')}\n"+
                "def updateMarketData(event){emitEvent(event)}\n"+
                "}\n"+
                "dummy = table(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs);\n"+
                "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(TIMESTAMP, 0) as commonKey) as intput\n"+
                "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n"+
                "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n"+
                "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n"+
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchemes=schema, outputTable=intput, eventTimeKey = \"timestamp\", commonKeys = \"timestamp\")\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("MarketData");
        scheme.setAttrKeys(Arrays.asList("timestamp", "time"));
        scheme.setAttrTypes(Arrays.asList( DT_TIMESTAMP,DT_TIME));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventScheme scheme1 = new EventScheme();
        scheme1.setEventType("MarketData1");
        scheme1.setAttrKeys(Arrays.asList("string", "timestamp"));
        scheme1.setAttrTypes(Arrays.asList(DT_STRING, DT_TIMESTAMP));
        scheme1.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        eventSchemes.add(scheme);
        eventSchemes.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"timestamp"});
        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);

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
        EventScheme scheme = new EventScheme();
        scheme.setEventType("market");
        scheme.setAttrKeys(Arrays.asList("market", "time"));
        scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventScheme scheme1 = new EventScheme();
        scheme1.setEventType("market1");
        scheme1.setAttrKeys(Arrays.asList("market", "time"));
        scheme1.setAttrTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme1.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        eventSchemes.add(scheme);
        eventSchemes.add(scheme1);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);

        sender.connect(conn, "inputTable");
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
        EventScheme scheme = new EventScheme();
        scheme.setEventType("MarketData");
        scheme.setAttrKeys(Arrays.asList("timestamp", "comment1"));
        scheme.setAttrTypes(Arrays.asList( DT_TIMESTAMP,DT_STRING));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = new ArrayList<>();
        eventSchemes.add(scheme);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"timestamp"});
        List<String> commonKeys = Arrays.asList(new String[]{"comment1"});
        sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
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
        sender.connect(conn,"inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_offset_negative_2() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -2, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_offset_0() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable");
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
        sender.connect(conn,"inputTable");
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
        sender.connect(conn,"inputTable");
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
        sender.connect(conn,"inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);
       // client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, false, "admin", "123456");
        Thread.sleep(1000);

        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_subscribe_reconnect_false() throws IOException, InterruptedException {
        subscribePrepare();

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
        subscribePrepare();
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("addAccessControl(`inputTable1)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable1");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler, -1, true, "admin", "123456");
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
        subscribePrepare();
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("addAccessControl(`inputTable1)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable1");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler, -1, true, "user1", "123456");
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
        subscribePrepare();
        conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("addAccessControl(`inputTable1)");
        String re = null;
        try{
            client.subscribe(HOST, PORT, "inputTable1", "test1", handler1, -1, true, "user2", "123456");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true, re.contains("No access to shared table [inputTable1]"));
    }
    @Test
    public  void test_EventClient_subscribe_unsubscribe_resubscribe() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        sender.sendEvent("MarketData", attributes);

        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);

        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        Thread.sleep(1000);

        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(2,re.rows());
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
        Assert.assertEquals(true, re.contains("already be subscribed' function: 'publishTable'"));
    }
    @Test(expected = NullPointerException.class)
    public  void test_EventClient_not_subscribe_unsubscribe() throws IOException, InterruptedException {
        subscribePrepare();
        String re = null;
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test//跟这个AJ-644问题应该是同一个
    public  void test_EventClient_unsubscribe_duplicated() throws IOException, InterruptedException {
        subscribePrepare();
        client.subscribe(HOST, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
        String re = null;
        try{
            client.unsubscribe(HOST, PORT, "inputTable", "test1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
    }

    @Test
    public  void test_EventClient_subscribe_haStreamTable() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("haStreamTable(11, table, `inputTable1, 100000)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable1");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_subscribe_haStreamTable_leader() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("haStreamTable(11, table, `inputTable1, 100000)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable1");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }

    @Test
    public  void test_EventClient_subscribe_haStreamTable_follower() throws IOException, InterruptedException {
        subscribePrepare();
        conn.run("table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
        conn.run("haStreamTable(11, table, `inputTable1, 100000)");
        conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
        sender.connect(conn,"inputTable1");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,10,45,3,100000000)));
        attributes.add(new BasicString("123456"));
        client.subscribe(HOST, PORT, "inputTable1", "test1", handler, -1, true, "user1", "123456");
        sender.sendEvent("MarketData", attributes);
        Thread.sleep(1000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventClient_sendEvent_attributes_null() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8)]) as outputTable;\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_dateType_null");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList( DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL, DT_STRING, DT_UUID, DT_DATEHOUR, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        List<Entity> attributes = new ArrayList<>();
        BasicBoolean boolv = new BasicBoolean(true);
        //boolv.setNull();
        BasicByte charv = new BasicByte(Byte.parseByte("0"));
        charv.setNull();
        BasicShort shortv = new BasicShort((short) 1);
        shortv.setNull();
        BasicInt intv = new BasicInt(0);
        intv.setNull();
        BasicLong longv = new BasicLong(0);
        longv.setNull();
        BasicDouble doublev = new BasicDouble(0);
        doublev.setNull();
        BasicFloat floatv = new BasicFloat(0);
        floatv.setNull();
        BasicDate datev = new BasicDate(0);
        datev.setNull();
        BasicMonth monthv = new BasicMonth(0);
        monthv.setNull();
        BasicTime timev = new BasicTime(0);
        timev.setNull();
        BasicMinute minutev = new BasicMinute(0);
        minutev.setNull();
        BasicSecond secondv = new BasicSecond(0);
        secondv.setNull();
        BasicDateTime datetimev = new BasicDateTime(0);
        datetimev.setNull();
        BasicTimestamp timestampv = new BasicTimestamp(0);
        timestampv.setNull();
        BasicNanoTime nanotimev = new BasicNanoTime(0);
        nanotimev.setNull();
        BasicNanoTimestamp nanotimestampv = new BasicNanoTimestamp(0);
        nanotimestampv.setNull();
        BasicString stringv = new BasicString("0");
        stringv.setNull();
        BasicUuid uuidv = new BasicUuid(1,1);
        uuidv.setNull();
        BasicDateHour datehourv = new BasicDateHour(0);
        datehourv.setNull();
        BasicIPAddr ippaddrv = new BasicIPAddr(1,1);
        ippaddrv.setNull();
        BasicInt128 int128v = new BasicInt128(1,1);
        int128v.setNull();
        BasicString blobv = new BasicString( "= new String[0],true",true);
        blobv.setNull();
        BasicComplex complexv = new BasicComplex(0,1);
        complexv.setNull();
        BasicPoint pointv = new BasicPoint(0,1);
        pointv.setNull();
        BasicDecimal32 decimal32v = new BasicDecimal32(0,0);
        decimal32v.setNull();
        BasicDecimal64 decimal64v = new BasicDecimal64(0,0);
        decimal64v.setNull();
        BasicDecimal128 decimal128V = new BasicDecimal128(BigInteger.valueOf(0),0);
        decimal128V.setNull();
        attributes.add(boolv);
        attributes.add(charv);
        attributes.add(shortv);
        attributes.add(intv);
        attributes.add(longv);
        attributes.add(doublev);
        attributes.add(floatv);
        attributes.add(datev);
        attributes.add(monthv);
        attributes.add(timev);
        attributes.add(minutev);
        attributes.add(secondv);
        attributes.add(datetimev);
        attributes.add(timestampv);
        attributes.add(nanotimev);
        attributes.add(nanotimestampv);
        //attributes.add(symbolv);
        attributes.add(stringv);
        attributes.add(datehourv);
        attributes.add(uuidv);
        attributes.add(ippaddrv);
        attributes.add(int128v);
        attributes.add(blobv);
        attributes.add(pointv);
        attributes.add(complexv);
        attributes.add(decimal32v);
        attributes.add(decimal64v);
        //attributes.add(decimal128V);
        sender.sendEvent("event_all_dateType_null", attributes);
        //conn.run("tableInsert{outputTable}", attributes);
    }

    @Test//需要修改成array
    public  void test_EventClient_sendEvent_attributes_array() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8)]) as outputTable;\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_dateType_null");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList( DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL, DT_STRING, DT_UUID, DT_DATEHOUR, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
        List<Entity> attributes = new ArrayList<>();
        BasicBoolean boolv = new BasicBoolean(true);
        //boolv.setNull();
        BasicByte charv = new BasicByte(Byte.parseByte("0"));
        charv.setNull();
        BasicShort shortv = new BasicShort((short) 1);
        shortv.setNull();
        BasicInt intv = new BasicInt(0);
        intv.setNull();
        BasicLong longv = new BasicLong(0);
        longv.setNull();
        BasicDouble doublev = new BasicDouble(0);
        doublev.setNull();
        BasicFloat floatv = new BasicFloat(0);
        floatv.setNull();
        BasicDate datev = new BasicDate(0);
        datev.setNull();
        BasicMonth monthv = new BasicMonth(0);
        monthv.setNull();
        BasicTime timev = new BasicTime(0);
        timev.setNull();
        BasicMinute minutev = new BasicMinute(0);
        minutev.setNull();
        BasicSecond secondv = new BasicSecond(0);
        secondv.setNull();
        BasicDateTime datetimev = new BasicDateTime(0);
        datetimev.setNull();
        BasicTimestamp timestampv = new BasicTimestamp(0);
        timestampv.setNull();
        BasicNanoTime nanotimev = new BasicNanoTime(0);
        nanotimev.setNull();
        BasicNanoTimestamp nanotimestampv = new BasicNanoTimestamp(0);
        nanotimestampv.setNull();
        BasicString stringv = new BasicString("0");
        stringv.setNull();
        BasicUuid uuidv = new BasicUuid(1,1);
        uuidv.setNull();
        BasicDateHour datehourv = new BasicDateHour(0);
        datehourv.setNull();
        BasicIPAddr ippaddrv = new BasicIPAddr(1,1);
        ippaddrv.setNull();
        BasicInt128 int128v = new BasicInt128(1,1);
        int128v.setNull();
        BasicString blobv = new BasicString( "= new String[0],true",true);
        blobv.setNull();
        BasicComplex complexv = new BasicComplex(0,1);
        complexv.setNull();
        BasicPoint pointv = new BasicPoint(0,1);
        pointv.setNull();
        BasicDecimal32 decimal32v = new BasicDecimal32(0,0);
        decimal32v.setNull();
        BasicDecimal64 decimal64v = new BasicDecimal64(0,0);
        decimal64v.setNull();
        BasicDecimal128 decimal128V = new BasicDecimal128(BigInteger.valueOf(0),0);
        decimal128V.setNull();
        attributes.add(boolv);
        attributes.add(charv);
        attributes.add(shortv);
        attributes.add(intv);
        attributes.add(longv);
        attributes.add(doublev);
        attributes.add(floatv);
        attributes.add(datev);
        attributes.add(monthv);
        attributes.add(timev);
        attributes.add(minutev);
        attributes.add(secondv);
        attributes.add(datetimev);
        attributes.add(timestampv);
        attributes.add(nanotimev);
        attributes.add(nanotimestampv);
        //attributes.add(symbolv);
        attributes.add(stringv);
        attributes.add(datehourv);
        attributes.add(uuidv);
        attributes.add(ippaddrv);
        attributes.add(int128v);
        attributes.add(blobv);
        attributes.add(pointv);
        attributes.add(complexv);
        attributes.add(decimal32v);
        attributes.add(decimal64v);
        //attributes.add(decimal128V);
        sender.sendEvent("event_all_dateType_null", attributes);
        //conn.run("tableInsert{outputTable}", attributes);
    }

    @Test
    public  void test_EventClient_subscribe_all_dateType_scalar() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8)]) as outputTable;\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_dateType");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList( DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL, DT_STRING, DT_UUID, DT_DATEHOUR, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        Preparedata(100);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                attributes.add(pt);
            }
            sender.sendEvent("event_all_dateType",attributes);
        }
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(100,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(100,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventClient_subscribe_all_dateType_scalar_100000() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8)]) as outputTable;\n";
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_dateType");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList( DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL, DT_STRING, DT_UUID, DT_DATEHOUR, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        Preparedata(100000);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                attributes.add(pt);
            }
            sender.sendEvent("event_all_dateType",attributes);
        }
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(100000,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(100000,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventClient_all_dateType_vector() throws IOException, InterruptedException {
        //DBConnection conn = new DBConnection(true);
        //conn.connect(HOST, PORT, "admin", "123456");
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..24);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_array_dateType");
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");
        Preparedata_array(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");

        EventClient client = new EventClient(eventSchemes, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                //System.out.println(pt.getDataType());
                //System.out.println(i + "行， " + j + "列：" + pt.getString());
                attributes.add(pt);
            }
            sender.sendEvent("event_all_array_dateType",attributes);
        }
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(10,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventClient_all_dateType_vector_asynchronousTask_true() throws IOException, InterruptedException {
        DBConnection conn = new DBConnection(true);
        conn.connect(HOST, PORT, "admin", "123456");
    }
    @Test
    public  void test_EventClient_all_dateType_array() throws IOException {
        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_array_dateType");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY));
        //scheme.setAttrForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY));
        scheme.setAttrForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
        conn.run(script);
        sender.connect(conn,"inputTable");
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
    }

    @Test
    public  void test_EventClient_connect_table_cloumn_not_match() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..24);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);

        EventScheme scheme = new EventScheme();
        scheme.setEventType("event_all_array_dateType");
        scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setAttrForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventScheme> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemes, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

    }
}
