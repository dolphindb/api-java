package com.xxdb.streaming.client.cep;

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
import static com.xxdb.data.Entity.DATA_TYPE.*;
import static com.xxdb.data.Entity.DATA_FORM.*;
import static com.xxdb.streaming.client.cep.EventClientTest.PrepareUser;
import static org.junit.Assert.assertEquals;

public class EventSenderTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static EventSender sender = null;
    static EventSchema scheme = null;


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
                "share table(boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v, decimal128v) as data;\n";
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
                "data = table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex,  cdecimal32, cdecimal64, cdecimal128)\n" ;
        conn.run(script1);
    }
    public static void PrepareInputSerializer(String type,Entity.DATA_TYPE data_type) throws IOException {
        String script = "login(`admin, `123456); \n"+
                "class event_dateType{\n" +
                "\tt_type :: "+ type +"\n" +
                "  def event_dateType(type){\n" +
                "\tt_type = type\n" +
                "                }\n" +
                "\t}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_dateType'\n" +
                "eventKeys = 't_type';\n" +
                "typeV = ["+ type +"];\n" +
                "formV = [SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array("+ type +", 0) as commonField) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, commonField=\"t_type\");\n" +
                "share streamTable(1000000:0, `eventType`blobs`commonField, [STRING,BLOB,"+ type +"]) as inputTable;" ;
        conn.run(script);
        scheme = new EventSchema();
        scheme.setEventType("event_dateType");
        scheme.setFieldNames(Arrays.asList("t_type"));
        scheme.setFieldTypes(Arrays.asList(data_type));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR));
        //scheme.setAttrExtraParams(Arrays.asList(null));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>(Arrays.asList("t_type"));
        sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
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
    @Test
    public  void test_EventSender_EventScheme_null() throws IOException, InterruptedException {
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty",re);
    }
    @Test
    public  void test_EventSender_EventScheme_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(null, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty",re);
    }

    @Test
    public  void test_EventSender_EventType_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventSender_EventType_null_1() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventType must be non-empty.",re);
    }

    @Test
    public  void test_EventSender_EventType_special_character() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 ");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");
        Assert.assertEquals("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 ",scheme.getEventType());
    }

    @Test
    public  void test_EventSender_EventType_repetition() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventType must be unique.",re);
    }

    @Test
    public  void test_EventSender_AttrKeys_null() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventKey in eventScheme must be non-empty.",re);
    }

    @Test
    public  void test_EventSender_AttrKeys_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("attrKey must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventSender_AttrKeys_repetition() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventScheme cannot has duplicated attrKey in attrKeys.",re);
    }

    @Test
    public  void test_EventSender_AttrKeys_one_colume() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("time"));
        scheme.setFieldTypes(Arrays.asList(DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("time");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);
        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(0).get(0).getString());
    }

    @Test
    public  void test_EventSender_AttrTypes_null() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }
    @Test
    public  void test_EventSender_AttrTypes_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(null, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("attrType must be non-null.",re);
    }
    @Test
    public  void test_EventSender_AttrTypes_not_support() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_VOID, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Compression Failed: only support integral and temporal data, not support DT_VOID",re);
    }

    @Test
    public  void test_EventSender_AttrForms_null() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }
    @Test
    public  void test_EventSender_AttrForms_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(null, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("attrForm must be non-null.",re);
    }

    @Test
    public  void test_EventSender_AttrForms_not_support() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_PAIR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("attrForm only can be DF_SCALAR or DF_VECTOR.",re);
    }
    @Test
    public  void test_EventSender_attrExtraParams_null() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        //scheme.setAttrExtraParams(Arrays.asList( 0, 0, 0, 1, 2));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("1"));
        attributes.add(new BasicString("2"));
        attributes.add(new BasicDecimal32("2",0));
        attributes.add(new BasicDecimal64("2.88",1));
        attributes.add(new BasicDecimal128("-2.1",2));
        String re = null;
        try{
            sender.sendEvent("market", attributes);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The decimal attribute' scale doesn't match to scheme attrExtraParams scale.",re);
    }
    @Test
    public  void test_EventSender_attrExtraParams_null_1() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        //scheme.setAttrExtraParams(Arrays.asList( 0, 0));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("1"));
        attributes.add(new BasicString("2"));
        sender.sendEvent("market", attributes);
        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
    }
    @Test
    public  void test_EventSender_attrExtraParams_not_match() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList( 0));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.",re);
    }
    @Test
    public  void test_EventSender_attrExtraParams_set_not_true() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale 10 is out of bounds, it must be in [0,9].",re);

        scheme.setFieldExtraParams(Arrays.asList( 1, 19, 39));
        String re1 = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale 19 is out of bounds, it must be in [0,18].",re1);

        scheme.setFieldExtraParams(Arrays.asList( 1, 18, 39));
        String re2 = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re2 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL128 scale 39 is out of bounds, it must be in [0,38].",re2);

        scheme.setFieldExtraParams(Arrays.asList( -1, 10, 10));
        String re3 = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re3 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale -1 is out of bounds, it must be in [0,9].",re3);

        scheme.setFieldExtraParams(Arrays.asList( 1, -1, 0));
        String re4 = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re4 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale -1 is out of bounds, it must be in [0,18].",re4);

        scheme.setFieldExtraParams(Arrays.asList( 0, 0, -1));
        String re5 = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re5 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL128 scale -1 is out of bounds, it must be in [0,38].",re5);
    }
    @Test
    public  void test_EventSender_attrExtraParams_set_true() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList( 0, 0, 0, 1, 2));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("1"));
        attributes.add(new BasicString("2"));
        attributes.add(new BasicDecimal32("2",0));
        attributes.add(new BasicDecimal64("2.88",1));
        attributes.add(new BasicDecimal128("-2.1",2));
        sender.sendEvent("market", attributes);
        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("1",re.getColumn(2).get(0).getString());
        Assert.assertEquals("2",re.getColumn(3).get(0).getString());
        Assert.assertEquals("2",re.getColumn(4).get(0).getString());
        Assert.assertEquals("2.9",re.getColumn(5).get(0).getString());
        Assert.assertEquals("-2.10",re.getColumn(6).get(0).getString());
    }

    @Test
    public  void test_EventSender_eventTimeKeys_not_exist() throws IOException, InterruptedException {
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
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("event market doesn't contain eventTimeKey datetimev.",re);
    }
    @Test
    public  void test_EventSender_eventTimeKeys_not_time_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `string`eventType`event, [STRING,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("market");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            sender.connect(conn, "inputTable");

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("First column of outputTable should be temporal if specified eventTimeKey.",re);
    }

    @Test
    public  void test_EventSender_eventTimeKeys_one_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time"});
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("123456"));
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,11,45,3,100000000))));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market1", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(2,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(0).get(0).getString());
        Assert.assertEquals("11:45:03.100",re.getColumn(0).get(1).getString());
    }
    @Test
    public  void test_EventSender_eventTimeKeys_one_column_1() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time1"});
        List<String> commonKeys = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("event market doesn't contain eventTimeKey time1.",re);
    }
    @Test
    public  void test_EventSender_eventTimeKeys_two_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("123456"));
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,11,45,3,100000000))));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market1", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(2,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(0).get(0).getString());
        Assert.assertEquals("12:45:03.100",re.getColumn(0).get(1).getString());
    }
    @Test
    public  void test_EventSender_commonKeys_not_exist() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonKeys = Arrays.asList(new String[]{"time123"});
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("event market doesn't contain commonKey time123",re);
    }

    @Test
    public  void test_EventSender_commonKeys_one_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonKeys = Arrays.asList(new String[]{"time"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        sender.connect(conn, "inputTable");
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicString("123456"));
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,11,45,3,100000000))));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market1", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(2,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(0).get(0).getString());
        Assert.assertEquals("12:45:03.100",re.getColumn(0).get(1).getString());
        Assert.assertEquals("10:45:03.100",re.getColumn(3).get(0).getString());
        Assert.assertEquals("11:45:03.100",re.getColumn(3).get(1).getString());
    }
    @Test
    public  void test_EventSender_commonKeys_one_column_1() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        EventSchema scheme1 = new EventSchema();
        scheme1.setEventType("market1");
        scheme1.setFieldNames(Arrays.asList("time", "time1"));
        scheme1.setFieldTypes(Arrays.asList(DT_TIME, DT_TIME));
        scheme1.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        eventSchemas.add(scheme1);
        List<String> eventTimeKeys = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonKeys = Arrays.asList(new String[]{"time1"});
        String re = null;
        try{
            EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("event market doesn't contain commonKey time1",re);
    }

    @Test
    public  void test_EventSender_commonKeys_two_column() throws IOException, InterruptedException {
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
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);

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

    @Test
    public  void test_EventSender_connect_not_connect() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        DBConnection conn1 = new DBConnection();
        String re = null;
        try{
            sender.connect(conn1, "inputTable");

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The connection to dolphindb has not been established.",re);
    }
    @Test
    public  void test_EventSender_connect_duplicated() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            sender.connect(conn, "inputTable");
            sender.connect(conn, "inputTable");

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The eventSender has already been called.",re);
    }

    //@Test//not support
    public  void test_EventSender_conn_asynchronousTask_true() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        DBConnection conn1 = new DBConnection(true);
        conn1.connect(HOST,PORT);
        sender.connect(conn1, "inputTable");
    }
    @Test
    public  void test_EventSender_conn_ssl_true() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        DBConnection conn1 = new DBConnection(false,true);
        conn1.connect(HOST,PORT);
        sender.connect(conn1, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("12:45:03.100",re.getColumn(2).get(0).getString());
        Assert.assertEquals("tesrtrrr",re.getColumn(3).get(0).getString());
    }
    @Test
    public  void test_EventSender_conn_compress_true() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        DBConnection conn1 = new DBConnection(false,true,true);
        conn1.connect(HOST,PORT);
        sender.connect(conn1, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market", attributes1);

        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("12:45:03.100",re.getColumn(2).get(0).getString());
        Assert.assertEquals("tesrtrrr",re.getColumn(3).get(0).getString());
    }

    @Test
    public  void test_EventSender_conn_not_admin() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        PrepareUser("user1","123456");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        DBConnection conn1 = new DBConnection(false,true,true);
        conn1.connect(HOST,PORT,"user1","123456");
        sender.connect(conn1, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        sender.sendEvent("market", attributes1);

        BasicTable re = (BasicTable)conn1.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("12:45:03.100",re.getColumn(2).get(0).getString());
        Assert.assertEquals("tesrtrrr",re.getColumn(3).get(0).getString());
    }

    @Test
    public  void test_EventSender_connect_tableName_not_exist() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            sender.connect(conn, "inputTable11");

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("Can't find the object with name inputTable11"));
    }

    @Test
    public  void test_EventSender_connect_tableName_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            sender.connect(conn, null);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("FROM clause must return a table."));
        String re1 = null;
        try{
            sender.connect(conn,"");
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals(true,re1.contains("select top 0 * from "));
    }
    @Test
    public  void test_EventSender_connect_table_cloumn_not_match() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable1;\n";
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        String re = null;
        try{
            sender.connect(conn,"inputTable1");
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Incompatible inputTable1 columns, expected: 2, got: 3",re);
    }

    @Test
    public  void test_EventSender_sendEvent_eventType_not_exist() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        String re = null;
        try{
            sender.sendEvent("market111", attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for unknown eventType market111",re);
    }
    @Test
    public  void test_EventSender_sendEvent_eventType_null() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("tesrtrrr"));
        attributes1.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,12,45,3,100000000))));
        String re = null;
        try{
            sender.sendEvent(null, attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for unknown eventType null",re);
        String re1 = null;
        try{
            sender.sendEvent("", attributes1);
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for unknown eventType ",re1);
    }

    @Test
    public  void test_EventSender_sendEvent_attributes_column_not_match() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,12,45,3,100000000)));
        //attributes1.add("11111");
        String re = null;
        try{
            sender.sendEvent("market", attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for the num of attributes is not match with market",re);
    }
    @Test
    public  void test_EventSender_sendEvent_attributes_type_not_match() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "time"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        eventSchemas.add(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = Arrays.asList(new String[]{"time","market"});
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("12"));
        attributes1.add(new BasicInt(1));
        String re = null;
        try{
            sender.sendEvent("market", attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for the type of 2th attribute of market should be DT_TIME but now it is DT_INT",re);
    }
    @Test
    public  void test_EventSender_sendEvent_attributes_null() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL,STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType_null");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv","stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL,DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
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
        BasicString symbolv = new BasicString("0");
        symbolv.setNull();
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
        attributes.add(symbolv);
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
        attributes.add(decimal128V);
        sender.sendEvent("event_all_dateType_null", attributes);
        //conn.run("tableInsert{outputTable}", attributes);
        Thread.sleep(2000);
        BasicTable re = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(1,re.rows());
    }
    @Test//AJ-647
    public  void test_EventClient_subscribe_attributes_vector_null() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..25);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //scheme.setAttrTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicBooleanVector(0));
        attributes.add(new BasicByteVector(0));
        attributes.add(new BasicShortVector(0));
        attributes.add(new BasicIntVector(0));
        attributes.add(new BasicLongVector(0));
        attributes.add(new BasicDoubleVector(0));
        attributes.add(new BasicFloatVector(0));
        attributes.add(new BasicDateVector(0));
        attributes.add(new BasicMonthVector(0));
        attributes.add(new BasicTimeVector(0));
        attributes.add(new BasicMinuteVector(0));
        attributes.add(new BasicSecondVector(0));
        attributes.add(new BasicDateTimeVector(0));
        attributes.add(new BasicTimestampVector(0));
        attributes.add(new BasicNanoTimeVector(0));
        attributes.add(new BasicNanoTimestampVector(0));
        attributes.add(new BasicDateHourVector(0));
        attributes.add(new BasicUuidVector(0));
        attributes.add(new BasicIPAddrVector(0));
        attributes.add(new BasicInt128Vector(0));
        attributes.add(new BasicPointVector(0));
        attributes.add(new BasicComplexVector(0));
        attributes.add(new BasicDecimal32Vector(0,0));
        attributes.add(new BasicDecimal64Vector(0,0));
        attributes.add(new BasicDecimal128Vector(0,0));
        sender.sendEvent("event_all_array_dateType", attributes);
        //conn.run("tableInsert{outputTable}", attributes);
        Thread.sleep(2000);
        BasicTable re = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,re.rows());
        BasicTable re1 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(1,re1.rows());
    }
    @Test//AJ-647
    public  void test_EventClient_subscribe_attributes_array_null() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..24);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        // scheme.setAttrTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY));

        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BOOL_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_BYTE_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SHORT_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_LONG_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DOUBLE_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_FLOAT_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATE_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MONTH_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIME_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_MINUTE_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_SECOND_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATETIME_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_TIMESTAMP_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIME_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_NANOTIMESTAMP_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DATEHOUR_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_UUID_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_IPADDR_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_INT128_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_POINT_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_COMPLEX_ARRAY,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL32_ARRAY,0,0));
        attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL64_ARRAY,0,0));
        //attributes.add(new BasicArrayVector(Entity.DATA_TYPE.DT_DECIMAL128_ARRAY,0,0));
        sender.sendEvent("event_all_array_dateType", attributes);
        //conn.run("tableInsert{outputTable}", attributes);
    }
    @Test
    public  void test_EventSender_all_dateType_scalar() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `eventTime`eventType`blobs, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
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
                "\tsymbolv :: SYMBOL\n" +
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
                "  def event_all_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, symbol, string, datehour, uuid, ippaddr, int128, blob,point, complex, decimal32, decimal64, decimal128){\n" +
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
                "\tsymbolv = symbol\n" +
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
                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,symbolv,stringv,datehourv,uuidv,ippaddrv,int128v,blobv,pointv,complexv,decimal32v,decimal64v,decimal128v';\n" +
                "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)];\n" +
                "formV = [SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");" +
                "all_data_type1=event_all_dateType(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,  \"world111SYMBOL\", \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\"), point(1, 2), complex(111, 1), decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10))\n" +
                "appendEvent(inputSerializer, all_data_type1)";
        conn.run(script1);
        String script2 = "colNames=\"col\"+string(1..28)\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,SYMBOL,STRING,DATEHOUR,UUID,IPADDR,INT128,BLOB,POINT,COMPLEX,DECIMAL32(3),DECIMAL64(8),DECIMAL128(10)]\n" +
                "t=table(1:0,colNames,colTypes)\n" +
                "insert into t values(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,  \"world111SYMBOL\", \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\"), point(1, 2), complex(111, 1), decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10)) ;";
        conn.run(script2);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("timestampv");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");

        BasicTable bt = (BasicTable) conn.run("select * from t");
        for (int i = 0; i < bt.rows(); i++) {
            List<Entity> attributes = new ArrayList<>();
            for (int j = 0; j < bt.columns(); j++) {
                Entity pt = bt.getColumn(j).get(i);
                attributes.add(pt);
            }
            sender.sendEvent("event_all_dateType", attributes);

            BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
            Assert.assertEquals(1, bt1.rows());
            Thread.sleep(2000);
            BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
            Assert.assertEquals(1, bt2.rows());
            checkData(bt1,bt2);
        }
    }
    @Test
    public  void test_EventSender_scaler_BOOL() throws IOException, InterruptedException {
        PrepareInputSerializer("BOOL",DT_BOOL);
        String script = "event_dateType1 = event_dateType(true);\n" +
                "event_dateType2 = event_dateType(false);\n" +
                "event_dateType3 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicBoolean(true));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicBoolean(false));
        List<Entity> attributes3 = new ArrayList<>();
        BasicBoolean bb = new BasicBoolean(true);
        bb.setNull();
        attributes3.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(3, bt1.rows());
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(3, bt2.rows());
        checkData(bt1,bt2);
        }
    @Test
    public  void test_EventSender_scaler_CHAR() throws IOException, InterruptedException {
        PrepareInputSerializer("CHAR",DT_BYTE);
        String script = "event_dateType1 = event_dateType('1');\n" +
                "event_dateType2 = event_dateType('2');\n" +
                "event_dateType3 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicByte((byte)49));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicByte((byte)50));
        List<Entity> attributes3 = new ArrayList<>();
        BasicByte bb = new BasicByte((byte)1);
        bb.setNull();
        attributes3.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(3, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(3, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_INT() throws IOException, InterruptedException {
        PrepareInputSerializer("INT",DT_INT);
        String script = "event_dateType1 = event_dateType(-2147483648);\n" +
                "event_dateType2 = event_dateType(2147483647);\n" +
                "event_dateType3 = event_dateType(0);\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicInt(-2147483648));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicInt(2147483647));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicInt(0));
        List<Entity> attributes4 = new ArrayList<>();
        BasicInt bb = new BasicInt(1);
        bb.setNull();
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_LONG() throws IOException, InterruptedException {
        PrepareInputSerializer("LONG",DT_LONG);
        String script = "event_dateType1 = event_dateType(-9223372036854775808);\n" +
                "event_dateType2 = event_dateType(9223372036854775807);\n" +
                "event_dateType3 = event_dateType(0);\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicLong(-9223372036854775808l));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicLong(9223372036854775807l));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicLong(0));
        List<Entity> attributes4 = new ArrayList<>();
        BasicLong bb = new BasicLong(1);
        bb.setNull();
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_DOUBLE() throws IOException, InterruptedException {
        PrepareInputSerializer("DOUBLE",DT_DOUBLE);
        String script = "event_dateType1 = event_dateType(-922337.2036854775808);\n" +
                "event_dateType2 = event_dateType(92233.72036854775807);\n" +
                "event_dateType3 = event_dateType(0);\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicDouble(-922337.2036854775808));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicDouble(92233.72036854775807));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicDouble(0));
        List<Entity> attributes4 = new ArrayList<>();
        BasicDouble bb = new BasicDouble(1);
        bb.setNull();
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }

    @Test
    public  void test_EventSender_scaler_FLOAT() throws IOException, InterruptedException {
        PrepareInputSerializer("FLOAT",DT_FLOAT);
        String script = "event_dateType1 = event_dateType(-922337.2036854775808f);\n" +
                "event_dateType2 = event_dateType(92233.72036854775807f);\n" +
                "event_dateType3 = event_dateType(0);\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicFloat(-922337.2036854775808f));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicFloat(92233.72036854775807f));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicFloat(0));
        List<Entity> attributes4 = new ArrayList<>();
        BasicFloat bb = new BasicFloat(1);
        bb.setNull();
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_UUID() throws IOException, InterruptedException {
        PrepareInputSerializer("UUID",DT_UUID);
        String script = "event_dateType1 = event_dateType(uuid(\"00000000-0000-006f-0000-000000000001\"));\n" +
                "event_dateType2 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicUuid(111, 1));
        List<Entity> attributes2 = new ArrayList<>();
        BasicUuid bb = new BasicUuid(0, 0);
        bb.setNull();
        System.out.println(bb.getString());
        attributes2.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(2, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(2, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_COMPLEX() throws IOException, InterruptedException {
        PrepareInputSerializer("COMPLEX",DT_COMPLEX);
        String script = "event_dateType1 = event_dateType(complex(111, 1));\n" +
                "event_dateType2 = event_dateType( complex(0, 0));\n" +
                "event_dateType3 = event_dateType(complex(-0.99, -0.11));\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicComplex(111, 1));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicComplex(0, 0));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicComplex(-0.99, -0.11));
        List<Entity> attributes4 = new ArrayList<>();
        BasicComplex bb = new BasicComplex(0, 0);
        bb.setNull();
        System.out.println(bb.getString());
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_scaler_POINT() throws IOException, InterruptedException {
        PrepareInputSerializer("POINT",DT_POINT);
        String script = "event_dateType1 = event_dateType(point(111, 1));\n" +
                "event_dateType2 = event_dateType( point(0, 0));\n" +
                "event_dateType3 = event_dateType(point(-0.99, -0.11));\n" +
                "event_dateType4 = event_dateType(NULL);\n" +
                "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        conn.run(script);
        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicPoint(111, 1));
        List<Entity> attributes2 = new ArrayList<>();
        attributes2.add(new BasicPoint(0, 0));
        List<Entity> attributes3 = new ArrayList<>();
        attributes3.add(new BasicPoint(-0.99, -0.11));
        List<Entity> attributes4 = new ArrayList<>();
        BasicPoint bb = new BasicPoint(0, 0);
        bb.setNull();
        System.out.println(bb.getString());
        attributes4.add(bb);
        sender.sendEvent("event_dateType", attributes1);
        sender.sendEvent("event_dateType", attributes2);
        sender.sendEvent("event_dateType", attributes3);
        sender.sendEvent("event_dateType", attributes4);
        BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
        Assert.assertEquals(4, bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
        Assert.assertEquals(4, bt2.rows());
        checkData(bt1,bt2);
    }
    @Test
    public  void test_EventSender_all_dateType_scalar_DECIMAL() throws IOException, InterruptedException {
        String script = "share streamTable(1:0, `eventType`blobs, [STRING,BLOB]) as inputTable;\n" +
                "share table(100:0, `decimal32v`decimal64v`decimal128v, [DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);
        String script1 = "class event_all_dateType{\n" +
                "\tdecimal32v :: DECIMAL32(3)\n" +
                "\tdecimal64v :: DECIMAL64(8)\n" +
                "\tdecimal128v :: DECIMAL128(10) \n" +
                "  def event_all_dateType(decimal32, decimal64, decimal128){\n" +
                "\tdecimal32v = decimal32\n" +
                "\tdecimal64v = decimal64\n" +
                "\tdecimal128v = decimal128\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_dateType'\n" +
                "eventKeys = 'decimal32v,decimal64v,decimal128v';\n" +
                "typeV = [ DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)];\n" +
                "formV = [ SCALAR, SCALAR, SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput);" +
                "all_data_type1=event_all_dateType(decimal32(1.1, 3),decimal64(1.1, 8),decimal128(1.1, 10))\n" +
                "appendEvent(inputSerializer, all_data_type1)";
        conn.run(script1);
        String script2 = "colNames=\"col\"+string(1..3)\n" +
                "colTypes=[DECIMAL32(3),DECIMAL64(8),DECIMAL128(10)]\n" +
                "t=table(1:0,colNames,colTypes)\n" +
                "insert into t values(decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10)) ;";
        conn.run(script2);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList( "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList( DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList( DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList( 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn, "inputTable");
        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        BasicTable bt = (BasicTable) conn.run("select * from t");
        for (int i = 0; i < bt.rows(); i++) {
            List<Entity> attributes = new ArrayList<>();
            for (int j = 0; j < bt.columns(); j++) {
                Entity pt = bt.getColumn(j).get(i);
                attributes.add(pt);
            }
            sender.sendEvent("event_all_dateType", attributes);

            BasicTable bt1 = (BasicTable) conn.run("select * from inputTable;");
            Assert.assertEquals(1, bt1.rows());
            Thread.sleep(2000);
            BasicTable bt2 = (BasicTable) conn.run("select * from intput;");
            Assert.assertEquals(1, bt2.rows());
            checkData(bt1,bt2);
            BasicTable bt3 = (BasicTable) conn.run("select * from outputTable;");
            Assert.assertEquals(1, bt3.rows());
            System.out.println(bt3.getString());
        }
    }
    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_1() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv","stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL,DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

        Preparedata(1);
        BasicTable bt = (BasicTable)conn.run("select * from data");
        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                attributes.add(pt);
            }
            sender.sendEvent("event_all_dateType",attributes);
        }
        System.out.println(bt.columns());
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(1,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_100() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv","stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL,DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("timestampv");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
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
        System.out.println(bt.columns());
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(100,bt1.rows());
        Thread.sleep(20000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(100,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_100000() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv","stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_SYMBOL,DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = Collections.singletonList("datetimev");
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
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
        System.out.println(bt.columns());
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(100000,bt1.rows());
        Thread.sleep(50000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(100000,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventSender_all_dateType_vector() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..24);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, 2, 7));

        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
        sender.connect(conn,"inputTable");
        Preparedata_array(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");

        EventClient client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

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
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(10,bt1.rows());
        Thread.sleep(50000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt,bt2);
    }

    @Test
    public  void test_EventSender_all_dateType_array() throws IOException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        //scheme.setAttrKeys(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //scheme.setAttrTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY));
        //scheme.setAttrForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeKeys = new ArrayList<>();
        List<String> commonKeys = new ArrayList<>();
        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeKeys, commonKeys);
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

}
