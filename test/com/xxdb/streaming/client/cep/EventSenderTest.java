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

import static com.xxdb.Prepare.*;
import static com.xxdb.data.Entity.DATA_TYPE.*;
import static com.xxdb.data.Entity.DATA_FORM.*;

public class EventSenderTest {
    public static DBConnection conn ;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static EventSender sender = null;
    static EventSchema scheme = null;
    static EventClient client = null;

    @Before
    public void setUp() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        clear_env();
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    }

    @After
    public  void after() throws IOException, InterruptedException {
        conn.close();
        try{client.unsubscribe(HOST, PORT, "inputTable", "test1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "intput", "test1");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "inputTable" ,"javaStreamingApi");}catch (Exception ex){}
        try{client.unsubscribe(HOST, PORT, "intput" ,"javaStreamingApi");}catch (Exception ex){}
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>(Arrays.asList("t_type"));
        sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
    public static  EventMessageHandler handler_array = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attributes) {
            System.out.println("eventType: " + eventType);
            String boolv = attributes.get(0).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String charv = attributes.get(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String shortv = attributes.get(2).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String intv = attributes.get(3).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String longv = attributes.get(4).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String doublev = attributes.get(5).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String floatv = attributes.get(6).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datev = attributes.get(7).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String monthv = attributes.get(8).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String timev = attributes.get(9).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String minutev = attributes.get(10).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String secondv = attributes.get(11).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datetimev = attributes.get(12).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String timestampv = attributes.get(13).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String nanotimev = attributes.get(14).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String nanotimestampv = attributes.get(15).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datehourv = attributes.get(16).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String uuidv = attributes.get(17).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String ippaddrv = attributes.get(18).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String int128v = attributes.get(19).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String pointv =attributes.get(20).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
            pointv = pointv.substring(1, pointv.length() - 1);
            String[] point1 = pointv.split("\\),\\(");
            String point2 = null;
            StringBuilder re1 = new StringBuilder();
            StringBuilder re2 = new StringBuilder();
            for(int i=0;i<point1.length;i++){
                point2 = point1[i];
                String[] dataType3 = point2.split(",");
                re1.append(dataType3[0]);
                re1.append(' ');
                re2.append(dataType3[1]);
                re2.append(' ');
            }
            pointv = re1+","+re2;
            pointv = pointv.replaceAll("\\(","").replaceAll("\\)","");

            String complex1 = attributes.get(21).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
            complex1 = complex1.substring(1, complex1.length() - 1);
            String[] complex2 = complex1.split(",");
            String complex3 = null;
            StringBuilder re11 = new StringBuilder();
            StringBuilder re21 = new StringBuilder();
            for(int i=0;i<complex2.length;i++){
                complex3 = complex2[i];
                String[] complex4 = complex3.split("\\+");
                re11.append(complex4[0]);
                re11.append(' ');
                re21.append(complex4[1]);
                re21.append(' ');
            }
            complex1 = re11+","+re21;
            String complexv = complex1.replaceAll("i","");

            String decimal32v = attributes.get(22).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String decimal64v = attributes.get(23).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String decimal128v = attributes.get(24).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');

            for (int i=0;i<attributes.size();i++){
                //attributes.get(i).getString();
                System.out.println(attributes.get(i).getString());
            }
            String script = null;
             script = String.format("insert into outputTable values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,[datehour(%s)],[uuid(%s)],[ipaddr(%s)],[int128(%s)],[point(%s)],[complex(%s)],%s,%s,%s)", boolv, charv, shortv, intv, longv, doublev, floatv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, datehourv, uuidv, ippaddrv, int128v, pointv, complexv, decimal32v, decimal64v, decimal128v);
            //script = String.format("insert into outputTable values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,[datehour(%s)],[uuid(%s)],[ipaddr(%s)],[int128(%s)],[point(%s)],[complex(%s)])", boolv, charv, shortv, intv, longv, doublev, floatv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, datehourv, uuidv, ippaddrv, int128v, pointv, complexv);

            try {
                conn.run(script);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    public static  EventMessageHandler handler_array_no_decimal = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attributes) {
            System.out.println("eventType: " + eventType);
            String boolv = attributes.get(0).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String charv = attributes.get(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String shortv = attributes.get(2).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String intv = attributes.get(3).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String longv = attributes.get(4).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String doublev = attributes.get(5).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String floatv = attributes.get(6).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datev = attributes.get(7).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String monthv = attributes.get(8).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String timev = attributes.get(9).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String minutev = attributes.get(10).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String secondv = attributes.get(11).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datetimev = attributes.get(12).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String timestampv = attributes.get(13).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String nanotimev = attributes.get(14).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String nanotimestampv = attributes.get(15).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String datehourv = attributes.get(16).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String uuidv = attributes.get(17).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String ippaddrv = attributes.get(18).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String int128v = attributes.get(19).getString().replaceAll("\\[", "\\[\"").replaceAll("]", "\"]").replaceAll(",", "\",\"").replaceAll("\"\"", "NULL");
            String pointv =attributes.get(20).getString().replaceAll("\\(,\\)", "\\(NULL,NULL\\)");
            pointv = pointv.substring(1, pointv.length() - 1);
            String[] point1 = pointv.split("\\),\\(");
            String point2 = null;
            StringBuilder re1 = new StringBuilder();
            StringBuilder re2 = new StringBuilder();
            for(int i=0;i<point1.length;i++){
                point2 = point1[i];
                String[] dataType3 = point2.split(",");
                re1.append(dataType3[0]);
                re1.append(' ');
                re2.append(dataType3[1]);
                re2.append(' ');
            }
            pointv = re1+","+re2;
            pointv = pointv.replaceAll("\\(","").replaceAll("\\)","");

            String complex1 = attributes.get(21).getString().replaceAll(",,", ",NULL+NULL,").replaceAll("\\[,", "[NULL+NULL,").replaceAll(",]", ",NULL+NULL]");
            complex1 = complex1.substring(1, complex1.length() - 1);
            String[] complex2 = complex1.split(",");
            String complex3 = null;
            StringBuilder re11 = new StringBuilder();
            StringBuilder re21 = new StringBuilder();
            for(int i=0;i<complex2.length;i++){
                complex3 = complex2[i];
                String[] complex4 = complex3.split("\\+");
                re11.append(complex4[0]);
                re11.append(' ');
                re21.append(complex4[1]);
                re21.append(' ');
            }
            complex1 = re11+","+re21;
            String complexv = complex1.replaceAll("i","");
            for (int i=0;i<attributes.size();i++){
                //attributes.get(i).getString();
                System.out.println(attributes.get(i).getString());
            }
            String script = null;
            script = String.format("insert into outputTable values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,[datehour(%s)],[uuid(%s)],[ipaddr(%s)],[int128(%s)],[point(%s)],[complex(%s)])", boolv, charv, shortv, intv, longv, doublev, floatv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, datehourv, uuidv, ippaddrv, int128v, pointv, complexv);
            try {
                DBConnection conn1 = new DBConnection();
                conn1.connect(HOST,PORT,"admin","123456");
                conn1.run(script);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    public static  EventMessageHandler handler_array_decimal = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attributes) {
            System.out.println("eventType: " + eventType);
            String decimal32v = attributes.get(0).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String decimal64v = attributes.get(1).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');
            String decimal128v = attributes.get(2).getString().replaceAll(",,", ",NULL,").replaceAll("\\[,", "[NULL,").replaceAll(",]", ",NULL]").replace(',', ' ');

            for (int i=0;i<attributes.size();i++){
                //attributes.get(i).getString();
                System.out.println(attributes.get(i).getString());
            }
            String script = null;
            script = String.format("insert into outputTable values( %s,%s,%s)", decimal32v, decimal64v, decimal128v);

            try {
                conn.run(script);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    @Test
    public  void test_EventSender_EventScheme_null() throws IOException, InterruptedException {
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty.",re);
    }
    @Test
    public  void test_EventSender_EventScheme_null_1() throws IOException, InterruptedException {
        //EventSchema scheme = new EventSchema();
        //List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",null, eventTimeFields, commonFields);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("eventSchema must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventSender_EventType_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = new ArrayList<>();
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = Collections.singletonList("market");
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventType must be unique.",re);
    }

    @Test
    public  void test_EventSender_fieldNames_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        //scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("fieldName in eventSchema must be non-empty.",re);
    }

    @Test
    public  void test_EventSender_fieldNames_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("fieldName must be non-null and non-empty.",re);
    }

    @Test
    public  void test_EventSender_fieldNames_repetition() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("time","time"));
        scheme.setFieldTypes(Arrays.asList(DT_TIME,DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR,DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("time");
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("EventSchema cannot has duplicated fieldName in fieldNames.",re);
    }

    @Test
    public  void test_EventSender_EventType_one_colume() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("time"));
        scheme.setFieldTypes(Arrays.asList(DT_TIME));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("time");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicTime(LocalTime.from(LocalDateTime.of(2024,3,22,10,45,3,100000000))));
        sender.sendEvent("market", attributes);
        BasicTable re = (BasicTable)conn.run("select * from inputTable");
        Assert.assertEquals(1,re.rows());
        Assert.assertEquals("10:45:03.100",re.getColumn(0).get(0).getString());
    }

    @Test
    public  void test_EventSender_FieldTypes_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        //scheme.setAttrTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of fieldName, fieldTypes, fieldForms and fieldExtraParams (if set) must have the same length.",re);
    }
    @Test
    public  void test_EventSender_fieldTypes_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(null, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("fieldType must be non-null.",re);
    }
    @Test
    public  void test_EventSender_FieldTypes_not_support() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_VOID, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Compression Failed: only support integral and temporal data, not support DT_VOID",re);
    }

    @Test
    public  void test_EventSender_fieldForms_null() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        //scheme.setAttrForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of fieldName, fieldTypes, fieldForms and fieldExtraParams (if set) must have the same length.",re);
    }
    @Test
    public  void test_EventSender_fieldForms_null_1() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(null, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("fieldForm must be non-null.",re);
    }

    @Test
    public  void test_EventSender_fieldForms_not_support() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "price", "qty", "eventTime"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP));
        scheme.setFieldForms(Arrays.asList(DF_PAIR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("fieldForm only can be DF_SCALAR or DF_VECTOR.",re);
    }
    @Test
    public  void test_EventSender_fieldExtraParams_null() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        //scheme.setAttrExtraParams(Arrays.asList( 0, 0, 0, 1, 2));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        Assert.assertEquals("The decimal attribute' scale doesn't match to schema fieldExtraParams scale.",re);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("the number of fieldName, fieldTypes, fieldForms and fieldExtraParams (if set) must have the same length.",re);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale 10 is out of bounds, it must be in [0,9].",re);

        scheme.setFieldExtraParams(Arrays.asList( 1, 19, 39));
        String re1 = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale 19 is out of bounds, it must be in [0,18].",re1);

        scheme.setFieldExtraParams(Arrays.asList( 1, 18, 39));
        String re2 = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        }catch(Exception ex){
            re2 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL128 scale 39 is out of bounds, it must be in [0,38].",re2);

        scheme.setFieldExtraParams(Arrays.asList( -1, 10, 10));
        String re3 = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        }catch(Exception ex){
            re3 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL32 scale -1 is out of bounds, it must be in [0,9].",re3);

        scheme.setFieldExtraParams(Arrays.asList( 1, -1, 0));
        String re4 = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        }catch(Exception ex){
            re4 = ex.getMessage();
        }
        Assert.assertEquals("DT_DECIMAL64 scale -1 is out of bounds, it must be in [0,18].",re4);

        scheme.setFieldExtraParams(Arrays.asList( 0, 0, -1));
        String re5 = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
    public  void test_EventSender_eventTimeFields_not_exist() throws IOException, InterruptedException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("datetimev");
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Event market doesn't contain eventTimeField datetimev.",re);
    }
    @Test
    public  void test_EventSender_eventTimeFields_not_time_column() throws IOException, InterruptedException {
        conn.run("share streamTable(1000000:0, `string`eventType`event, [STRING,STRING,BLOB]) as inputTable;");
        EventSchema scheme = new EventSchema();
        scheme.setEventType("market");
        scheme.setFieldNames(Arrays.asList("market", "code"));
        scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("market");
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("First column of outputTable should be temporal if specified eventTimeField.",re);
    }

    @Test
    public  void test_EventSender_eventTimeFields_one_column() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time"});
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
    public  void test_EventSender_eventTimeFields_one_column_1() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time1"});
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Event market doesn't contain eventTimeField time1.",re);
    }
    @Test
    public  void test_EventSender_eventTimeFields_two_column() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
    public  void test_EventSender_commonFields_not_exist() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonFields = Arrays.asList(new String[]{"time123"});
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Event market doesn't contain commonField time123",re);
    }

    @Test
    public  void test_EventSender_commonFields_one_column() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonFields = Arrays.asList(new String[]{"time"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
    public  void test_EventSender_commonFields_one_column_1() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = Arrays.asList(new String[]{"time", "time1"});
        List<String> commonFields = Arrays.asList(new String[]{"time1"});
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Event market doesn't contain commonField time1",re);
    }

    @Test
    public  void test_EventSender_commonFields_two_column() throws IOException, InterruptedException {
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        DBConnection conn1 = new DBConnection();
        String re = null;
        try{
            EventSender sender = new EventSender(conn1, "inputTable",eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Couldn't send script/function to the remote host because the connection has been closed",re);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        EventSender sender1 = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        DBConnection conn1 = new DBConnection(true);
        conn1.connect(HOST,PORT);
        EventSender sender = new EventSender(conn1, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        DBConnection conn1 = new DBConnection(false,true);
        conn1.connect(HOST,PORT);
        EventSender sender = new EventSender(conn1, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        DBConnection conn1 = new DBConnection(false,true,true);
        conn1.connect(HOST,PORT);
        EventSender sender = new EventSender(conn1, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        DBConnection conn1 = new DBConnection(false,true,true);
        conn1.connect(HOST,PORT,"user1","123456");
        EventSender sender = new EventSender(conn1, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable11",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        String re = null;
        try{
            EventSender sender = new EventSender(conn, null,eventSchemas, eventTimeFields, commonFields);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals(true,re.contains("FROM clause must be followed by a table."));
        String re1 = null;
        try{
            EventSender sender = new EventSender(conn, "",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String re = null;
        try{
            EventSender sender = new EventSender(conn, "inputTable1",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicTimestamp(LocalDateTime.of(2024,3,22,12,45,3,100000000)));
        //attributes1.add("11111");
        String re = null;
        try{
            sender.sendEvent("market", attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for the number of event values does not match market",re);
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = Arrays.asList(new String[]{"time","market"});
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        List<Entity> attributes1 = new ArrayList<>();
        attributes1.add(new BasicString("12"));
        attributes1.add(new BasicInt(1));
        String re = null;
        try{
            sender.sendEvent("market", attributes1);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("serialize event Fail for Expected type for the field time of market:DT_TIME, but now it is DT_INT",re);
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
        List<String> eventTimeFields = Collections.singletonList("datetimev");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");

    }
    @Test
    public  void test_EventSender_all_dateType_scalar() throws IOException, InterruptedException {
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
               // "\tsymbolv :: SYMBOL\n" +
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
               // "\tsymbolv = symbol\n" +
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
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");" +
                "all_data_type1=event_all_dateType(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,   \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\"), point(1, 2), complex(111, 1), decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10))\n" +
                "appendEvent(inputSerializer, all_data_type1)";
        conn.run(script1);
        String script2 = "colNames=\"col\"+string(1..27)\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,STRING,DATEHOUR,UUID,IPADDR,INT128,BLOB,POINT,COMPLEX,DECIMAL32(3),DECIMAL64(8),DECIMAL128(10)]\n" +
                "t=table(1:0,colNames,colTypes)\n" +
                "insert into t values(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,  \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\"), point(1, 2), complex(111, 1), decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10)) ;";
        conn.run(script2);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv",  "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP,  DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,  null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("timestampv");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

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
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
            client.unsubscribe(HOST, PORT, "inputTable", "test1");
        }
    }
    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_1() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR,  DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("datetimev");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_100() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR,  DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null, null, 3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("timestampv");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventSender_subscribe_all_dateType_scalar_100000() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n"+
                "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB, POINT, COMPLEX, DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv","pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_STRING, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_BLOB, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR,  DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, null, null,  3, 8, 10));

        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = Collections.singletonList("datetimev");
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
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
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test//精度问题
    public  void test_EventSender_all_dateType_vector() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..25);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);

        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, 2, 7,19));

        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
        Preparedata_array(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");

        client = new EventClient(eventSchemas, eventTimeFields, commonFields);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler_array, -1, true, "admin", "123456");

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
        Thread.sleep(10000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt,bt2);
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }
    @Test
    public  void test_EventSender_all_dateType_vector_no_decimal() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "colNames=\"col\"+string(1..22);\n" +
                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[]];\n" +
                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        conn.run(script);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null));

        EventSender sender = new EventSender(conn, "inputTable",eventSchemes, eventTimeFields, commonFields);
        Preparedata_array_1(100,10);
        BasicTable bt = (BasicTable)conn.run("select * from data");

        client = new EventClient(eventSchemes, eventTimeFields, commonFields);
        client.subscribe(HOST, PORT, "inputTable", "test1", handler_array_no_decimal, -1, true, "admin", "123456");

        for(int i=0;i<bt.rows();i++){
            List<Entity> attributes = new ArrayList<>();
            for(int j=0;j<bt.columns();j++){
                Entity pt = bt.getColumn(j).get(i);
                // System.out.println(pt.getDataType());
                // System.out.println(i + "行， " + j + "列：" + pt.getString());
                attributes.add(pt);
            }
            sender.sendEvent("event_all_array_dateType",attributes);
        }
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(10,bt1.rows());
        Thread.sleep(10000);
        BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
        Assert.assertEquals(10,bt2.rows());
        checkData(bt,bt2);
        client.unsubscribe(HOST, PORT, "inputTable", "test1");
    }

    @Test
    public  void test_EventClient_vector_decimal_1() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "share table(1:0,[\"col1\"],[DECIMAL32(2)[]]) as outputTable;\n" ;
        conn.run(script);
        String script1 ="class event_all_array_dateType{\n" +
                "\tdecimal32v :: DECIMAL32(3)  VECTOR\n" +
                "  def event_all_array_dateType(decimal32){\n" +
                "\tdecimal32v = decimal32\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_all_array_dateType'\n" +
                "eventKeys = 'decimal32v';\n" +
                "typeV = [ DECIMAL32(2)[]];\n" +
                "formV = [ VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("decimal32v"));
        scheme.setFieldTypes(Arrays.asList( DT_DECIMAL32));
        scheme.setFieldForms(Arrays.asList(  DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList( 2));

        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemes, eventTimeFields, commonFields);
        String script2 = "\tevent_all_array_dateType1=event_all_array_dateType( decimal32(1 2.001,2))\n" +
                "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" ;
        conn.run(script2);
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicDecimal32Vector(new String[]{"1","2.001"},2));
        sender.sendEvent("event_all_array_dateType",attributes);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,bt1.rows());
        BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
        Assert.assertEquals(1,bt2.rows());
        checkData(bt1,bt2);
    }
    public static  EventMessageHandler handler_string = new EventMessageHandler() {
        @Override
        public void doEvent(String eventType, List<Entity> attribute) {
            System.out.println("eventType: " + eventType);
            System.out.println(attribute.toString());
            System.out.println(eventType.equals("event_string"));
                try {
                    conn.run("tableInsert{outputTable}", attribute);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
    };
    @Test
    public  void test_EventClient_vector_string() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "share table(1:0,[\"col1\"],[STRING]) as outputTable;\n" ;
        conn.run(script);
        String script1 ="class event_string{\n" +
                "\tstringv :: STRING  VECTOR\n" +
                "  def event_string(string){\n" +
                "\tstringv = string\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_string'\n" +
                "eventKeys = 'stringv';\n" +
                "typeV = [ STRING];\n" +
                "formV = [ VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_string");
        scheme.setFieldNames(Arrays.asList("stringv"));
        scheme.setFieldTypes(Arrays.asList( DT_STRING));
        scheme.setFieldForms(Arrays.asList(  DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList( 2));

        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemes, eventTimeFields, commonFields);
        client = new EventClient(eventSchemes, eventTimeFields, commonFields);
        client.subscribe(HOST, PORT, "intput1", "test1", handler_string, -1, true, "admin", "123456");

        String script2 = "\tevent_string1=event_string( [\"111\",\"222\",\"\",NULL])\n" +
                "\tappendEvent(inputSerializer, event_string1)\n" ;
        conn.run(script2);
         List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicStringVector(new String[]{"111","222","",""}));
        sender.sendEvent("event_string",attributes);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
        Assert.assertEquals(1,bt2.rows());
        checkData(bt1,bt2);
        client.unsubscribe(HOST, PORT, "intput1", "test1");
    }
    @Test
    public  void test_EventClient_vector_symbol() throws IOException, InterruptedException {
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
                "share table(1:0,[\"col1\"],[STRING]) as outputTable;\n" ;
        conn.run(script);
        String script1 ="class event_symbol{\n" +
                "\tsymbolv :: SYMBOL  VECTOR\n" +
                "  def event_symbol(symbol){\n" +
                "\tsymbolv = symbol\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_symbol'\n" +
                "eventKeys = 'symbolv';\n" +
                "typeV = [ SYMBOL];\n" +
                "formV = [ VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        conn.run(script1);
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_symbol");
        scheme.setFieldNames(Arrays.asList("symbolv"));
        scheme.setFieldTypes(Arrays.asList( DT_SYMBOL));
        scheme.setFieldForms(Arrays.asList(  DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList( 2));

        List<EventSchema> eventSchemes = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        EventSender sender = new EventSender(conn, "inputTable",eventSchemes, eventTimeFields, commonFields);
        client = new EventClient(eventSchemes, eventTimeFields, commonFields);
        client.subscribe(HOST, PORT, "intput1", "test1", handler_string, -1, true, "admin", "123456");

        String script2 = "\tevent_symbol1=event_symbol( symbol([\"111\",\"222\",\"\",NULL]))\n" +
                "\tappendEvent(inputSerializer, event_symbol1)\n" ;
        conn.run(script2);
        List<Entity> attributes = new ArrayList<>();
        attributes.add(new BasicSymbolVector(Arrays.asList(new String[]{"111", "222", "", ""})));
        sender.sendEvent("event_symbol",attributes);
        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        Assert.assertEquals(1,bt1.rows());
        Thread.sleep(2000);
        BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
        Assert.assertEquals(1,bt2.rows());
        checkData(bt1,bt2);
        client.unsubscribe(HOST, PORT, "intput1", "test1");
    }
    @Test
    public  void test_EventSender_all_dateType_array() throws IOException {
        EventSchema scheme = new EventSchema();
        scheme.setEventType("event_all_array_dateType");
        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        scheme.setFieldTypes(Arrays.asList(DT_BOOL_ARRAY, DT_BYTE_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DOUBLE_ARRAY, DT_FLOAT_ARRAY, DT_DATE_ARRAY,DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_UUID_ARRAY, DT_IPADDR_ARRAY, DT_INT128_ARRAY, DT_POINT_ARRAY, DT_COMPLEX_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY));
        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        scheme.setFieldExtraParams(Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,null, null, null, null, null, null, null, null, null, null, 2, 7,19));
        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        List<String> eventTimeFields = new ArrayList<>();
        List<String> commonFields = new ArrayList<>();
        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
        conn.run(script);
        EventSender sender = new EventSender(conn, "inputTable",eventSchemas, eventTimeFields, commonFields);
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
