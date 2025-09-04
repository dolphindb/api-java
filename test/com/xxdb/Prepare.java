package com.xxdb;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import java.io.IOException;
import java.util.Arrays;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class Prepare {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static int[] port_list = Arrays.stream(bundle.getString("PORTS").split(",")).mapToInt(Integer::parseInt).toArray();

    public static void clear_env() throws IOException {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            conn.run("a = getStreamingStat().pubTables\n" +
                    "for(i in a){\n" +
                    "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
                    "}");
        conn.run("res = getStreamingSQLStatus()\n" +
                "    for(sqlStream in res){\n" +
                "        try{unsubscribeStreamingSQL(, sqlStream.queryId)}catch(ex){print ex}\n" +
                "        try{revokeStreamingSQL(sqlStream.queryId)}catch(ex){print ex}\n" +
                "    }\n" +
                "    go;\n" +
                "    try{revokeStreamingSQLTable(`t1)}catch(ex){print ex}\n" +
                "    try{revokeStreamingSQLTable(`t2)}catch(ex){print ex}\n" +
                "    try{revokeStreamingSQLTable(`bondFilter)}catch(ex){print ex}\n" +
                "    try{revokeStreamingSQLTable(`bestBondQuotation)}catch(ex){print ex}\n" );
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
    public static void clear_env_1() throws IOException {
        for (int i = 0; i < port_list.length; i++) {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, port_list[i], "admin", "123456");
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
                "share table(boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v,decimal128v) as data;\n";
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
    }

    public static void Preparedata_keyTable(long count) throws IOException {
        String script = "login(`admin, `123456); \n" +
                "n="+count+";\n" +
                "id = take(\"A\"+string(1..300000), n) ;\n" +
                "tv = timestamp(2025.08.26T12:36:23.438+1..n) ;\n" +
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
                "share keyedTable(`id, id, tv, boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v,decimal128v) as t1;\n" +
                "share keyedTable(`id, id, tv, boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v,decimal128v) as t2;\n";
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script);
    }

    public static void Preparedata1(long count) throws IOException {
        String script = "login(`admin, `123456); \n" +
                "n="+count+";\n" +
                "colNames = `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv`pointv`complexv`decimal32v`decimal64v`decimal128v ;\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,SYMBOL,STRING,DATEHOUR,UUID,IPADDR,INT128,BLOB,POINT,COMPLEX,DECIMAL32(2),DECIMAL64(7),DECIMAL128(18)]\n" +
                "share table(1:0,colNames,colTypes) as data;\n" +
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
                "data.append!(table(boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  symbolv, stringv, datehourv, uuidv, ippaddrv, int128v, blobv, pointv, complexv, decimal32v, decimal64v,decimal128v)) ;\n";
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
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
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25, 5), m))\n" +
                "share table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex,  cdecimal32, cdecimal64, cdecimal128) as data;\n" ;
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script1);
    }

    public static void Preparedata_array_keyTable(long count1,long count2) throws IOException {
        String script1 = "login(`admin, `123456); \n"+
                "n = "+count1+";\n" +
                "m = "+count2+";\n" +
                "rows = ceil(double(n)/m)\n" +
                "id = take(\"A\"+string(1..300000), rows) ;\n" +
                "tv = timestamp(2025.08.26T12:36:23.438+1..rows) ;\n" +
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
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25, 5), m))\n" +
                "share keyedTable(`id, id, tv, cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex,  cdecimal32, cdecimal64, cdecimal128) as t1;\n" +
                "share keyedTable(`id, id, tv, cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex,  cdecimal32, cdecimal64, cdecimal128) as t2;\n" ;
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script1);
    }

    public static void Preparedata_array_1(long count1,long count2) throws IOException {
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
                "share table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, cpoint, ccomplex) as data;\n" ;
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script1);
    }
    public static void Preparedata_array_decimal(long count1,long count2) throws IOException {
        String script1 = "login(`admin, `123456); \n"+
                "n="+count1+";\n" +
                "m="+count2+";\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, n) + 0.254, 3), m))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.2546, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25, 5), m))\n" +
                "share table( cdecimal32, cdecimal64,cdecimal128) as data;" ;
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run(script1);
    }

    public static void PrepareStreamTable_array(String dataType) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n"+
                "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n"+
                "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n"+
                "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n"+
                "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n"+"" +
                "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n"+
                "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n"+
                "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n"+
                "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n"+
                "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n"+
                "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n"+
                "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n"+
                "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n"+
                "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n"+
                "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n"+
                "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n"+
                "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n"+
                "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n"+
                "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n"+
                "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(10000:0, `permno`dateType, [INT,"+dataType +"[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareStreamTableDecimal_array(String dataType, int scale) throws IOException {
        String script = "share streamTable(1000000:0, `permno`dateType, [INT,"+dataType+"("+scale+")[]]) as Trades;\n"+
                "permno = take(1..1000,1000); \n"+
                "dateType_DECIMAL32 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL64 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "dateType_DECIMAL128 =   array(DECIMAL128(8)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n"+
                "share table(permno,dateType_"+dataType +") as pub_t\n"+
                "share streamTable(10000:0, `permno`dateType, [INT,"+dataType +"("+scale+")[]]) as sub1;\n";
        DBConnection conn1 = new DBConnection();
        conn1.connect(HOST, PORT,"admin","123456");
        conn1.run(script);
    }
    public static void PrepareUser(String userName,String password) throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("def create_user(){try{deleteUser(`"+userName+")}catch(ex){};createUser(`"+userName+", '"+password+"',,true);};"+
                "rpc(getControllerAlias(),create_user);" );
    }

    public static void PrepareUser_authMode(String userName,String password,String authMode) throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("def create_user(){try{deleteUser(`"+userName+")}catch(ex){};createUser(`"+userName+", '"+password+"',,true,\""+authMode+"\");};"+
                "rpc(getControllerAlias(),create_user);" );
    }

    public static void checkData(BasicTable exception, BasicTable resTable) {
        assertEquals(exception.rows(), resTable.rows());
        for (int i = 0; i < exception.columns(); i++) {
            //System.out.println("col" + resTable.getColumnName(i));
            assertEquals(exception.getColumn(i).getString(), resTable.getColumn(i).getString());
        }
    }

    public static void wait_data(String table_name, int data_row) throws IOException, InterruptedException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicInt row_num;
        for(int i=0;i<200;i++){
            row_num = (BasicInt)conn.run("(exec count(*) from "+table_name+")[0]");
//            System.out.println(row_num.getInt());
            if(row_num.getInt() == data_row){
                break;
            }
            Thread.sleep(300);
            i++;
        }
    }
}
