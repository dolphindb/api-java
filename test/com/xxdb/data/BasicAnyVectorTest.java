package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static com.xxdb.data.Entity.DATA_TYPE.*;
import static java.sql.JDBCType.NULL;
import static org.junit.Assert.*;

public class BasicAnyVectorTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test(expected = UnsupportedOperationException.class)
    public void test_combine(){
        BasicAnyVector bav = new BasicAnyVector(4);
        assertEquals(Entity.DATA_CATEGORY.MIXED,bav.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_ANY,bav.getDataType());
        assertEquals(Entity.class,bav.getElementClass());
        Vector v = new BasicPointVector(5);
        bav.combine(v);
    }

    @Test(expected = RuntimeException.class)
    public void test_asof(){
        BasicAnyVector bav = new BasicAnyVector(5);
        Scalar value = new BasicPoint(2.1,8.4);
        bav.asof(value);
    }

    @Test(expected = RuntimeException.class)
    public void test_getUnitlength(){
        BasicAnyVector bav = new BasicAnyVector(2);
        bav.getUnitLength();
    }

    @Test(expected = RuntimeException.class)
    public void test_serialize() throws IOException {
        BasicAnyVector bav = new BasicAnyVector(1);
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bav.serialize(0,1,out);
    }

    @Test(expected = RuntimeException.class)
    public void test_serialize2() throws IOException {
        BasicAnyVector bav = new BasicAnyVector(5);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(12,24);
        ByteBuffer bb = ByteBuffer.allocate(10);
        bav.serialize(0,1,65534,numElementAndPartial,bb);
    }

    @Test
    public void test_BasicAnyVector_Entity() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[3];
        arr[0] = conn.run("x=[1 3 2];x;");
        arr[1] = conn.run("y=1..4;y;");
        arr[2] = conn.run("z=1..10$5:2;z;");
        BasicAnyVector bav = new BasicAnyVector(arr,true);
        String str = "(([1,3,2]),[1,2,3,4],#0 #1\n" +
                "1  6 \n" +
                "2  7 \n" +
                "3  8 \n" +
                "4  9 \n" +
                "5  10\n" +
                ")";
        assertEquals(str,bav.getString());
        assertEquals("[1,2,3,4]",bav.getEntity(1).getString());
        assertEquals(3,bav.rows());
        String str2 = "(#0 #1\n" +
                "1  6 \n" +
                "2  7 \n" +
                "3  8 \n" +
                "4  9 \n" +
                "5  10\n" +
                ",([1,3,2]))";
        assertEquals(str2,bav.getSubVector(new int[]{2,0}).getString());
        assertFalse(bav.isNull(2));
        bav.setEntity(2,conn.run("x=1..3;y=4..6;z=dict(x,y);z;"));
        assertEquals("{1,2,3}->{4,5,6}",bav.getEntity(2).getString());
        bav.setNull(1);
        assertTrue(bav.isNull(1));
        try{
            bav.get(2);
        }catch(RuntimeException re){
            assertEquals("The element of the vector is not a scalar object.",re.getMessage());
        }
    }
    @Test
    public void test_BasicAnyVector_scalar() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        BasicAnyVector bav1 = new BasicAnyVector(0);
        System.out.println();
        assertEquals("()",bav1.getString());

    }

    @Test
    public void test_ExtendedData() throws IOException {
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return null;
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 4;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 1;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        System.out.println(in.readShort());
        BasicAnyVector bav = new BasicAnyVector(in);
        ExtendedDataOutput out = new LittleEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bav.writeVectorToOutputStream(out);
    }

    @Test
    public void test_basicAnyVector_Append_scalar() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        bav.Append(new BasicInt(16));
        assertEquals("16",bav.get(12).getString());
    }

    @Test
    public void test_BasicAnyVector_Append_vector() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        bav.Append(new BasicIntVector(new int[]{26,31,23,24}));
        assertEquals("[26,31,23,24]",bav.get(12).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicAnyVector_AddRange() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        bav.addRange(new Object[]{new BasicInt(36),new BasicInt(54)});
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicAnyVector_Add() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        bav.add(new BasicInt(33));
    }
    @Test
    public void test_BasicAnyVector_toJsonString() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        String re = JSONObject.toJSONString(bav);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"MIXED\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_ANY\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.Entity\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"(0,1,2,3,4,5,6,7,8,9,...)\",\"table\":false,\"vector\":true}", re);
    }
    @Test
    public void test_BasicAnyVector_table() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicTable re = (BasicTable) conn.run("t= table(`tom`dickh`arry`jack`jill as name,  `m`m`m`m`f as sex, `blue`green`blue`blue`gray as eye);\nselect toArray(name),toArray(eye) from t group by sex ;");
        System.out.println(re.getString());
        assertEquals("sex toArray_name          toArray_eye           \n" +
                "--- --------------------- ----------------------\n" +
                "f   [jill]                [gray]                \n" +
                "m   [tom,dickh,arry,jack] [blue,green,blue,blue]\n",re.getString());
    }

    //@Test
    public void test_BasicAnyVector_performence() throws IOException, InterruptedException {
        List<String> colNames = new ArrayList<>();
        colNames.add("cbool");
        colNames.add("cchar");
        colNames.add("cshort");
        colNames.add("cint");
        colNames.add("clong");
        colNames.add("cdouble");
        colNames.add("cfloat");

        colNames.add("cdate");
        colNames.add("cmonth");
        colNames.add("ctime");
        colNames.add("cminute");
        colNames.add("csecond");
        colNames.add("cdatetime");
        colNames.add("ctimestamp");
        colNames.add("cnanotime");
        colNames.add("cnanotimestamp");
        colNames.add("cdatehour");
        colNames.add("cuuid");
        colNames.add("cipaddr");
        colNames.add("cint128");
        colNames.add("cpoint");
        colNames.add("ccomplex");
        colNames.add("cdecimal32");
        colNames.add("cdecimal64");
        colNames.add("cdecimal128");

        List<String> testCases = new ArrayList<>();
        testCases.add("cbool");
        testCases.add("cchar");
        testCases.add("cshort");
        testCases.add("cint");
        testCases.add("clong");
        testCases.add("cdouble");
        testCases.add("cfloat");

        testCases.add("cdate");
        testCases.add("cmonth");
        testCases.add("ctime");
        testCases.add("cminute");
        testCases.add("csecond");
        testCases.add("cdatetime");
        testCases.add("ctimestamp");
        testCases.add("cnanotime");
        testCases.add("cnanotimestamp");
        testCases.add("cdatehour");
        testCases.add("cuuid");
        testCases.add("cipaddr");
        testCases.add("cint128");
        testCases.add("cpoint");
        testCases.add("ccomplex");
        testCases.add("cdecimal32");
        testCases.add("cdecimal64");
        testCases.add("cdecimal128");
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        //Preparedata_array(500000,10);
        String pre_data = "try{undef(`data,SHARED)}catch(ex){}\n" +
                "try{undef(`data1,SHARED)}catch(ex){}\n" +
                "def createDataTableTuple(n,num){\n" +
                "    boolv = bool(rand([true, false, NULL], n))\n" +
                "        cbool = cut(take([true, false, NULL], n*num), num)\n" +
                "        cchar = cut(take(char(-100..100 join NULL), n*num), num)\n" +
                "        cshort = cut(take(short(-100..100 join NULL), n*num), num)\n" +
                "        cint = cut(take(-100..100 join NULL, n*num), num)\n" +
                "        clong = cut(take(long(-100..100 join NULL), n*num), num)\n" +
                "        cdouble = cut(take(-100..100 join NULL, n*num) + 0.254, num)\n" +
                "        cfloat = cut(take(-100..100 join NULL, n*num) + 0.254f, num)\n" +
                "        cdate = cut(take(2012.01.01..2012.02.29, n*num), num)\n" +
                "        cmonth = cut(take(2012.01M..2013.12M, n*num), num)\n" +
                "        ctime = cut(take(09:00:00.000 + 0..99 * 1000, n*num), num)\n" +
                "        cminute = cut(take(09:00m..15:59m, n*num), num)\n" +
                "        csecond = cut(take(09:00:00 + 0..999, n*num), num)\n" +
                "        cdatetime = cut(take(2012.01.01T09:00:00 + 0..999, n*num), num)\n" +
                "        ctimestamp = cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, n*num), num)\n" +
                "        //timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 10), n))\n" +
                "        cnanotime =cut(take(09:00:00.000000000 + 0..999 * 1000000000, n*num), num)\n" +
                "        cnanotimestamp = cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, n*num), num)\n" +
                "        csymbol = cut(take(symbol(\"A\"+string(1..n)), n*num), num)\n" +
                "        cstring = cut(\"A\"+string(1..(n*num)), num)\n" +
                "        cuuid = cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), n*num), num)\n" +
                "        cdatehour = cut(take(datehour(1..10 join NULL), n*num), num)\n" +
                "        cipaddr = cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), n*num), num)\n" +
                "        cint128 = cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), n*num), num)\n" +
                "        cblob = cut(blob(\"A\"+string(1..(n*num))), num)\n" +
                "        ccomplex = cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n*num), num)\n" +
                "        cpoint = cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n*num), num) \n" +
                "        cdecimal32 = cut(decimal32(take(-100..100 join NULL, n*num) + 0.254, 3), num)\n" +
                "        cdecimal64 = cut(decimal64(take(-100..100 join NULL, n*num) + 0.25467, 5), num)\n" +
                "        cdecimal128 = cut(decimal128(take(-100..100 join NULL, n*num) + 0.25467, 5), num)\n" +
                "        data = table(boolv, cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, csymbol, cstring, cuuid, cipaddr, cint128, cdatehour,cblob,ccomplex,cpoint,cdecimal32, cdecimal64, cdecimal128)\n" +
                "        return data \n" +
                "}\n" +
                "\n" +
                "data = createDataTableTuple(100000,11) \n" +
                "share  data as data1";
        conn.run(pre_data);
        for (int ii = 0; ii < colNames.size(); ii++) {
            //System.out.println("start " + testCases.get(ii));
            long start_download = System.nanoTime();
            for (int i = 0; i < 10; i++) {
                BasicAnyVector bnv = (BasicAnyVector) conn.run("exec " + colNames.get(ii) + " from data1");
            }
            long end_download = System.nanoTime();
            System.out.println(testCases.get(ii) + "下载花费时间：" + (end_download - start_download) / 10000000);

            BasicAnyVector bnv = (BasicAnyVector) conn.run("exec " + colNames.get(ii) + " from data1");
            long start = System.nanoTime();
            for (int i = 0; i < 10; i++) {
                Map<String, Entity> map = new HashMap<String, Entity>();
                map.put("bnv", bnv);
                conn.upload(map);
            }
            long end = System.nanoTime();
            System.out.println(testCases.get(ii) + "上传花费时间：" + (end - start) / 10000000);
        }
    }

    @Test
    public void test_BasicAnyVector_set_dict() throws Exception {
        BasicAnyVector bbb = new BasicAnyVector(1);
        BasicDictionary dictionary3 = new BasicDictionary(DT_STRING, DT_ANY);
        BasicDictionary dictionaryAAA = new BasicDictionary(DT_STRING, DT_STRING);
        dictionaryAAA.put(new BasicString("p1"), new BasicString("1"));
        dictionaryAAA.put(new BasicString("p2"), new BasicString("2"));

        BasicDictionary dictionaryBBB = new BasicDictionary(DT_STRING, DT_STRING);
        dictionaryBBB.put(new BasicString("p1"), new BasicString("100"));
        dictionaryBBB.put(new BasicString("p2"), new BasicString("200"));

        dictionary3.put(new BasicString("aaa"), dictionaryAAA);
        dictionary3.put(new BasicString("bbb"), dictionaryBBB);
        bbb.set(0, dictionary3);
        System.out.println(bbb.getString());
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicAnyVector re = (BasicAnyVector)conn.run("any1=array(ANY,0).append!(dict(`aaa`bbb, [dict(`p1`p2, `1`2), dict(`p1`p2, `100`200)]));any1;");
        System.out.println(re.getString());
        assertEquals(bbb.getString(),re.getString());
    }

    @Test
    public void test_BasicAnyVector_set_dict1() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST, PORT, "admin", "123456");
        String script=
                "try{undef(`st,SHARED)}catch(ex){}\n" +
                "share table(1000:0, `c1`c2,[STRING,ANY]) as st;\n" ;
        conn.run(script);
        Entity RE22 =  conn.run("st");
        BasicAnyVector anyVector = new BasicAnyVector(1);
        BasicDictionary dict = new BasicDictionary(DT_STRING, DT_ANY);
        BasicDictionary dictionaryAAA = new BasicDictionary(DT_STRING, DT_STRING);
        dictionaryAAA.put(new BasicString("p1"), new BasicString("1"));
        dictionaryAAA.put(new BasicString("p2"), new BasicString("2"));
        BasicDictionary dictionaryBBB = new BasicDictionary(DT_STRING, DT_STRING);
        dictionaryBBB.put(new BasicString("p1"), new BasicString("100"));
        dictionaryBBB.put(new BasicString("p2"), new BasicString("200"));
        dict.put(new BasicString("aaa"), dictionaryAAA);
        dict.put(new BasicString("bbb"), dictionaryBBB);
        anyVector.set(0, dict);
        System.out.println(anyVector.getString());
        List<String> colNames = new ArrayList<>();
        colNames.add("c1");
        colNames.add("c2");
        List<Vector> colVectors = new ArrayList<>();
        BasicStringVector stringVector = new BasicStringVector(0);
        stringVector.add("test!");
        colVectors.add(stringVector);
        colVectors.add(anyVector);

        BasicTable table = new BasicTable(colNames, colVectors);
        List<Entity> arguments = Arrays.asList(table);
        conn.run("tableInsert{st}", arguments);
        BasicTable bt = (BasicTable)conn.run("select * from st");
        assertEquals(bt.getColumn(1).getString(), anyVector.getString());
    }
}
