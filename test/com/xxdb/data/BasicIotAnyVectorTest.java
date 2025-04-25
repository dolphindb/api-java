package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.*;

import static com.xxdb.data.Entity.DATA_TYPE.DT_IOTANY;
import static com.xxdb.data.Entity.DATA_TYPE.DT_VOID;

public class BasicIotAnyVectorTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
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
    public void test_BasicIotAnyVector_Scalar() throws IOException {
        BasicByte bbyte = new BasicByte((byte)127);
        BasicShort bshort = new BasicShort((short) 0);
        BasicInt bint = new BasicInt(-4);
        BasicLong blong = new BasicLong(-4);
        BasicBoolean bbool = new BasicBoolean(false);
        BasicFloat bfloat = new BasicFloat((float) 1.99);
        BasicDouble bdouble = new BasicDouble( 1.99);
        BasicString bsting = new BasicString( "最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa");
        Scalar[] scalar = new Scalar[]{bbyte,bshort,bint,blong,bbool,bfloat,bdouble,bsting};
        BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);
        Assert.assertEquals(Entity.DATA_CATEGORY.MIXED,BIV.getDataCategory());
        Assert.assertEquals(DT_IOTANY,BIV.getDataType());
        Assert.assertEquals(8,BIV.rows());
        Assert.assertEquals(1,BIV.columns());
        System.out.println(BIV.getString());
        Assert.assertEquals("[127,0,-4,-4,false,1.99000001,1.99,最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa]",BIV.getString());
        Scalar[] scalar1 = new Scalar[]{new BasicString( "qqa123"),new BasicString( "最新字符qqa")};
        BasicIotAnyVector BIV1 = new BasicIotAnyVector(scalar1);
        Assert.assertEquals("[qqa123,最新字符qqa]",BIV1.getString());
        System.out.println(BIV1.getString(1));
        Scalar[] scalar2 = new Scalar[100000];
        for(int i=0;i<scalar2.length;i++){
            scalar2[i] = scalar[i % scalar.length];
        }
        BasicIotAnyVector BIV2 = new BasicIotAnyVector(scalar2);
        //System.out.println(BIV2.getString());
        Assert.assertEquals(100000,BIV2.rows());
        Assert.assertEquals(1,BIV2.columns());
    }

    @Test
    public void test_BasicIotAnyVector_Scalar_null() throws IOException {
        Scalar[] scalar = new Scalar[]{};
        String re = null;
        try{
            BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);
        }catch(Exception E){
            re = E.getMessage();
        }
        Assert.assertEquals("The param 'scalars' cannot be null or empty.",re);
        Scalar[] scalar1 = null;
        String re1 = null;
        try{
            BasicIotAnyVector BIV1 = new BasicIotAnyVector(scalar1);
        }catch(Exception E){
            re1 = E.getMessage();
        }
        Assert.assertEquals("The param 'scalars' cannot be null or empty.",re1);

        BasicByte bbyte = new BasicByte((byte)127);
        bbyte.setNull();
        BasicShort bshort = new BasicShort((short) 0);
        bshort.setNull();
        BasicInt bint = new BasicInt(-4);
        bint.setNull();
        BasicLong blong = new BasicLong(-4);
        blong.setNull();
        BasicBoolean bbool = new BasicBoolean(false);
        bbool.setNull();
        BasicFloat bfloat = new BasicFloat((float) 1.99);
        bfloat.setNull();
        BasicDouble bdouble = new BasicDouble( 1.99);
        bdouble.setNull();
        BasicString bsting = new BasicString( "BasicString");
        bsting.setNull();
        Scalar[] scalar2 = new Scalar[]{bbyte,bshort,bint,blong,bbool,bfloat,bdouble,bsting};
        BasicIotAnyVector BIV2 = new BasicIotAnyVector(scalar2);
        Assert.assertEquals("[,,,,,,,]",BIV2.getString());
    }

//    @Test  该种构建方式不支持，可能后面会支持，故case仅注释掉
//    public void test_BasicIotAnyVector_1() throws IOException {
//        byte[] vchar = new byte[]{(byte)'c',(byte)'a'};
//        BasicByteVector bcv = new BasicByteVector(vchar);
//        //cshort
//        short[] vshort = new short[]{32767,-29};
//        BasicShortVector bshv = new BasicShortVector(vshort);
//        //cint
//        int[] vint = new int[]{2147483647,483647};
//        BasicIntVector bintv = new BasicIntVector(vint);
//        //clong
//        long[] vlong = new long[]{2147483647,483647};
//        BasicLongVector blongv = new BasicLongVector(vlong);
//
//        boolean[] vbool = new boolean[]{true,false};
//        BasicBooleanVector bboolv = new BasicBooleanVector(vbool);
//
//        float[] vfloat = new float[]{2147.483647f,483.647f};
//        BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
//        //cdouble
//        double[] vdouble = new double[]{214.7483647,48.3647};
//        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
//        //csymbol
//        List<String> list = new ArrayList<>();
//        list.add("KingBase");
//        list.add("vastBase");
//        list.add(null);
//        list.add("OceanBase");
//        BasicSymbolVector bsymbolv = new BasicSymbolVector(list);
//
//        //cstring
//        String[] vstring1 = new String[]{"GOOG","MS"};
//        BasicStringVector bstringv1 = new BasicStringVector(vstring1);
//        //cstring
//        String[] vstring = new String[]{"string","test string1"};
//        BasicStringVector bstringv = new BasicStringVector(vstring);
//
//        Entity[] entity = new Entity[]{bcv,bshv,bintv, blongv,bboolv,bfloatv,bdoublev,bsymbolv,bstringv1,bstringv};
        //BasicIotAnyVector BIV = new BasicIotAnyVector(entity);
        //System.out.println(BIV.getString());
        //Assert.assertEquals("['c','a',32767,-29,2147483647,483647,2147483647,483647,true,false,2147.48364258,483.64700317,214.7483647,48.3647,KingBase,vastBase,,OceanBase,GOOG,MS,string,test string1]",BIV.getString());
        //System.out.println(BIV.getString(23));
    //}

    @Test
    public void test_BasicIotAnyVector_2() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT222\")) dropDatabase(\"dfs://testIOT222\")\n" +
                "     create database \"dfs://testIOT222\" partitioned by  HASH([INT, 40]),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT222\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT222\",\"pt\");\n" +
                "t = table([1,2] as deviceId,\n" +
                "  [now(),now()] as timestamp,\n" +
                "  [`loc1`loc2] as location,\n" +
                "  [long(233),string(234)] as value)\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT222\", `pt) ;");
        System.out.println(entity1.getString());
        BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
        Assert.assertEquals(Entity.DATA_CATEGORY.MIXED,BIV.getDataCategory());
        Assert.assertEquals(DT_IOTANY,BIV.getDataType());
        Assert.assertEquals(2,BIV.rows());
        Assert.assertEquals(1,BIV.columns());
        Assert.assertEquals("[233,234]",BIV.getString());
        Assert.assertEquals("233",BIV.get(0).getString());
        Assert.assertEquals("233",BIV.getString(0));
        String re = null;
        try{
            BIV.getUnitLength();
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("IotAnyVector.getUnitLength not supported.",re);
        String re1 = null;
        try{
            BIV.getExtraParamForType();
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("Not support yet",re1);

        String re2 = null;
        try{
            BIV.getSubVector(new int[]{1,2,3});
        }catch(Exception ex){
            re2 = ex.getMessage();
        }
        Assert.assertEquals("BasicIotAnyVector.getSubVector not supported.",re2);

        String re3 = null;
        try{
            BIV.set(0, new BasicInt(1));
        }catch(Exception ex){
            re3 = ex.getMessage();
        }
        Assert.assertEquals("BasicIotAnyVector.set not supported.",re3);

        String re5 = null;
        try{
            BIV.isNull(0);
        }catch(Exception ex){
            re5 = ex.getMessage();
        }
        Assert.assertEquals("BasicIotAnyVector.isNull not supported.",re5);

        String re7 = null;
        try{
            BIV.setNull(0);
        }catch(Exception ex){
            re7 = ex.getMessage();
        }
        Assert.assertEquals("BasicIotAnyVector.setNull not supported.",re7);

        String re8 = null;
        try{
            BIV.addRange(new Object[]{1,2});
        }catch(Exception ex){
            re8 = ex.getMessage();
        }
        Assert.assertEquals("IotAnyVector.addRange not supported.",re8);

        String re9 = null;
        try{
            BIV.addRange(new Object[]{1,2});
        }catch(Exception ex){
            re9 = ex.getMessage();
        }
        Assert.assertEquals("IotAnyVector.addRange not supported.",re9);

        String re10 = null;
        try{
            BIV.Append(new BasicString("1"));
        }catch(Exception ex){
            re10 = ex.getMessage();
        }
        Assert.assertEquals("IotAnyVector.Append not supported.",re10);

        String re12 = null;
        try{
            BIV.Append(new BasicStringVector(0));
        }catch(Exception ex){
            re12 = ex.getMessage();
        }
        Assert.assertEquals("IotAnyVector.Append not supported.",re12);

        String re13 = null;
        try{
            BIV.asof(new BasicString("1"));
        }catch(Exception ex){
            re13 = ex.getMessage();
        }
        Assert.assertEquals("BasicAnyVector.asof not supported.",re13);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_iotAnyVector_combine() throws IOException {
        Scalar[] scalar1 = new Scalar[]{new BasicString( "qqa123"),new BasicString( "最新字符qqa")};
        BasicIotAnyVector BIV = new BasicIotAnyVector(scalar1);
        BIV.combine(new BasicIntVector(2));
        }

    @Test
    public void test_iotAnyVector_bigData() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT123\")) dropDatabase(\"dfs://testIOT123\")\n" +
                "     create database \"dfs://testIOT123\" partitioned by  HASH([INT, 40]),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT123\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT123\",\"pt\");\n" +
                "t=table(take(1..100000,100000) as deviceId, take(now()+(0..100), 100000) as timestamp,  take(\"bb\"+string(0..100), 100000) as location, take(int(1..100000),100000) as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table(take(100001..200000,100000) as deviceId, take(now()+(0..100), 100000) as timestamp,take(lpad(string(1), 8, \"0\"), 100000) as location, rand(200.0, 100000) as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT123\", `pt) order by timestamp;");
        System.out.println(entity1.getColumn(3).getString());
        Assert.assertEquals(200000,entity1.rows());
        BasicTable entity2 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT123\", `pt)  where deviceId in 1..100000 order by timestamp");
        Assert.assertEquals(entity2.getColumn(0).getString(),entity2.getColumn(0).getString());
    }

    @Test
    public void test_iotAnyVector_allDateType() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", entity1.getColumn("value").getString());
        BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("  exee=exec value from loadTable( \"dfs://testIOT\", `pt) order by deviceId;exee");
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", entity2.getString());
        BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", BIV.getString());
        Assert.assertEquals("'Q'", BIV.get(0).getString());
        Assert.assertEquals("233", BIV.get(1).getString());
        Assert.assertEquals("-233", BIV.get(2).getString());
        Assert.assertEquals("233121", BIV.get(3).getString());
        Assert.assertEquals("true", BIV.get(4).getString());
        Assert.assertEquals("233.33999634", BIV.get(5).getString());
        Assert.assertEquals("233.34", BIV.get(6).getString());
        Assert.assertEquals("loc1", BIV.get(7).getString());
        Assert.assertEquals("AAA", BIV.get(8).getString());
        Assert.assertEquals("bbb", BIV.get(9).getString());
        Assert.assertEquals("xxx", BIV.get(10).getString());
        Assert.assertEquals("'Q'", BIV.getString(0));
        Assert.assertEquals("233", BIV.getString(1));
        Assert.assertEquals("-233", BIV.getString(2));
        Assert.assertEquals("233121", BIV.getString(3));
        Assert.assertEquals("true", BIV.getString(4));
        Assert.assertEquals("233.33999634", BIV.getString(5));
        Assert.assertEquals("233.34", BIV.getString(6));
        Assert.assertEquals("loc1", BIV.getString(7));
        Assert.assertEquals("AAA", BIV.getString(8));
        Assert.assertEquals("bbb", BIV.getString(9));
        Assert.assertEquals("xxx", BIV.getString(10));
    }

    @Test
    public void test_iotAnyVector_allDateType_null() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol([`AAA,`AAA,NULL])] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("[,,,,,,,,AAA,AAA,]", entity1.getColumn("value").getString());
        BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("  exec value from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("[,,,,,,,,AAA,AAA,]", entity2.getString());
        BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
        Assert.assertEquals("[,,,,,,,,AAA,AAA,]", BIV.getString());
        Assert.assertEquals("", BIV.get(0).getString());
        Assert.assertEquals("", BIV.get(1).getString());
        Assert.assertEquals("", BIV.get(2).getString());
        Assert.assertEquals("", BIV.get(3).getString());
        Assert.assertEquals("", BIV.get(4).getString());
        Assert.assertEquals("", BIV.get(5).getString());
        Assert.assertEquals("", BIV.get(6).getString());
        Assert.assertEquals("", BIV.get(7).getString());
        Assert.assertEquals("AAA", BIV.get(8).getString());
        Assert.assertEquals("AAA", BIV.get(9).getString());
        Assert.assertEquals("", BIV.get(10).getString());
        Assert.assertEquals("", BIV.getString(0));
        Assert.assertEquals("", BIV.getString(1));
        Assert.assertEquals("", BIV.getString(2));
        Assert.assertEquals("", BIV.getString(3));
        Assert.assertEquals("", BIV.getString(4));
        Assert.assertEquals("", BIV.getString(5));
        Assert.assertEquals("", BIV.getString(6));
        Assert.assertEquals("", BIV.getString(7));
        Assert.assertEquals("AAA", BIV.getString(8));
        Assert.assertEquals("AAA", BIV.getString(9));
        Assert.assertEquals("", BIV.getString(10));
    }
    @Test
    public void test_iotAnyVector_allDateType_upload() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", entity1.getColumn("value").getString());
        BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
        System.out.println(BIV.getString());
        Map<String, Entity> map = new HashMap<>();
        map.put("iotAny1", BIV);
        conn.upload(map);
        Entity entity2 = conn.run("iotAny1");
        System.out.println(entity2.getString());
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", entity2.getString());
    }

    @Test
    public void test_iotAnyVector_allDateType_upload_null() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol([NULL,`AAA,`AAA])] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("[,,,,,,,,,AAA,AAA]", entity1.getColumn("value").getString());
        BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
        System.out.println(BIV.getString());
        Map<String, Entity> map = new HashMap<>();
        map.put("iotAny1", BIV);
        conn.upload(map);
        Entity entity2 = conn.run("iotAny1");
        System.out.println(entity2.getString());
        Assert.assertEquals("[,,,,,,,,,AAA,AAA]", entity2.getString());

        Entity entity22 = conn.run("typestr(iotAny1)");
        System.out.println(entity22.getString());
        Assert.assertEquals("IOTANY VECTOR", entity22.getString());

        Map<String, Entity> map1 = new HashMap<>();
        map1.put("iotAny2", entity1);
        conn.upload(map1);
        BasicTable entity3 = (BasicTable)conn.run("iotAny2");
        System.out.println(entity3.getString());
        Assert.assertEquals("[,,,,,,,,,AAA,AAA]", entity3.getColumn(3).getString());

        BasicTable entity33 = (BasicTable)conn.run("select typeString from schema(pt).colDefs where name = \"value\";");
        System.out.println(entity33.getString());
        Assert.assertEquals("IOTANY", entity33.getColumn(0).getString(0));

        BasicTable entity4 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId limit 9 ;");
        //System.out.println(entity4.getColumn("value"));
        BasicIotAnyVector entity44 = (BasicIotAnyVector)entity4.getColumn("value");
        Map<String, Entity> map2 = new HashMap<>();
        map2.put("iotAny3", entity44);
        conn.upload(map2);
        BasicIotAnyVector entity5 = (BasicIotAnyVector)conn.run("iotAny3");
        System.out.println(entity5.getString());
        Assert.assertEquals("[,,,,,,,,]", entity5.getString());

        List<String> colNames = new ArrayList<String>();
        colNames.add("deviceId");
        colNames.add("timestamp");
        colNames.add("location");
        colNames.add("value");
        List<Vector> cols = new ArrayList<Vector>();
        cols.add(new BasicIntVector(0));
        cols.add(new BasicTimestampVector(0));
        cols.add(new BasicStringVector(0));
        cols.add(new BasicIntVector(0));
        BasicTable bt = new BasicTable(colNames,cols);
        Map<String, Entity> map3 = new HashMap<>();
        map3.put("iotAnyTable", bt);
        conn.upload(map3);
        BasicTable iotAnyTable = (BasicTable)conn.run("iotAnyTable");
        System.out.println(iotAnyTable.getString());
        Assert.assertEquals(0, iotAnyTable.rows());
    }

    @Test
    public void test_iotAnyVector_allDateType_void() throws IOException {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n"  ;
        conn.run(script);
        BasicIotAnyVector entity1 = (BasicIotAnyVector)conn.run("exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx]", entity1.getString());
        BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("pt = exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId; subarray(pt,0:20)");
        System.out.println(entity2.getString());
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx,,,,,,,,,]",entity2.getString());
        Assert.assertEquals("", entity2.getString(11));
        Assert.assertEquals(DT_VOID, entity2.get(11).getDataType());
        Assert.assertEquals("", entity2.get(11).getString());
        Assert.assertEquals("", entity2.get(19).getString());
        Map<String, Entity> map = new HashMap<>();
        map.put("iotAny1", entity2);
        conn.upload(map);
        Entity entity3 = conn.run("iotAny1");
        System.out.println(entity3.getString());
        Assert.assertEquals("['Q',233,-233,233121,true,233.33999634,233.34,loc1,AAA,bbb,xxx,,,,,,,,,]",entity3.getString());
        BasicIotAnyVector entity4= (BasicIotAnyVector)conn.run("pt = exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId; subarray(pt,11:20)");
        System.out.println(entity4.getString());
        Assert.assertEquals("[,,,,,,,,]",entity4.getString());
    }

    @Test
    public void test_iotAnyVector_allDateType_void_1() throws IOException {
        String script = "if(existsDatabase(\"dfs://IOTDB\")) dropDatabase(\"dfs://IOTDB\")\n" +
                "create database \"dfs://IOTDB\" partitioned by HASH([SYMBOL,10]),VALUE([today()]),engine = \"IOTDB\";\n" +
                "create table \"dfs://IOTDB\".\"data\" (\n" +
                "        Time TIMESTAMP,\n" +
                "        systemID SYMBOL,\n" +
                "        TagName SYMBOL,\n" +
                "        Value IOTANY,\n" +
                ")\n" +
                "partitioned by systemID, Time,\n" +
                "sortColumns = [\"systemID\",\"TagName\",\"Time\"],\n" +
                "latestKeyCache = true\n" +
                "pt = loadTable(\"dfs://IOTDB\",\"data\")\n" +
                "Time = take(2024.01.01T00:00:00,2)\n" +
                "systemID = `0x001`0x002\n" +
                "TagName = `value1`value1\n" +
                "Value = [4,6]\n" +
                "t = table(Time,systemID,TagName,Value)\n" +
                "pt.append!(t)\n" +
                "Time = take(2024.01.01T00:00:00,1)\n" +
                "systemID = [`0x001]\n" +
                "TagName = [`value2]\n" +
                "Value = [true]\n" +
                "t = table(Time,systemID,TagName,Value)\n" +
                "pt.append!(t)\n"  ;
        conn.run(script);
        BasicTable entity1 = (BasicTable)conn.run("select Value from pt pivot by Time,sysTemID,TagName;");
        Assert.assertEquals("[true,]", entity1.getColumn(3).getString());
        Assert.assertEquals("", entity1.getColumn(3).get(1).getString());
        Assert.assertEquals(DT_IOTANY, entity1.getColumn(3).getDataType());
        Assert.assertEquals(DT_VOID, entity1.getColumn(3).get(1).getDataType());
        Map<String, Entity> map = new HashMap<>();
        map.put("iotAny1", entity1);
        conn.upload(map);
        Entity entity3 = conn.run("iotAny1");
        System.out.println(entity3.getString());
        Assert.assertEquals(entity1.getString(),entity3.getString());
    }
    @Test
    public void Test_iotanyvector_upload() throws Exception {
        String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n";
        //conn.run(script);
        BasicByte bbyte = new BasicByte((byte) 127);
        BasicShort bshort = new BasicShort((short) 0);
        BasicInt bint = new BasicInt(-4);
        BasicLong blong = new BasicLong(-4);
        BasicBoolean bbool = new BasicBoolean(false);
        BasicFloat bfloat = new BasicFloat((float) 1.99);
        BasicDouble bdouble = new BasicDouble(1.99);
        BasicString bsting = new BasicString("最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa");
        Scalar[] scalar = new Scalar[]{bbyte, bshort, bint, blong, bbool, bfloat, bdouble, bsting, bdouble, bsting};
        BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);

        BasicIntVector deviceId = new BasicIntVector(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        BasicAnyVector BIV1 = new BasicAnyVector(10);
        for (int i = 0; i < 10; i++) {
            BIV1.set(i,  BIV.get(i));
        }
        BasicAnyVector BIV2 = new BasicAnyVector(10);
        for (int i = 0; i < 10; i++) {
            BIV2.set(i,  new BasicInt(i));
        }
        System.out.println(BIV2.getString());
        System.out.println(BIV2.getDataType());
        BasicTimestampVector timestamp = new BasicTimestampVector(new long[]{1577836800001l, 1577836800002l, 1577836800003l, 1577836800004l, 1577836800005l, 1577836800006l, 1577836800007l, 1577836800008l, 1577836800009l, 1577836800010l});
        BasicSymbolVector location = new BasicSymbolVector(Arrays.asList(new String[]{"d1d", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10"}));

        List<String> colNames = Arrays.asList("deviceId", "timestamp", "location", "BIV2");
        List<Vector> cols = Arrays.asList(deviceId, timestamp, location, deviceId);
        BasicTable table = new BasicTable(colNames, cols);
        List<Entity> argList = new ArrayList<Entity>(1);
        argList.add(table);
        System.out.println(table.getString());
        conn.run(String.format("testTable = loadTable('%s','%s')", "dfs://testIOT", "pt"));
        conn.run("tableInsert{testTable}", argList);

        Entity re = conn.run("pt = loadTable(\"dfs://testIOT\", \"pt\");schema(pt)");
        System.out.println(re.getString());
    }

}
