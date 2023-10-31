package com.xxdb.data;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;

import static com.xxdb.data.BasicDecimalTest.HOST;
import static com.xxdb.data.BasicDecimalTest.PORT;
import static org.junit.Assert.*;

public class BasicTableTest {
    @Test
    public void test_table_alltype(){
        BasicTable table1 = createBasicTable();
        assertEquals(2,table1.rows());
        assertTrue(((BasicBoolean)table1.getColumn("cbool").get(0)).getBoolean());
    }

    @Test
    public void test_set_nullvalue() throws IOException {
        try{
            List<String> colNames =  Arrays.asList("cint","cdouble","clong","cfloat","cshort");
            List<Vector> colData = Arrays.asList(new BasicIntVector(1),new BasicDoubleVector(1),new BasicLongVector(1),new BasicFloatVector(1),new BasicShortVector(1));
            BasicTable bt = new BasicTable(colNames,colData);
            bt.getColumn("cint").set(0,new BasicInt(Integer.MIN_VALUE));
            bt.getColumn("cdouble").set(0,new BasicDouble(-Double.MIN_VALUE));
            bt.getColumn("clong").set(0,new BasicLong(Long.MIN_VALUE));
            bt.getColumn("cfloat").set(0,new BasicFloat(-Float.MAX_VALUE));
            bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
            bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
            bt.getColumn("cshort").set(0,new BasicShort(Short.MIN_VALUE));
            assertTrue(((Scalar)bt.getColumn("clong").get(0)).isNull());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private	BasicTable createBasicTable(){
        List<String> colNames = new ArrayList<String>();
        colNames.add("cbool");
        colNames.add("cchar");
        colNames.add("cshort");
        colNames.add("cint");
        colNames.add("clong");
        colNames.add("cdate");
        colNames.add("cmonth");
        colNames.add("ctime");
        colNames.add("cminute");
        colNames.add("csecond");
        colNames.add("cdatetime");
        colNames.add("ctimestamp");
        colNames.add("cnanotime");
        colNames.add("cnanotimestamp");
        colNames.add("cfloat");
        colNames.add("cdouble");
        colNames.add("csymbol");
        colNames.add("cstring");
        List<Vector> cols = new ArrayList<Vector>(){};


        //boolean
        byte[] vbool = new byte[]{1,0};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.add(bbv);
        //char
        byte[] vchar = new byte[]{(byte)'c',(byte)'a'};
        BasicByteVector bcv = new BasicByteVector(vchar);
        cols.add(bcv);
        //cshort
        short[] vshort = new short[]{32767,29};
        BasicShortVector bshv = new BasicShortVector(vshort);
        cols.add(bshv);
        //cint
        int[] vint = new int[]{2147483647,483647};
        BasicIntVector bintv = new BasicIntVector(vint);
        cols.add(bintv);
        //clong
        long[] vlong = new long[]{2147483647,483647};
        BasicLongVector blongv = new BasicLongVector(vlong);
        cols.add(blongv);
        //cdate
        int[] vdate = new int[]{Utils.countDays(LocalDate.of(2018,2,14)),Utils.countDays(LocalDate.of(2018,8,15))};
        BasicDateVector bdatev = new BasicDateVector(vdate);
        cols.add(bdatev);
        //cmonth
        int[] vmonth = new int[]{Utils.countMonths(YearMonth.of(2018,2)),Utils.countMonths(YearMonth.of(2018,8))};
        BasicMonthVector bmonthv = new BasicMonthVector(vmonth);
        cols.add(bmonthv);
        //ctime
        int[] vtime = new int[]{Utils.countMilliseconds(16,46,05,123),Utils.countMilliseconds(18,32,05,321)};
        BasicTimeVector btimev = new BasicTimeVector(vtime);
        cols.add(btimev);
        //cminute
        int[] vminute = new int[]{Utils.countMinutes(LocalTime.of(16,30)),Utils.countMinutes(LocalTime.of(9,30))};
        BasicMinuteVector bminutev = new BasicMinuteVector(vminute);
        cols.add(bminutev);
        //csecond
        int[] vsecond = new int[]{Utils.countSeconds(LocalTime.of(9,30,30)),Utils.countSeconds(LocalTime.of(16,30,50))};
        BasicSecondVector bsecondv = new BasicSecondVector(vsecond);
        cols.add(bsecondv);
        //cdatetime
        int[] vdatetime = new int[]{Utils.countSeconds(LocalDateTime.of(2018,9,8,9,30,01)),Utils.countSeconds(LocalDateTime.of(2018,11,8,16,30,01))};
        BasicDateTimeVector bdatetimev = new BasicDateTimeVector(vdatetime);
        cols.add(bdatetimev);
        //ctimestamp
        long[] vtimestamp = new long[]{Utils.countMilliseconds(2018,11,12,9,30,01,123),Utils.countMilliseconds(2018,11,12,16,30,01,123)};
        BasicTimestampVector btimestampv = new BasicTimestampVector(vtimestamp);
        cols.add(btimestampv);
        //cnanotime
        long[] vnanotime = new long[]{Utils.countNanoseconds(LocalTime.of(9,30,05,123456789)),Utils.countNanoseconds(LocalTime.of(16,30,05,987654321))};
        Vector bnanotimev = new BasicNanoTimeVector(vnanotime);
        cols.add(bnanotimev);
        //cnanotimestamp
        long[] vnanotimestamp = new long[]{Utils.countDTNanoseconds(LocalDateTime.of(2018,11,12,9,30,05,123456789)),Utils.countNanoseconds(LocalDateTime.of(2018,11,13,16,30,05,987654321))};
        BasicNanoTimestampVector bnanotimestampv = new BasicNanoTimestampVector(vnanotimestamp);
        cols.add(bnanotimestampv);
        //cfloat
        float[] vfloat = new float[]{2147.483647f,483.647f};
        BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
        cols.add(bfloatv);
        //cdouble
        double[] vdouble = new double[]{214.7483647,48.3647};
        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
        cols.add(bdoublev);
        //csymbol
        String[] vsymbol = new String[]{"GOOG","MS"};
        BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
        cols.add(bsymbolv);
        //cstring
        String[] vstring = new String[]{"stringstringstringstringstringstringstringstringstringstringstringstringstringstringstringstringstringstringstringstring","test string"};
        BasicStringVector bstringv = new BasicStringVector(vstring);
        cols.add(bstringv);
        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }
    public final static Integer ARRAY_NUM = 1000000;
    private	BasicTable createBigArrayTable(){
        List<String> colNames = new ArrayList<String>();
        colNames.add("cbool");
        colNames.add("cchar");
        List<Vector> cols = new ArrayList<Vector>(){};

        //boolean
        byte[] vbool = new byte[ARRAY_NUM];
        byte[] vchar = new byte[ARRAY_NUM];

        for (int i =0;i<ARRAY_NUM;i++){
            vbool[i]  = (byte) (2*Math.random());
            vchar[i] = (byte) new Random().nextInt(62);
        }
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.add(bbv);
        //char
        BasicByteVector bcv = new BasicByteVector(vchar);
        cols.add(bcv);

        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }

    @Test
    public void test_table_copy(){
        BasicTable t1 = createBasicTable();
        BasicTable t2 = createBasicTable();
        BasicTable t3 = t1.combine(t2);
        Assert.assertEquals(t3.rows(),t1.rows()+t2.rows());
        for (int i = 0;i<t3.rows();i++){
            for (int j=0;j<t3.getColumn(i).columns();j++){
                for (int k=0;k<t3.getColumn(i).rows();k++){
                    if (k<t1.rows()) {
                        assertEquals(t1.getColumn(j).get(k).getString(), t3.getColumn(j).get(k).getString());
                    }else{
                        assertEquals(t2.getColumn(j).get(k-(t1.rows())).getString(), t3.getColumn(j).get(k).getString());
                    }
                }
            }
        }
    }

    @Test
    public void testCombineBigArrayTable() throws Exception {
        BasicTable t1 = createBigArrayTable();
        BasicTable t2 = createBigArrayTable();
        long startTime = System.currentTimeMillis();
        t1.combine(t2);
        long endTime = System.currentTimeMillis();
        long timeBigTable =endTime - startTime;

        List<String> colNames = new ArrayList<String>();
        colNames.add("cbool");
        colNames.add("cchar");
        List<Vector> cols = new ArrayList<Vector>(){};
        BasicBooleanVector bbv = new BasicBooleanVector( new byte[ARRAY_NUM*2]);
        BasicByteVector bcv = new BasicByteVector(new byte[ARRAY_NUM*2]);
        cols.add(bbv);
        cols.add(bcv);
        BasicTable t3 = new BasicTable(colNames,cols);
        long  startTime1 = System.currentTimeMillis();
            for (int i = 0;i<2;i++){
                for (int k=0;k<t3.rows();k++){
                   for (int j=0;j<t3.columns();j++){
                        if (k<t1.rows()) {
                            t3.getColumn(j).set(k,(t1.getColumn(j).get(k)));
                        }else{
                            t3.getColumn(j).set(k,(t2.getColumn(j).get(k-(t1.rows()))));
                        }
                    }
                }
            }
            
        long endTime1 = System.currentTimeMillis();
        long timeSimpleTable = endTime1-startTime1;
        long time = timeSimpleTable/timeBigTable;
        System.out.println(time);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicTable_setColCompressTypes_match(){
        BasicTable bt = createBasicTable();
        System.out.println(bt.getString());
        int[] colCompresses = new int[]{1,2,3,4,5};
        bt.setColumnCompressTypes(colCompresses);
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicTable_other(){
        BasicTable bt = createBasicTable();
        int[] colCompresses = new int[]{2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2};
        System.out.println(Arrays.toString(colCompresses));
        bt.setColumnCompressTypes(colCompresses);
    }

    @Test
    public void test_BasicTable_basic(){
        BasicTable bt = createBasicTable();
        assertEquals(Entity.DATA_CATEGORY.MIXED,bt.getDataCategory());
        bt.addColumn(null,null);
        assertNull(bt.getColumn(null));
    }

    @Test
    public void test_BasicTable_Date_ArrayVector_getRowJson() throws Exception {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("id");
        cols.add(new BasicIntVector(new int[]{1,2,3,4}));
        colNames.add("date");
        BasicDateVector bdv = new BasicDateVector(new int[]{13,637,4898,8495,9400,493,8009,1039,938,2748});
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,2,4,10},bdv);
        cols.add(bdav);
        colNames.add("month");
        BasicMonthVector bmv = new BasicMonthVector(new int[]{291,284,2810,102,392,482,1839});
        BasicArrayVector bmav = new BasicArrayVector(new int[]{2,3,6,7},bmv);
        cols.add(bmav);
        colNames.add("time");
        BasicTimeVector btv = new BasicTimeVector(new int[]{940,293,139,589,348,468});
        BasicArrayVector btav = new BasicArrayVector(new int[]{3,4,5,6},btv);
        cols.add(btav);
        colNames.add("minute");
        BasicMinuteVector bmv2 = new BasicMinuteVector(new int[]{1,15,38,98,21,384,892,984,1371,904});
        BasicArrayVector bmav2 = new BasicArrayVector(new int[]{4,6,9,10},bmv2);
        cols.add(bmav2);
        colNames.add("second");
        BasicSecondVector bsv = new BasicSecondVector(new int[]{1440,2880,1560,1676,2022,4859,1020,8923});
        BasicArrayVector bsav = new BasicArrayVector(new int[]{0,3,5,8},bsv);
        cols.add(bsav);
        colNames.add("datetime");
        BasicDateTimeVector bdtv = new BasicDateTimeVector(new int[]{3849,1039,2301,4821,2392,9821,903});
        BasicArrayVector bdtav = new BasicArrayVector(new int[]{1,2,3,7},bdtv);
        cols.add(bdtav);
        colNames.add("timestamp");
        BasicTimestampVector btsv = new BasicTimestampVector(new long[]{47849203,338203920,447490282,484772,5940583,6443982847L,374626,58694873,57694732,69827472,6840392});
        BasicArrayVector btsav = new BasicArrayVector(new int[]{2,5,9,11},btsv);
        cols.add(btsav);
        colNames.add("nanotime");
        BasicNanoTimeVector bntv = new BasicNanoTimeVector(new long[]{4833,593028487,82019282,59481048,482487283,594829,920492030,583849291038L,382847373,482048294,5839490,40229482});
        BasicArrayVector bntav = new BasicArrayVector(new int[]{2,8,9,12},bntv);
        cols.add(bntav);
        colNames.add("nanotimestamp");
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(new long[]{37,4728,293,5920,29448,92854,29482,5938,39203});
        BasicArrayVector bntsav = new BasicArrayVector(new int[]{1,3,4,9},bntsv);
        cols.add(bntsav);
        BasicTable bt = new BasicTable(colNames,cols);
        for(int i=0;i<4;i++){
            assertTrue(isJSON2(bt.getRowJson(i)));
            System.out.println(bt.getRowJson(i));
        }
    }

    public static boolean isJSON2(String str){
        boolean result = false;
        try{
            Object obj = JSON.parse(str);
            result = true;
        }catch (Exception e){
            result = false;
        }
        return result;
    }

    @Test
    public void test_BasicTable_ArrayVector_NonDate_getRowJson() throws Exception {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("id");
        cols.add(new BasicIntVector(new int[]{1,2,3,4}));
        colNames.add("bool");
        BasicBooleanVector bbv = new BasicBooleanVector(new boolean[]{true,true,false,false,false,true,true,false,true});
        BasicArrayVector bbav = new BasicArrayVector(new int[]{1,4,7,9},bbv);
        cols.add(bbav);
        colNames.add("char");
        BasicByteVector byv = new BasicByteVector(new byte[]{'a','b','c','d','f','h','j','k','o','p','x','y','z'});
        BasicArrayVector byav = new BasicArrayVector(new int[]{3,5,7,13},byv);
        cols.add(byav);
        colNames.add("short");
        BasicShortVector bsv = new BasicShortVector(new short[]{1,2,4,5,6,10,11,13});
        BasicArrayVector bsav = new BasicArrayVector(new int[]{2,5,7,8},bsv);
        cols.add(bsav);
        colNames.add("int");
        BasicIntVector biv = new BasicIntVector(new int[]{3,4,5,7,8,10,11,12,13,16,17});
        BasicArrayVector biav = new BasicArrayVector(new int[]{3,5,9,11},biv);
        cols.add(biav);
        colNames.add("long");
        BasicLongVector blv = new BasicLongVector(new long[]{12,13,14,155,156,255,256,257,258,259,371,372});
        BasicArrayVector blav = new BasicArrayVector(new int[]{3,5,10,12},blv);
        cols.add(blav);
        colNames.add("float");
        BasicFloatVector bfv = new BasicFloatVector(new float[]{0.15F, 0.25F, 0.35F, 1.74F,1.84f,2.31F,2.41f,2.51f,3.62f});
        BasicArrayVector bfav = new BasicArrayVector(new int[]{3,5,8,9},bfv);
        cols.add(bfav);
        colNames.add("double");
        BasicDoubleVector bdv = new BasicDoubleVector(new double[]{4.731,4.732,4.733,5.105,5.115,5.125,5.135,6.001,6.102,6.203,7.009});
        BasicArrayVector bdav = new BasicArrayVector(new int[]{3,7,10,11},bdv);
        cols.add(bdav);
        colNames.add("string");
        BasicStringVector bstv = new BasicStringVector(new String[]{"MySQL","Oracle","PostgreSQL","dolphindb"});
        BasicArrayVector bstav = new BasicArrayVector(new int[]{1,2,3,4},bstv);
        cols.add(bstav);
        colNames.add("uuid");
        BasicUuidVector buv = new BasicUuidVector(new Long2[]{new Long2(11,2),new Long2(32,175),new Long2(88,186)});
        BasicArrayVector buav = new BasicArrayVector(new int[]{1,1,2,3},buv);
        cols.add(buav);
        colNames.add("datehour");
        BasicDateHourVector bdhv = new BasicDateHourVector(new int[]{11,12,13,231,241,251,369,379,424,434,454});
        BasicArrayVector bdhav = new BasicArrayVector(new int[]{3,6,8,11},bdhv);
        cols.add(bdhav);
        colNames.add("ipaddr");
        BasicIPAddrVector bipv = new BasicIPAddrVector(new Long2[]{new Long2(980,12),new Long2(11,333),new Long2(15,345),new Long2(1,1997)});
        BasicArrayVector bipav = new BasicArrayVector(new int[]{2,2,3,4},bipv);
        cols.add(bipav);
        colNames.add("int128");
        BasicInt128Vector bi128v = new BasicInt128Vector(new Long2[]{new Long2(16,31),new Long2(1928,12),new Long2(201,2022),new Long2(189,342)});
        BasicArrayVector bi128av = new BasicArrayVector(new int[]{1,3,3,4},bi128v);
        cols.add(bi128av);
        colNames.add("complex");
        BasicComplexVector bcv = new BasicComplexVector(new Double2[]{new Double2(0.31,0.71),new Double2(87.28,28.12),new Double2(35.25,26.12),new Double2(27.09,90.23)});
        BasicArrayVector bcav = new BasicArrayVector(new int[]{1,2,4,4},bcv);
        cols.add(bcav);
        colNames.add("point");
        BasicPointVector bpv = new BasicPointVector(new Double2[]{new Double2(12.02,23.05),new Double2(21.02,32.05),new Double2(0.98,23.10),new Double2(22.45,0.76)});
        BasicArrayVector bpav = new BasicArrayVector(new int[]{1,1,2,4},bpv);
        cols.add(bpav);
//        colNames.add("decimal32");
//        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(4,2);
//        bd32v.set(0,new BasicDecimal32(16,2));
//        bd32v.set(1,new BasicDecimal32(25.3,2));
//        bd32v.set(2,new BasicDecimal32(32.4,2));
//        bd32v.set(3,new BasicDecimal32(66,2));
//        BasicArrayVector bd32av = new BasicArrayVector(new int[]{1,2,2,4},bd32v);
//        cols.add(bd32av);
//        colNames.add("decimal64");
//        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(4,4);
//        bd64v.set(0,new BasicDecimal64(31L,4));
//        bd64v.set(1,new BasicDecimal64(98.296,4));
//        bd64v.set(2,new BasicDecimal64(27.12,4));
//        bd64v.set(3,new BasicDecimal64(18L,4));
//        BasicArrayVector bd64av = new BasicArrayVector(new int[]{1,2,4,4},bd64v);
//        cols.add(bd64av);
        BasicTable bt = new BasicTable(colNames,cols);
        for(int i=0;i<bt.rows();i++){
            assertTrue(isJSON2(bt.getRowJson(i)));
            System.out.println("---------"+bt.getRowJson(i)+"-------");
        }
    }

    @Test
    public void test_BasicTable_getSubTable() {
        List<String> colNames = new ArrayList<>();
        List<Vector> cols = new ArrayList<>();
        colNames.add("bool");
        colNames.add("char");
        colNames.add("short");
        colNames.add("int");
        colNames.add("long");
        colNames.add("date");
        colNames.add("month");
        colNames.add("time");
        colNames.add("minute");
        colNames.add("second");
        colNames.add("datetime");
        colNames.add("timestamp");
        colNames.add("nanotime");
        colNames.add("nanotimestamp");
        colNames.add("float");
        colNames.add("double");
        colNames.add("string");
        colNames.add("uuid");
        colNames.add("datehour");
        colNames.add("ipaddr");
        colNames.add("int128");
        colNames.add("complex");
        colNames.add("point");
        //colNames.add("decimal32");
        //colNames.add("decimal64");
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        BasicByteVector bbyv = new BasicByteVector(1);
        BasicShortVector bsv = new BasicShortVector(1);
        BasicIntVector biv = new BasicIntVector(1);
        BasicLongVector blv = new BasicLongVector(1);
        BasicDateVector bdv = new BasicDateVector(1);
        BasicMonthVector bmv = new BasicMonthVector(1);
        BasicTimeVector btv = new BasicTimeVector(1);
        BasicMinuteVector bmiv = new BasicMinuteVector(1);
        BasicSecondVector bsev = new BasicSecondVector(1);
        BasicDateTimeVector bdtv = new BasicDateTimeVector(1);
        BasicTimestampVector btsv = new BasicTimestampVector(1);
        BasicNanoTimeVector bntv = new BasicNanoTimeVector(1);
        BasicNanoTimestampVector bntsv = new BasicNanoTimestampVector(1);
        BasicFloatVector bfv = new BasicFloatVector(1);
        BasicDoubleVector bdov = new BasicDoubleVector(1);
        BasicStringVector bstv = new BasicStringVector(1);
        BasicUuidVector buv = new BasicUuidVector(1);
        BasicDateHourVector bdhv = new BasicDateHourVector(1);
        BasicIPAddrVector bipv = new BasicIPAddrVector(1);
        BasicInt128Vector bi128v = new BasicInt128Vector(1);
        BasicComplexVector bcv = new BasicComplexVector(1);
        BasicPointVector bpv = new BasicPointVector(1);
        //BasicDecimal32Vector bd32v = new BasicDecimal32Vector(1);
        //BasicDecimal64Vector bd64v = new BasicDecimal64Vector(1);
        for(int i=0;i<1048576;i++){
            bbv.add((byte) (i%2));
            bbyv.add((byte) ('a'+i%26));
            bsv.add((short) (i%255));
            biv.add(i);
            blv.add(i);
            bdv.add(i);
            bmv.add(i);
            btv.add(i);
            bmiv.add(i%1440);
            bsev.add(i%86400);
            bdtv.add(i);
            btsv.add(i);
            bntv.add(i);
            bntsv.add(i);
            bfv.add((float) (i+3.5));
            bdov.add(i+0.75);
            bstv.add(i+"st");
            buv.add(new Long2(i+15,i+1));
            bdhv.add(i);
            bipv.add(new Long2(i+13,i+31));
            bi128v.add(new Long2(i+4,i+19));
            bcv.add(new Double2(i+0.35,i+1.66));
            bpv.add(new Double2(i+1.98,i+0.21));
            //bd32v.add(i);
            //bd64v.add(i);
        }
        cols.add(bbv);
        cols.add(bbyv);
        cols.add(bsv);
        cols.add(biv);
        cols.add(blv);
        cols.add(bdv);
        cols.add(bmv);
        cols.add(btv);
        cols.add(bmiv);
        cols.add(bsev);
        cols.add(bdtv);
        cols.add(btsv);
        cols.add(bntv);
        cols.add(bntsv);
        cols.add(bfv);
        cols.add(bdov);
        cols.add(bstv);
        cols.add(buv);
        cols.add(bdhv);
        cols.add(bipv);
        cols.add(bi128v);
        cols.add(bcv);
        cols.add(bpv);
        //cols.add(bd32v);
        //cols.add(bd64v);
        BasicTable bt = new BasicTable(colNames,cols);
        assertEquals(1048577,bt.rows());
        Table gs = bt.getSubTable(5,15);
        System.out.println(gs.getString());
        Table ge = bt.getSubTable(100000,100100);
        System.out.println(ge.getString());
    }

    @Test(timeout = 70000)
    public void test_BasicTable_GetSubTable_DFS() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        String script = "n=10000000;\n" +
                "bool = take(true false,n);\n" +
                "char = take('a'..'z',n);\n" +
                "short = take(1h..255h,n);\n" +
                "int = take(1..2000,n);\n" +
                "long = take(2000l..5000l,n);\n" +
                "date = take(2012.01.01..2016.12.31,n);\n" +
                "month = take(2012.01M..2021.12M,n);\n" +
                "time = take(01:01:01.001..23:59:59.999,n);\n" +
                "minute = take(01:01m..23:59m,n);\n" +
                "second = take(01:01:01..23:59:59,n);\n" +
                "datetime = take(2022.10.14 01:01:01..2022.10.14 23:59:59,n);\n" +
                "timestamp = take(2022.10.14 01:01:01.001..2022.10.14 23:59:59.999,n);\n" +
                "nanotime = take(14:00:00.000000001..14:00:00.199999999,n);\n" +
                "nanotimestamp = take(2022.10.14 13:39:51.000000001..2022.10.14 13:39:51.199999999,n);\n" +
                "float = rand(33.2f,n);\n" +
                "double = rand(53.1,n);\n" +
                "string = take(\"orcl\" \"APPL\" \"AMZON\" \"GOOG\",n)\n" +
                "datehour = take(datehour(2011.01.01 01:01:01..2011.12.31 23:59:59),n)\n" +
                "t = table(bool,char,short,int,long,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp,float,double,string,datehour);\n" +
                "if(existsDatabase(\"dfs://testSubTable\")){dropDatabase(\"dfs://testSubTable\")}\n" +
                "db = database(\"dfs://testSubTable\",VALUE,1..2000);\n" +
                "pt = db.createPartitionedTable(t,`pt,`int);\n" +
                "pt.append!(t)";
        conn.run(script);
        BasicTable bt = (BasicTable) conn.run("select * from pt;");
        assertEquals(10000000,bt.rows());
        Table gs = bt.getSubTable(0,9999999);
        System.out.println(gs.getString());
    }
    @Test
    public void test_BasicTable_getRowJson()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("share streamTable(100:0, `sym`d1, [SYMBOL, DOUBLE]) as `st;t = table(take(`s1, 10) as `sym,take(1000000000.02, 10) as `d1);st.append!(t);");
        BasicTable bTable = (BasicTable) conn.run("select * from st");
        System.out.println(bTable.getRowJson(0));
        assertEquals("{\"sym\":\"s1\",\"d1\":1000000000.02}",bTable.getRowJson(0));
    }
    @Test
    public void test_BasicTable_symbol_big_data()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        String createData = "symbol1 = table((take(concat(take(`abcd中文123,10000000)), 100)).string().symbol() as id)";
        conn.run(createData);
        long stime = System.currentTimeMillis();
        BasicTable data =  (BasicTable)conn.run("symbol1");
        long etime = System.currentTimeMillis();
        System.out.print("查询耗时："+(etime-stime)+"ms ");
        System.out.println(data.rows() +"条");
    }
    @Test
    public void test_BasicTable_symbol()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        String createData = "t1 = table((take(10111..10211, 10000000)).string().symbol() as id)";
        conn.run(createData);
        long stime = System.currentTimeMillis();
        BasicTable data =  (BasicTable)conn.run("t1");
        long etime = System.currentTimeMillis();
        System.out.print("查询耗时："+(etime-stime)+"ms ");
        System.out.println(data.rows() +"条");
    }
    @Test
    public void test_BasicTable_string()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        String createData = "t1 = table((take(10000000..20000000, 10000000)).string() as id)";
        conn.run(createData);
        long stime = System.currentTimeMillis();
        EntityBlockReader data =  (EntityBlockReader)conn.run("t1", null, 4, 2, 65536, false);
        int rows = 0;
        while(data.hasNext()){
            BasicTable sub = (BasicTable)data.read();
            rows += sub.rows();
        }
        long etime = System.currentTimeMillis();
        System.out.print("查询耗时："+(etime-stime)+"ms ");
        System.out.println(rows +"条");
    }
    @Test
    public void test_BasicTable_toJSONString()throws Exception{
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        conn.run("share streamTable(100:0, `sym`d1, [SYMBOL, DOUBLE]) as `st;t = table(take(`s1, 10) as `sym,take(1000000000.02, 10) as `d1);st.append!(t);");
        BasicTable bTable = (BasicTable) conn.run("select * from st");
        String re = JSONObject.toJSONString(bTable);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"MIXED\",\"dataForm\":\"DF_TABLE\",\"dataType\":\"DT_DICTIONARY\",\"dictionary\":false,\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"sym d1           \\n--- -------------\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\ns1  1000000000.02\\n\",\"table\":true,\"vector\":false}", re);

    }
    @Test
    public void Test_BasicTable_addColumn_same_col() throws Exception {
        BasicTable bt = createBasicTable();
        String re = null;
        byte[] vbool = new byte[]{1,0,0};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        try{
            bt.addColumn("cbool",bbv);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The table already contains column 'cbool'.",re);
    }
    @Test
    public void Test_BasicTable_addColumn_colName_null() throws Exception {
        BasicTable bt = createBasicTable();
        String re = null;
        byte[] vbool = new byte[]{1,0,0};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        try{
            bt.addColumn(null,bbv);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The param 'colName' or 'col' in table cannot be null.",re);
    }
    @Test
    public void Test_BasicTable_addColumn_col_null() throws Exception {
        BasicTable bt = createBasicTable();
        String re = null;
        try{
            bt.addColumn("cbool1",null);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("The param 'colName' or 'col' in table cannot be null.",re);
    }
    @Test
    public void Test_BasicTable_addColumn_dataLength_not_match() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        bbv.setNull(0);
        bbv.add((byte) 1);
//        bbv.add((byte) 0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,true,]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_BOOL() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        bbv.setNull(0);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,true,false]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_CHAR() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicByteVector bbv = new BasicByteVector(1);
        bbv.setNull(0);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1,0]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SHORT() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicShortVector bbv = new BasicShortVector(1);
        bbv.setNull(0);
        bbv.add((short) 32767);
        bbv.add((short) -32767);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,32767,-32767]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_INT() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicIntVector bbv = new BasicIntVector(1);
        bbv.setNull(0);
        bbv.add(12323);
        bbv.add(-99990);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,12323,-99990]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_LONG() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicLongVector bbv = new BasicLongVector(1);
        bbv.setNull(0);
        bbv.add((long) 1343223234);
        bbv.add((long) -987233223);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1343223234,-987233223]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATE() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateVector bbv = new BasicDateVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-2);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1970.01.02,1969.12.30]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_MONTH() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicMonthVector bbv = new BasicMonthVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,0000.02M,0000.01M]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_TIME() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicTimeVector bbv = new BasicTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(2);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00:00:00.001,00:00:00.002]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_MINUTE() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicMinuteVector bbv = new BasicMinuteVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00:01m,00:00m]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SECOND() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicSecondVector bbv = new BasicSecondVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00:00:01,00:00:00]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATETIME() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateTimeVector bbv = new BasicDateTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1970.01.01T00:00:01,1969.12.31T23:59:59]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_TIMESTAMP() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicTimestampVector bbv = new BasicTimestampVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1970.01.01T00:00:00.001,1969.12.31T23:59:59.999]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_NANOTIME() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00:00:00.000000001,00:00:00.000000000]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_NANOTIMESTAMP() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1970.01.01T00:00:00.000000001,1969.12.31T23:59:59.999999999]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_FLOAT() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicFloatVector bbv = new BasicFloatVector(1);
        bbv.setNull(0);
        bbv.add((float) 166666.676);
        bbv.add((float) -3434343.787);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,166666.671875,-3434343.75]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DOUBLE() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDoubleVector bbv = new BasicDoubleVector(1);
        bbv.setNull(0);
        bbv.add((double) 166666.676);
        bbv.add((double) -3434343.787);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,166666.676,-3434343.787]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SYMBOL() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        String[] vsymbol = new String[]{"GOOG","MS",""};
        BasicSymbolVector bbv = new BasicSymbolVector(Arrays.asList(vsymbol));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[GOOG,MS,]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_STRING() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        String[] vs= new String[]{"GOOG","MS",""};
        BasicStringVector bbv = new BasicStringVector(vs);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[GOOG,MS,]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_UUID() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicUuidVector bbv = new BasicUuidVector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00000000-0000-0001-0000-000000000002,ffffffff-ffff-ffff-ffff-fffffffffffe]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATEHOUR() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateHourVector bbv = new BasicDateHourVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1970.01.01T01,1970.01.01T00]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_IPADDR() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicIPAddrVector bbv = new BasicIPAddrVector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[0.0.0.0,0::1:0:0:0:2,ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_INT128() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicInt128Vector bbv = new BasicInt128Vector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,00000000000000010000000000000002,fffffffffffffffffffffffffffffffe]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_BLOB() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        String[] vs= new String[]{"GOOG","MS",""};
        BasicStringVector bbv = new BasicStringVector(vs,true);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[GOOG,MS,]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_COMPLEX() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicComplexVector bbv = new BasicComplexVector(1);
        bbv.setNull(0);
        bbv.add(new Double2((double)1,(double)2));
        bbv.add(new Double2((double)-1.87,(double)-2.99));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1.0+2.0i,-1.87-2.99i]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_POINT() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicPointVector bbv = new BasicPointVector(1);
        bbv.setNull(0);
        bbv.add(new Double2((double)1,(double)2));
        bbv.add(new Double2((double)-1.87,(double)-2.99));
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[(,),(1.0, 2.0),(-1.87, -2.99)]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL32() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(1,5);
        bbv.setNull(0);
        bbv.add("1.33333331");
        bbv.add("-1.300000031");
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1.33333,-1.30000]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL64() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal64Vector bbv = new BasicDecimal64Vector(1,10);
        bbv.setNull(0);
        bbv.add("1.33333331");
        bbv.add("-1.300000031");
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,1.3333333100,-1.3000000310]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL128() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(1,18);
        bbv.setNull(0);
        bbv.add("9999999999.39999999999999999991");
        bbv.add("-99999999999999.9999999000000000000031");
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,9999999999.399999999999999999,-99999999999999.999999900000000000]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_void() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 3 as int1);select * from t");
        System.out.println(bt.getString());
        BasicVoidVector bbv = new BasicVoidVector(3);
        bbv.setNull(0);
        System.out.println(bbv.getString());
        bt.addColumn("col1", bbv);
        System.out.println(bt.getString());
        assertEquals("[,,]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_BOOL_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 as int1);select * from t");
        System.out.println(bt.getString());
        BasicBooleanVector bbv = new BasicBooleanVector(1);
        bbv.setNull(0);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[true,false]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_CHAR_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2 as int1);select * from t");
        System.out.println(bt.getString());
        BasicByteVector bbv = new BasicByteVector(1);
        bbv.setNull(0);
        bbv.add((byte) 1);
        bbv.add((byte) 0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1,0]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SHORT_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicShortVector bbv = new BasicShortVector(1);
        bbv.setNull(0);
        bbv.add((short) 32767);
        bbv.add((short) -32767);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[32767,-32767]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_INT_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicIntVector bbv = new BasicIntVector(1);
        bbv.setNull(0);
        bbv.add(12323);
        bbv.add(-99990);
        System.out.println(bbv.getString());
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        assertEquals("[[],[12323,-99990]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_LONG_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicLongVector bbv = new BasicLongVector(1);
        bbv.setNull(0);
        bbv.add((long) 1343223234);
        bbv.add((long) -987233223);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1343223234,-987233223]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATE_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateVector bbv = new BasicDateVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-2);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1970.01.02,1969.12.30]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_MONTH_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicMonthVector bbv = new BasicMonthVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[0000.02M,0000.01M]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_TIME_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicTimeVector bbv = new BasicTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(2);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00:00:00.001,00:00:00.002]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_MINUTE_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicMinuteVector bbv = new BasicMinuteVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00:01m,00:00m]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SECOND_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicSecondVector bbv = new BasicSecondVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00:00:01,00:00:00]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATETIME_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateTimeVector bbv = new BasicDateTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1970.01.01T00:00:01,1969.12.31T23:59:59]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_TIMESTAMP_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicTimestampVector bbv = new BasicTimestampVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1970.01.01T00:00:00.001,1969.12.31T23:59:59.999]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_NANOTIME_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicNanoTimeVector bbv = new BasicNanoTimeVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00:00:00.000000001,00:00:00.000000000]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_NANOTIMESTAMP_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicNanoTimestampVector bbv = new BasicNanoTimestampVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(-1);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1970.01.01T00:00:00.000000001,1969.12.31T23:59:59.999999999]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_FLOAT_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicFloatVector bbv = new BasicFloatVector(1);
        bbv.setNull(0);
        bbv.add((float) 166666.676);
        bbv.add((float) -3434343.787);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[166666.671875,-3434343.75]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DOUBLE_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDoubleVector bbv = new BasicDoubleVector(1);
        bbv.setNull(0);
        bbv.add((double) 166666.676);
        bbv.add((double) -3434343.787);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[166666.676,-3434343.787]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_SYMBOL_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        String[] vsymbol = new String[]{"GOOG","MS",""};
        BasicSymbolVector bbv = new BasicSymbolVector(Arrays.asList(vsymbol));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[GOOG],[MS,]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_STRING_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        String[] vs= new String[]{"GOOG","MS",""};
        BasicStringVector bbv = new BasicStringVector(vs);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[GOOG],[MS,]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_UUID_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicUuidVector bbv = new BasicUuidVector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00000000-0000-0001-0000-000000000002,ffffffff-ffff-ffff-ffff-fffffffffffe]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DATEHOUR_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDateHourVector bbv = new BasicDateHourVector(1);
        bbv.setNull(0);
        bbv.add(1);
        bbv.add(0);
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1970.01.01T01,1970.01.01T00]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_IPADDR_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicIPAddrVector bbv = new BasicIPAddrVector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[0.0.0.0],[0::1:0:0:0:2,ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_INT128_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicInt128Vector bbv = new BasicInt128Vector(1);
        bbv.setNull(0);
        bbv.add(new Long2((long)1,(long)2));
        bbv.add(new Long2((long)-1,(long)-2));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[00000000000000010000000000000002,fffffffffffffffffffffffffffffffe]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_COMPLEX_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicComplexVector bbv = new BasicComplexVector(1);
        bbv.setNull(0);
        bbv.add(new Double2((double)1,(double)2));
        bbv.add(new Double2((double)-1.87,(double)-2.99));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1.0+2.0i,-1.87-2.99i]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_POINT_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicPointVector bbv = new BasicPointVector(1);
        bbv.setNull(0);
        bbv.add(new Double2((double)1,(double)2));
        bbv.add(new Double2((double)-1.87,(double)-2.99));
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[(,)],[(1.0, 2.0),(-1.87, -2.99)]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL32_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(1,5);
        bbv.setNull(0);
        bbv.add("1.33333331");
        bbv.add("-1.300000031");
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1.33333,-1.30000]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL64_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal64Vector bbv = new BasicDecimal64Vector(1,10);
        bbv.setNull(0);
        bbv.add("1.33333331");
        bbv.add("-1.300000031");
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[1.3333333100,-1.3000000310]]",bt.getColumn(1).getString());
    }
    @Test
    public void Test_BasicTable_addColumn_DECIMAL128_array() throws Exception {
        DBConnection conn = new DBConnection(false, false, false);
        conn.connect(HOST, PORT, "admin", "123456");
        BasicTable bt = (BasicTable) conn.run("t = table(1 2  as int1);select * from t");
        System.out.println(bt.getString());
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(1,18);
        bbv.setNull(0);
        bbv.add("9999999999.39999999999999999991");
        bbv.add("-99999999999999.9999999000000000000031");
        BasicArrayVector bdav = new BasicArrayVector(new int[]{1,3},bbv);
        System.out.println(bdav.getString());
        bt.addColumn("col1", bdav);
        System.out.println(bt.getString());
        assertEquals("[[],[9999999999.399999999999999999,-99999999999999.999999900000000000]]",bt.getColumn(1).getString());
    }
}
