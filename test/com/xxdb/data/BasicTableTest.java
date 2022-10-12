package com.xxdb.data;

import com.alibaba.fastjson.JSON;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;

import static com.xxdb.data.BasicDecimalTest.HOST;
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
        BasicNanoTimeVector bnanotimev = new BasicNanoTimeVector(vnanotime);
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
        colNames.add("decimal32");
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(4);
        bd32v.set(0,new BasicDecimal32(16,2));
        bd32v.set(1,new BasicDecimal32(25.3,2));
        bd32v.set(2,new BasicDecimal32(32.4,2));
        bd32v.set(3,new BasicDecimal32(66,2));
        BasicArrayVector bd32av = new BasicArrayVector(new int[]{1,2,2,4},bd32v);
        cols.add(bd32av);
        colNames.add("decimal64");
        BasicDecimal64Vector bd64v = new BasicDecimal64Vector(4);
        bd64v.set(0,new BasicDecimal64(31L,4));
        bd64v.set(1,new BasicDecimal64(98.296,4));
        bd64v.set(2,new BasicDecimal64(27.12,4));
        bd64v.set(3,new BasicDecimal64(18L,4));
        BasicArrayVector bd64av = new BasicArrayVector(new int[]{1,2,4,4},bd64v);
        cols.add(bd64av);
        BasicTable bt = new BasicTable(colNames,cols);
        for(int i=0;i<bt.rows();i++){
            assertTrue(isJSON2(bt.getRowJson(i)));
            System.out.println(bt.getRowJson(i));
        }

    }

}
