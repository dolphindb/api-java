package com.xxdb.data;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            assertTrue(bt.getColumn("clong").get(0).isNull());
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
        long[] vnanotimestamp = new long[]{Utils.countNanoseconds(LocalDateTime.of(2018,11,12,9,30,05,123456789)),Utils.countNanoseconds(LocalDateTime.of(2018,11,13,16,30,05,987654321))};
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


    @Test
    public void test_table_copy(){
        BasicTable t1 = createBasicTable();
        BasicTable t2 = createBasicTable();
        BasicTable t3 = t1.combine(t2);
        Assert.assertEquals(t3.rows(),t1.rows()+t2.rows());
    }


}
