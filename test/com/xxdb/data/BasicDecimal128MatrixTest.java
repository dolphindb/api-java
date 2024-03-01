package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import static org.junit.Assert.assertEquals;

public class BasicDecimal128MatrixTest {
    private static final BigInteger BIGINT_MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");

    @Test
    public void test_BasicDecimal128Matrix() throws Exception {
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(0, 0,0);
        System.out.println(re1.getString());
        assertEquals("\n", re1.getString());
        assertEquals(Entity.DATA_CATEGORY.DENARY, re1.getDataCategory());
        assertEquals(BasicDecimal128.class, re1.getElementClass());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL128, re1.getDataType());

        BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 4,0);
        System.out.println(re2.getString());
        assertEquals("#0 #1 #2 #3\n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n", re2.getString());

        List<BigInteger[]> list = new ArrayList<>();
        list.add(new BigInteger[]{BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3)});
        list.add(new BigInteger[]{BigInteger.valueOf(4), BigInteger.valueOf(5), BigInteger.valueOf(6)});
        BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 2,list,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"1", "2", "3"});
        list1.add(new String[]{"4", "5", "6"});
        BasicDecimal128Matrix re4 = new BasicDecimal128Matrix(3, 2,list1,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        String exception = null;
        try{
            BasicDecimal128Matrix re5 = new BasicDecimal128Matrix(4, 2,list,2);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The length of array 1 doesn't have 4 elements", exception);
        String exception1 = null;
        try{
            BasicDecimal128Matrix re6 = new BasicDecimal128Matrix(3, 3,list,2);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("input list of arrays does not have 3 columns", exception1);

        List<long[]> list2 = new ArrayList<>();
        list2.add(new long[]{1, 2, 3});
        String exception2 = null;
        try{
            BasicDecimal128Matrix re6 = new BasicDecimal128Matrix(3, 1,list2,2);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("BasicDecimal128Matrix 'listOfArrays' param only support String[] or BigInteger[].", exception2);
    }

    @Test
    public void test_BasicDecimal128Matrix_scale_not_true() throws Exception {
        List<BigInteger[]> list = new ArrayList<>();
        list.add(new BigInteger[]{BigInteger.valueOf(999999999), BigInteger.valueOf(0), BigInteger.valueOf(-999999999)});
        list.add(new BigInteger[]{BigInteger.valueOf(999999999), BigInteger.valueOf(999999999), BigInteger.valueOf(999999999)});
        list.add(new BigInteger[]{BigInteger.valueOf(-999999999), BigInteger.valueOf(-999999999), BigInteger.valueOf(-999999999)});
        String exception = null;
        try{
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3,list,-1);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].", exception);
        String exception1 = null;
        try{
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3,list,39);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].", exception1);

        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"999999999", "0", "-999999999"});
        list1.add(new String[]{"999999999", "999999999", "999999999"});
        list1.add(new String[]{"-999999999", "-999999999", "-999999999"});
        String exception2 = null;
        try{
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3,list1,-1);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].", exception2);
        String exception3 = null;
        try{
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3,list1,39);
        }catch(Exception ex){
            exception3 = ex.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].", exception3);
    }
    @Test
    public void test_BasicDecimal128Matrix_BigInteger() throws Exception {
        List<BigInteger[]> list = new ArrayList<>();
        list.add(new BigInteger[]{new BigInteger("99999999999999999999999999999999999999"), new BigInteger("0"), new BigInteger("-99999999999999999999999999999999999999")});
        list.add(new BigInteger[]{new BigInteger("99999999999999999999999999999999999999"), new BigInteger("99999999999999999999999999999999999999"), new BigInteger("99999999999999999999999999999999999999")});
        list.add(new BigInteger[]{new BigInteger("-99999999999999999999999999999999999999"), new BigInteger("-99999999999999999999999999999999999999"), new BigInteger("-99999999999999999999999999999999999999")});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0                                      #1                                     #2                                     \n" +
                "99999999999999999999999999999999999999  99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                "0                                       99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                "-99999999999999999999999999999999999999 99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n", re1.getString());

        List<BigInteger[]> list1 = new ArrayList<>();
        list1.add(new BigInteger[]{new BigInteger("9999999999999999999"), new BigInteger("0"), new BigInteger("-9999999999999999999")});
        list1.add(new BigInteger[]{new BigInteger("9999999999999999999"), new BigInteger("9999999999999999999"), new BigInteger("9999999999999999999")});
        list1.add(new BigInteger[]{new BigInteger("-9999999999999999999"), new BigInteger("-9999999999999999999"), new BigInteger("-9999999999999999999")});
        BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 3,list1,19);
        System.out.println(re2.getString());
        assertEquals("#0                                       #1                                      #2                                      \n" +
                "9999999999999999999.0000000000000000000  9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                "0.0000000000000000000                    9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                "-9999999999999999999.0000000000000000000 9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n", re2.getString());

        List<BigInteger[]> list2 = new ArrayList<>();
        list2.add(new BigInteger[]{BigInteger.valueOf(1), BigInteger.valueOf(0), BigInteger.valueOf(-1)});
        BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 1,list2,38);
        System.out.println(re3.getString());
        assertEquals("#0                                       \n" +
                "1.00000000000000000000000000000000000000 \n" +
                "0.00000000000000000000000000000000000000 \n" +
                "-1.00000000000000000000000000000000000000\n", re3.getString());
    }
    @Test
    public void test_BasicDecimal128Matrix_string() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"99999999999999999999999999999999999999", "0", "-99999999999999999999999999999999999999"});
        list.add(new String[]{"99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "99999999999999999999999999999999999999"});
        list.add(new String[]{"-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999"});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0                                      #1                                     #2                                     \n" +
                "99999999999999999999999999999999999999  99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                "0                                       99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                "-99999999999999999999999999999999999999 99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n", re1.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"9999999999999999999.9999999999999999999", "0", "-9999999999999999999"});
        list1.add(new String[]{"9999999999999999999.0000000000000000009", "9999999999999999999", "9999999999999999999"});
        list1.add(new String[]{"-9999999999999999999.0000000000000000009", "-9999999999999999999", "-9999999999999999999"});
        BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 3,list1,19);
        System.out.println(re2.getString());
        assertEquals("#0                                       #1                                      #2                                      \n" +
                "9999999999999999999.9999999999999999999  9999999999999999999.0000000000000000009 -9999999999999999999.0000000000000000009\n" +
                "0.0000000000000000000                    9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                "-9999999999999999999.0000000000000000000 9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n", re2.getString());

        List<String[]> list2 = new ArrayList<>();
        list2.add(new String[]{"1", "0", "-1"});
        BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 1,list2,38);
        System.out.println(re3.getString());
        assertEquals("#0                                       \n" +
                "1.00000000000000000000000000000000000000 \n" +
                "0.00000000000000000000000000000000000000 \n" +
                "-1.00000000000000000000000000000000000000\n", re3.getString());
        List<String[]> list3 = new ArrayList<>();
        list3.add(new String[]{"-1.9999", "0", "-1.00000009","-1.999999999999"});
        list3.add(new String[]{"1.9999", "0", "1.00000009","1.999999999999"});
        list3.add(new String[]{"-0.9999", "0.01", "-0.00000009","-0.999999999999"});
        list3.add(new String[]{"0.9999", "-0.001", "0.00000009","0.999999999999"});
        BasicDecimal128Matrix re4 = new BasicDecimal128Matrix(4, 4,list3,9);
        System.out.println(re4.getString());
        assertEquals("#0           #1          #2           #3          \n" +
                "-1.999900000 1.999900000 -0.999900000 0.999900000 \n" +
                "0.000000000  0.000000000 0.010000000  -0.001000000\n" +
                "-1.000000090 1.000000090 -0.000000090 0.000000090 \n" +
                "-1.999999999 1.999999999 -0.999999999 0.999999999 \n", re4.getString());

        List<String[]> list4 = new ArrayList<>();
        list4.add(new String[]{"0.49","-123.44","132.50","-0.51"});
        BasicDecimal128Matrix re5 = new BasicDecimal128Matrix(4, 1,list4,0);
        System.out.println(re5.getString());
        assertEquals("#0  \n" +
                "0   \n" +
                "-123\n" +
                "133 \n" +
                "-1   \n", re5.getString());
    }

    @Test
    public void test_BasicDecimal128Matrix_setNull() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 1,list,0);
        assertEquals(false, re1.isNull(0,0));
        re1.setNull(0,0);
        assertEquals(true, re1.isNull(0,0));
        assertEquals("", re1.get(0,0).getString());
    }

    @Test
    public void test_BasicDecimal128Matrix_set() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2,0);
        assertEquals(new BasicDecimal128("0",0).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal128("0",0).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal128("0",0).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal128("0",0).getString(), re1.get(1,1).getString());
        re1.setScale(6);
        re1.set(0,0,new BasicDecimal128("1.99999",9));
        re1.set(0,1,new BasicDecimal128("-0.99999",0));
        re1.set(1,0,new BasicDecimal128("999.99999",3));
        re1.set(1,1,new BasicDecimal128("-999.99999",6));
        System.out.println(re1.getString());
        assertEquals(6, re1.getScale());
        assertEquals(new BasicDecimal128("1.999990",6).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal128("0.000000",6).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal128("999.999000",6).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal128("-999.999990",6).getString(), re1.get(1,1).getString());
    }

    @Test
    public void test_BasicDecimal128Matrix_set_null() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"999999999","-999999999"});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 1,list,2);
        BasicDecimal128 bd1 = new BasicDecimal128("1.99999",9);
        bd1.setNull();
        re1.set(0 , 0, bd1);
        re1.isNull(0,0);
        BasicDecimal128 bd2 = new BasicDecimal128(BIGINT_MIN_VALUE,0);
        re1.set(1 , 0, bd2);
        re1.isNull(1,0);
        System.out.println(re1.getString());
        System.out.println(re1.get(0,0));

        assertEquals("", re1.get(0,0).getString());
        assertEquals("", re1.get(1,0).getString());
    }

    @Test
    public void test_BasicDecimal128Matrix_set_entity_not_support() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2,0);

        String exception = null;
        try{
            re1.set(0,0,new BasicDecimal64("1.99999",9));
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal128!", exception);

        String exception1 = null;
        try{
            re1.set(0,0,new BasicDecimal128Vector(1,1));
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal128!", exception1);
    }

}
