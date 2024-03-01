package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicDecimal64MatrixTest {

    @Test
    public void test_BasicDecimal64Matrix() throws Exception {
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(0, 0,0);
        System.out.println(re1.getString());
        assertEquals("\n", re1.getString());
        assertEquals(Entity.DATA_CATEGORY.DENARY, re1.getDataCategory());
        assertEquals(BasicDecimal64.class, re1.getElementClass());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL64, re1.getDataType());

        BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 4,0);
        System.out.println(re2.getString());
        assertEquals("#0 #1 #2 #3\n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n", re2.getString());

        List<long[]> list = new ArrayList<>();
        list.add(new long[]{1, 2, 3});
        list.add(new long[]{4, 5, 6});
        BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 2,list,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"1", "2", "3"});
        list1.add(new String[]{"4", "5", "6"});
        BasicDecimal64Matrix re4 = new BasicDecimal64Matrix(3, 2,list1,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        String exception = null;
        try{
            BasicDecimal64Matrix re5 = new BasicDecimal64Matrix(4, 2,list,2);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The length of array 1 doesn't have 4 elements", exception);
        String exception1 = null;
        try{
            BasicDecimal64Matrix re6 = new BasicDecimal64Matrix(3, 3,list,2);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("input list of arrays does not have 3 columns", exception1);

        List<int[]> list2 = new ArrayList<>();
        list2.add(new int[]{1, 2, 3});
        String exception2 = null;
        try{
            BasicDecimal64Matrix re6 = new BasicDecimal64Matrix(3, 1,list2,2);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("BasicDecimal64Matrix 'listOfArrays' param only support String[] or long[].", exception2);
    }

    @Test
    public void test_BasicDecimal64Matrix_scale_not_true() throws Exception {
        List<long[]> list = new ArrayList<>();
        list.add(new long[]{999999999, 0, -999999999});
        list.add(new long[]{999999999, 999999999, 999999999});
        list.add(new long[]{-999999999, -999999999, -999999999});
        String exception = null;
        try{
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3,list,-1);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,18].", exception);
        String exception1 = null;
        try{
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3,list,19);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("Scale 19 is out of bounds, it must be in [0,18].", exception1);

        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"999999999999999999", "0", "-999999999999999999"});
        list1.add(new String[]{"999999999999999999", "999999999999999999", "999999999999999999"});
        list1.add(new String[]{"-999999999999999999", "-999999999999999999", "-999999999999999999"});
        String exception2 = null;
        try{
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3,list1,-1);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,18].", exception2);
        String exception3 = null;
        try{
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3,list1,19);
        }catch(Exception ex){
            exception3 = ex.getMessage();
        }
        assertEquals("Scale 19 is out of bounds, it must be in [0,18].", exception3);
    }
    @Test
    public void test_BasicDecimal64Matrix_long() throws Exception {
        List<long[]> list = new ArrayList<>();
        list.add(new long[]{999999999999999999l, 0, -999999999999999999l});
        list.add(new long[]{999999999999999999l, 999999999999999999l, 999999999999999999l});
        list.add(new long[]{-999999999999999999l, -999999999999999999l, -999999999999999999l});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0                  #1                 #2                 \n" +
                "999999999999999999  999999999999999999 -999999999999999999\n" +
                "0                   999999999999999999 -999999999999999999\n" +
                "-999999999999999999 999999999999999999 -999999999999999999\n", re1.getString());

        List<long[]> list1 = new ArrayList<>();
        list1.add(new long[]{99999, 0, -99999});
        list1.add(new long[]{99999, 99999, 99999});
        list1.add(new long[]{-99999, -99999, -1999});
        BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 3,list1,13);
        System.out.println(re2.getString());
        assertEquals("#0                   #1                  #2                  \n" +
                "99999.0000000000000  99999.0000000000000 -99999.0000000000000\n" +
                "0.0000000000000      99999.0000000000000 -99999.0000000000000\n" +
                "-99999.0000000000000 99999.0000000000000 -1999.0000000000000 \n", re2.getString());

        List<long[]> list2 = new ArrayList<>();
        list2.add(new long[]{1, 0, -1});
        BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 1,list2,18);
        System.out.println(re3.getString());
        assertEquals("#0                   \n" +
                "1.000000000000000000 \n" +
                "0.000000000000000000 \n" +
                "-1.000000000000000000\n", re3.getString());
    }
    @Test
    public void test_BasicDecimal64Matrix_string() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"999999999999999999", "0", "-999999999999999999"});
        list.add(new String[]{"999999999999999999", "999999999999999999", "999999999999999999"});
        list.add(new String[]{"-999999999999999999", "-999999999999999999", "-999999999999999999"});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0                  #1                 #2                 \n" +
                "999999999999999999  999999999999999999 -999999999999999999\n" +
                "0                   999999999999999999 -999999999999999999\n" +
                "-999999999999999999 999999999999999999 -999999999999999999\n", re1.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"99999", "0", "-99999"});
        list1.add(new String[]{"99999", "99999", "99999"});
        list1.add(new String[]{"-99999", "-99999", "-1999"});
        BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 3,list1,13);
        System.out.println(re2.getString());
        assertEquals("#0                   #1                  #2                  \n" +
                "99999.0000000000000  99999.0000000000000 -99999.0000000000000\n" +
                "0.0000000000000      99999.0000000000000 -99999.0000000000000\n" +
                "-99999.0000000000000 99999.0000000000000 -1999.0000000000000 \n", re2.getString());

        List<String[]> list2 = new ArrayList<>();
        list2.add(new String[]{"1", "0", "-1"});
        BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 1,list2,18);
        System.out.println(re3.getString());
        assertEquals("#0                   \n" +
                "1.000000000000000000 \n" +
                "0.000000000000000000 \n" +
                "-1.000000000000000000\n", re3.getString());
        List<String[]> list3 = new ArrayList<>();
        list3.add(new String[]{"-1.9999", "0", "-1.00000000000009","-1.999999999999999999999999"});
        list3.add(new String[]{"1.9999", "0", "1.00000000009","1.999999999999999999999999"});
        list3.add(new String[]{"-0.9999", "0.01", "-0.00000000009","-0.999999999999999999999999"});
        list3.add(new String[]{"0.9999", "-0.001", "0.00000000000000000009","0.999999999999999999999999"});
        BasicDecimal64Matrix re4 = new BasicDecimal64Matrix(4, 4,list3,18);
        System.out.println(re4.getString());
        assertEquals("#0                    #1                   #2                    #3                   \n" +
                "-1.999900000000000000 1.999900000000000000 -0.999900000000000000 0.999900000000000000 \n" +
                "0.000000000000000000  0.000000000000000000 0.010000000000000000  -0.001000000000000000\n" +
                "-1.000000000000090000 1.000000000090000000 -0.000000000090000000 0.000000000000000000 \n" +
                "-1.999999999999999999 1.999999999999999999 -0.999999999999999999 0.999999999999999999 \n", re4.getString());
    }

    @Test
    public void test_BasicDecimal64Matrix_setNull() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 1,list,0);
        assertEquals(false, re1.isNull(0,0));
        re1.setNull(0,0);
        assertEquals(true, re1.isNull(0,0));
        assertEquals("", re1.get(0,0).getString());
    }

    @Test
    public void test_BasicDecimal64Matrix_set() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2,0);
        assertEquals(new BasicDecimal64(0L,0).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal64(0L,0).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal64(0L,0).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal64(0L,0).getString(), re1.get(1,1).getString());
        re1.setScale(6);
        re1.set(0,0,new BasicDecimal64("1.99999",9));
        re1.set(0,1,new BasicDecimal64("-0.99999",0));
        re1.set(1,0,new BasicDecimal64("999.99999",3));
        re1.set(1,1,new BasicDecimal64("-999.99999",6));
        System.out.println(re1.getString());
        assertEquals(6, re1.getScale());
        assertEquals(new BasicDecimal64("1.999990",6).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal64("0.000000",6).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal64("999.999000",6).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal64("-999.999990",6).getString(), re1.get(1,1).getString());
        re1.setScale(10);
        re1.set(0,0,new BasicDecimal64("1.999900009",9));
        re1.set(0,1,new BasicDecimal64("-0.999000099",0));
        re1.set(1,0,new BasicDecimal64("99999999.99999",3));
        re1.set(1,1,new BasicDecimal64("-99999999.99999",6));
        System.out.println(re1.getString());
        assertEquals(10, re1.getScale());
        assertEquals(new BasicDecimal64("1.9999000090",10).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal64("-0.0000000000",10).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal64("99999999.9990000000",10).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal64("-99999999.9999900000",10).getString(), re1.get(1,1).getString());
    }

    @Test
    public void test_BasicDecimal64Matrix_set_null() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"999999999","-999999999"});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 1,list,2);
        BasicDecimal64 bd1 = new BasicDecimal64("1.99999",9);
        bd1.setNull();
        re1.set(0 , 0, bd1);
        re1.isNull(0,0);
        BasicDecimal64 bd2 = new BasicDecimal64(Long.MIN_VALUE,0);
        re1.set(1 , 0, bd2);
        re1.isNull(1,0);
        assertEquals("", re1.get(0,0).getString());
        assertEquals("", re1.get(1,0).getString());
    }

    @Test
    public void test_BasicDecimal64Matrix_set_entity_not_support() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2,0);

        String exception = null;
        try{
            re1.set(0,0,new BasicDecimal128("1.99999",9));
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal64!", exception);

        String exception1 = null;
        try{
            re1.set(0,0,new BasicDecimal64Vector(1));
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal64!", exception1);
    }

}
