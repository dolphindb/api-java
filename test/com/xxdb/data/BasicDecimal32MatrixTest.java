package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class BasicDecimal32MatrixTest {

    @Test
    public void test_BasicDecimal32Matrix() throws Exception {
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(0, 0,0);
        System.out.println(re1.getString());
        assertEquals("\n", re1.getString());
        assertEquals(Entity.DATA_CATEGORY.DENARY, re1.getDataCategory());
        assertEquals(BasicDecimal32.class, re1.getElementClass());
        assertEquals(Entity.DATA_TYPE.DT_DECIMAL32, re1.getDataType());

        BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 4,0);
        System.out.println(re2.getString());
        assertEquals("#0 #1 #2 #3\n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n" +
                "0  0  0  0 \n", re2.getString());

        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1, 2, 3});
        list.add(new int[]{4, 5, 6});
        BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 2,list,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"1", "2", "3"});
        list1.add(new String[]{"4", "5", "6"});
        BasicDecimal32Matrix re4 = new BasicDecimal32Matrix(3, 2,list1,2);
        System.out.println(re3.getString());
        assertEquals("#0   #1  \n" +
                "1.00 4.00\n" +
                "2.00 5.00\n" +
                "3.00 6.00\n", re3.getString());
        String exception = null;
        try{
            BasicDecimal32Matrix re5 = new BasicDecimal32Matrix(4, 2,list,2);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The length of array 1 doesn't have 4 elements", exception);
        String exception1 = null;
        try{
            BasicDecimal32Matrix re6 = new BasicDecimal32Matrix(3, 3,list,2);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("input list of arrays does not have 3 columns", exception1);

        List<long[]> list2 = new ArrayList<>();
        list2.add(new long[]{1, 2, 3});
        String exception2 = null;
        try{
            BasicDecimal32Matrix re6 = new BasicDecimal32Matrix(3, 1,list2,2);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("BasicDecimal32Matrix 'listOfArrays' param only support String[] or int[].", exception2);
    }

    @Test
    public void test_BasicDecimal32Matrix_scale_not_true() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{999999999, 0, -999999999});
        list.add(new int[]{999999999, 999999999, 999999999});
        list.add(new int[]{-999999999, -999999999, -999999999});
        String exception = null;
        try{
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3,list,-1);
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].", exception);
        String exception1 = null;
        try{
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3,list,10);
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].", exception1);

        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"999999999", "0", "-999999999"});
        list1.add(new String[]{"999999999", "999999999", "999999999"});
        list1.add(new String[]{"-999999999", "-999999999", "-999999999"});
        String exception2 = null;
        try{
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3,list1,-1);
        }catch(Exception ex){
            exception2 = ex.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].", exception2);
        String exception3 = null;
        try{
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3,list1,10);
        }catch(Exception ex){
            exception3 = ex.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].", exception3);
    }
    @Test
    public void test_BasicDecimal32Matrix_int() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{999999999, 0, -999999999});
        list.add(new int[]{999999999, 999999999, 999999999});
        list.add(new int[]{-999999999, -999999999, -999999999});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0         #1        #2        \n" +
                "999999999  999999999 -999999999\n" +
                "0          999999999 -999999999\n" +
                "-999999999 999999999 -999999999\n", re1.getString());

        List<int[]> list1 = new ArrayList<>();
        list1.add(new int[]{99999, 0, -99999});
        list1.add(new int[]{99999, 99999, 99999});
        list1.add(new int[]{-99999, -99999, -1999});
        BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 3,list1,4);
        System.out.println(re2.getString());
        assertEquals("#0          #1         #2         \n" +
                "99999.0000  99999.0000 -99999.0000\n" +
                "0.0000      99999.0000 -99999.0000\n" +
                "-99999.0000 99999.0000 -1999.0000 \n", re2.getString());

        List<int[]> list2 = new ArrayList<>();
        list2.add(new int[]{1, 0, -1});
        BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 1,list2,9);
        System.out.println(re3.getString());
        assertEquals("#0          \n" +
                "1.000000000 \n" +
                "0.000000000 \n" +
                "-1.000000000\n", re3.getString());
    }
    @Test
    public void test_BasicDecimal32Matrix_string() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"999999999", "0", "-999999999"});
        list.add(new String[]{"999999999", "999999999", "999999999"});
        list.add(new String[]{"-999999999", "-999999999", "-999999999"});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 3,list,0);
        System.out.println(re1.getString());
        assertEquals("#0         #1        #2        \n" +
                "999999999  999999999 -999999999\n" +
                "0          999999999 -999999999\n" +
                "-999999999 999999999 -999999999\n", re1.getString());
        List<String[]> list1 = new ArrayList<>();
        list1.add(new String[]{"99999", "0", "-99999"});
        list1.add(new String[]{"99999", "99999", "99999"});
        list1.add(new String[]{"-99999", "-99999", "-1999"});
        BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 3,list1,4);
        System.out.println(re2.getString());
        assertEquals("#0          #1         #2         \n" +
                "99999.0000  99999.0000 -99999.0000\n" +
                "0.0000      99999.0000 -99999.0000\n" +
                "-99999.0000 99999.0000 -1999.0000 \n", re2.getString());

        List<String[]> list2 = new ArrayList<>();
        list2.add(new String[]{"1", "0", "-1"});
        BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 1,list2,9);
        System.out.println(re3.getString());
        assertEquals("#0          \n" +
                "1.000000000 \n" +
                "0.000000000 \n" +
                "-1.000000000\n", re3.getString());
        List<String[]> list3 = new ArrayList<>();
        list3.add(new String[]{"-1.9999", "0", "-1.00000009","-1.999999999999"});
        list3.add(new String[]{"1.9999", "0", "1.00000009","1.999999999999"});
        list3.add(new String[]{"-0.9999", "0.01", "-0.00000009","-0.999999999999"});
        list3.add(new String[]{"0.9999", "-0.001", "0.00000009","0.999999999999"});
        BasicDecimal32Matrix re4 = new BasicDecimal32Matrix(4, 4,list3,9);
        System.out.println(re4.getString());
        assertEquals("#0           #1          #2           #3          \n" +
                "-1.999900000 1.999900000 -0.999900000 0.999900000 \n" +
                "0.000000000  0.000000000 0.010000000  -0.001000000\n" +
                "-1.000000090 1.000000090 -0.000000090 0.000000090 \n" +
                "-1.999999999 1.999999999 -0.999999999 0.999999999 \n", re4.getString());

        List<String[]> list4 = new ArrayList<>();
        list4.add(new String[]{"0.49","-123.44","132.50","-0.51"});
        BasicDecimal32Matrix re5 = new BasicDecimal32Matrix(4, 1,list4,0);
        System.out.println(re5.getString());
        assertEquals("#0  \n" +
                "0   \n" +
                "-123\n" +
                "133 \n" +
                "-1   \n", re5.getString());
    }

    @Test
    public void test_BasicDecimal32Matrix_setNull() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 1,list,0);
        assertEquals(false, re1.isNull(0,0));
        re1.setNull(0,0);
        assertEquals(true, re1.isNull(0,0));
        //assertEquals("true", re1.getString());
    }

    @Test
    public void test_BasicDecimal32Matrix_set() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2,0);
        assertEquals(new BasicDecimal32(0,0).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal32(0,0).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal32(0,0).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal32(0,0).getString(), re1.get(1,1).getString());
        re1.setScale(6);
        re1.set(0,0,new BasicDecimal32("1.99999",9));
        re1.set(0,1,new BasicDecimal32("-0.99999",0));
        re1.set(1,0,new BasicDecimal32("999.99999",3));
        re1.set(1,1,new BasicDecimal32("-999.99999",6));
        System.out.println(re1.getString());
        assertEquals(6, re1.getScale());
        assertEquals(new BasicDecimal32("1.999990",6).getString(), re1.get(0,0).getString());
        assertEquals(new BasicDecimal32("0.000000",6).getString(), re1.get(0,1).getString());
        assertEquals(new BasicDecimal32("999.999000",6).getString(), re1.get(1,0).getString());
        assertEquals(new BasicDecimal32("-999.999990",6).getString(), re1.get(1,1).getString());
    }

    @Test
    public void test_BasicDecimal32Matrix_set_null() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"999999999","-999999999"});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 1,list,2);
        BasicDecimal32 bd1 = new BasicDecimal32("1.99999",9);
        bd1.setNull();
        re1.set(0 , 0, bd1);
        re1.isNull(0,0);
        BasicDecimal32 bd2 = new BasicDecimal32(Integer.MIN_VALUE,0);
        BasicDecimal32 bd3 = new BasicDecimal32(1,0);
        bd3.setNull();
        re1.set(1 , 0, bd2);
        re1.isNull(1,0);
        assertEquals("", re1.get(0,0).getString());
        assertEquals("", re1.get(1,0).getString());
        assertEquals(-2147483648, re1.get(0,0).getNumber());
        assertEquals(-2147483648, re1.get(1,0).getNumber());
    }

    @Test
    public void test_BasicDecimal32Matrix_set_entity_not_support() throws Exception {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"111", "0", "-111"});
        BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2,0);

        String exception = null;
        try{
            re1.set(0,0,new BasicDecimal64("1.99999",9));
        }catch(Exception ex){
            exception = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal32!", exception);

        String exception1 = null;
        try{
            re1.set(0,0,new BasicDecimal32Vector(1));
        }catch(Exception ex){
            exception1 = ex.getMessage();
        }
        assertEquals("The value type is not BasicDecimal32!", exception1);
    }

}
