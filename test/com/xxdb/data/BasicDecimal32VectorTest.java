package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicDecimal32VectorTest {
    private static DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Before
    public void setUp() throws IOException {
        conn = new DBConnection(false,false,true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to dolphindb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
    @Test
    public void test_BasicDecimal32Vector_scale_not_true() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        String ex = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,10);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
        String ex1 = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex1);
    }

    @Test
    public void test_BasicDecimal32Vector_scale_not_true_1() throws Exception {
        double[] tmp_double_v = {1.1};
        String ex = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,10);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
        String ex1 = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex1);
    }

    @Test
    public void test_BasicDecimal32Vector_list_scale_not_true() throws Exception {
        List<String> tmp_string_v = Arrays.asList("0.0","-123.00432","132.204234","100.0");
        String ex = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,10);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
        String ex1 = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex1);
    }
    @Test
    public void test_BasicDecimal32Vector_scale_not_true_2() throws Exception {
        String ex = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(2,10);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
        String ex1 = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(2,-1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex1);
    }
    @Test
    public void test_BasicDecimal32Vector_scale_not_true_3() throws Exception {
        int[] tmp_int_v = {1};
        String ex = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_int_v,10);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
        String ex1 = null;
        try{
            BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_int_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex1);
    }
    @Test
    public void test_BasicDecimal32Vector_capacity_lt_size() throws Exception {
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(6,1,3);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new BigDecimal("1.111"));
        bbv.set(3, new BigDecimal("-1.111"));
        bbv.set(4, "999.9999");
        bbv.set(5, "-999");
        Assert.assertEquals("[,,1.111,-1.111,999.999,-999.000]", bbv.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_size_capacity_set() throws Exception {
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(6,6,3);
        Assert.assertEquals("[0.000,0.000,0.000,0.000,0.000,0.000]", bbv.getString());
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new BigDecimal("1.111"));
        bbv.set(3, new BigDecimal("-1.111"));
        bbv.set(4, "999.9999");
        bbv.set(5, "-999");
        Assert.assertEquals("[,,1.111,-1.111,999.999,-999.000]", bbv.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_size_capacity_add() throws Exception {
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(0,6,3);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add(null);
        bbv.add(new BigDecimal("1.111"));
        bbv.add(new BigDecimal("-1.111"));
        bbv.add("999.9999");
        bbv.add("-999");
        Assert.assertEquals("[,,1.111,-1.111,999.999,-999.000]", bbv.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_set_type_not_match() throws Exception {
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(1,1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only String, BigDecimal or null is supported.", re);
    }

    @Test
    public void test_BasicDecimal32Vector_add_type_not_match() throws Exception {
        BasicDecimal32Vector bbv = new BasicDecimal32Vector(1,1,1);
        String re = null;
        try{
            bbv.add((Object)1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only String, BigDecimal or null is supported.", re);
    }

    @Test
    public void test_BasicDecimal32Vector_run_vector() throws IOException {
        BasicDecimal32Vector re1 =(BasicDecimal32Vector) conn.run("decimal32([1.232,-12.43,123.53],6)");
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_run_vector_has_NULL() throws IOException {
        BasicDecimal32Vector re1 =(BasicDecimal32Vector) conn.run("decimal32([1.232,-12.43,NULL],6)");
        assertEquals("[1.232000,-12.430000,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_run_vector_all_NULL() throws IOException {
        BasicDecimal32Vector re1 =(BasicDecimal32Vector) conn.run("decimal32([int(),NULL,NULL],6)");
        assertEquals("[,,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_basicFunction() throws Exception {
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(3,2);
        bd32v.set(0,new BasicDecimal32(11,2));
        bd32v.set(1,new BasicDecimal32(17,2));
        assertFalse(bd32v.isNull(1));
        assertEquals(BasicDecimal32.class,bd32v.getElementClass());
        bd32v.setNull(0);
        assertTrue(bd32v.isNull(0));
        bd32v.set(2,new BasicDecimal32("7.3322",4));
        assertEquals("7.33",bd32v.getString(2));
    }

    @Test
    public void test_BasicDecimal32Vector_create_Decimal32Vector() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_create_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_string_scale_0() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,0);
        assertEquals("[0,-123,132,100]",tmp_32_v.getString());

        String[] tmp_string_v1 = {"0.49","-123.44","132.50","-0.51"};
        BasicDecimal32Vector tmp_32_v1 = new BasicDecimal32Vector(tmp_string_v1,0);
        assertEquals("[0,-123,133,-1]",tmp_32_v1.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_string_scale_8() throws Exception {
        String[] tmp_string_v = {"0.0","-1.00000001","1.00000001","9.99999999","-9.99999999"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,8);
        assertEquals("[0.00000000,-1.00000001,1.00000001,9.99999999,-9.99999999]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_create_list_string() throws Exception {
        List<String> tmp_string_v = Arrays.asList("0.0","-123.00432","132.204234","100.0");
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_list_string_scale_0() throws Exception {
        List<String> tmp_string_v = Arrays.asList("0.0","-123.00432","132.204234","100.0");
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,0);
        assertEquals("[0,-123,132,100]",tmp_32_v.getString());

        List<String> tmp_string_v1 = Arrays.asList("0.49","-123.44","132.50","-0.51");
        BasicDecimal32Vector tmp_32_v1 = new BasicDecimal32Vector(tmp_string_v1,0);
        assertEquals("[0,-123,133,-1]",tmp_32_v1.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_list_string_scale_8() throws Exception {
        List<String> tmp_string_v = Arrays.asList("0.0","-1.00000001","1.00000001","9.99999999","-9.99999999");
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,8);
        assertEquals("[0.00000000,-1.00000001,1.00000001,9.99999999,-9.99999999]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_Decimal32Vector_null() throws Exception {
        double[] tmp_double_v = {};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        assertEquals("[]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_create_string_null() throws Exception {
        String[] tmp_string_v = {};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        assertEquals("[]",tmp_32_v.getString());

        String[] tmp_string_v1= new String[]{String.valueOf(Integer.MIN_VALUE), String.valueOf(Integer.MIN_VALUE-1)};
        BasicDecimal32Vector tmp_32_v1 = new BasicDecimal32Vector(tmp_string_v1,4);
        assertEquals("[,]",tmp_32_v1.getString());

    }

    @Test
    public void test_BasicDecimal32Vector_create_list_string_null() throws Exception {
        List<String> tmp_string_v = Arrays.asList("","-123.00432",null,"100.0");
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        assertEquals("[,-123.0043,,100.0000]",tmp_32_v.getString());

        List<String> tmp_string_v1 = new ArrayList<>();
        BasicDecimal32Vector tmp_32_v1 = new BasicDecimal32Vector(tmp_string_v1,4);
        assertEquals("[]",tmp_32_v1.getString());

        List<String> tmp_string_v2 = Arrays.asList(String.valueOf(Integer.MIN_VALUE), String.valueOf(Integer.MIN_VALUE-1));
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_string_v2,4);
        assertEquals("[,]",tmp_32_v2.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_isNull() throws Exception {
        BasicDecimal32Vector re1 =(BasicDecimal32Vector) conn.run("decimal32([1.232,-12.43,NULL],6)");
        assertEquals(true,re1.isNull(2));
        assertEquals(false,re1.isNull(0));
    }

    @Test
    public void test_BasicDecimal32Vector_setNUll() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        tmp_32_v.setNull(2);
        assertEquals("[0.0000,-123.0043,,100.0000]",tmp_32_v.getString());
        assertEquals(true,tmp_32_v.isNull(2));
    }

    @Test
    public void test_BasicDecimal32Vector_get() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 ex = new BasicDecimal32(-123.00432,4);
        assertEquals(ex.getString(),tmp_32_v.get(1).getString());
    }

    @Test
    public void test_BasicDecimal32Vector_set() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 tmp_32 = new BasicDecimal32(3.032,4);
        tmp_32_v.set(0,tmp_32);
        assertEquals("[3.0320,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_set_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 tmp_32 = (BasicDecimal32) conn.run("decimal32(NULL,4)");
        tmp_32_v.set(0,tmp_32);
        assertEquals("[,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_set_int() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 tmp_32 = (BasicDecimal32) conn.run("decimal32(NULL,4)");
        String RE = null;
        try{
            tmp_32_v.set(0,new BasicInt(2));
        }catch(Exception E){
            RE = E.getMessage();
        }
        assertEquals("value type is not BasicDecimal32!",RE);
        String RE1 = null;
        try{
            tmp_32_v.set(0,new BasicIntVector(2));
        }catch(Exception E){
            RE1 = E.getMessage();
        }
        assertEquals("value type is not BasicDecimal32!",RE1);
    }

    @Test
    public void test_BasicDecimal32Vector_set_string() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        tmp_32_v.set(0,new BasicDecimal32("2",2));
        assertEquals("[2.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
        String ex = null;
        try{
            tmp_32_v.set(0,new BasicString("2"));
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("value type is not BasicDecimal32!",ex);
    }

    @Test
    public void test_BasicDecimal32Vector_set_error_scale() throws Exception {
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(2);
        String a = bd32v.getElementClass().getName();
        assertEquals("com.xxdb.data.BasicDecimal32",a);
    }

    @Test
    public void test_BasicDecimal32Vector_getUnitLength() throws Exception {
        BasicDecimal32Vector bd32v = new BasicDecimal32Vector(2);
        int a = bd32v.getUnitLength();
        assertEquals(4,a);
    }


    @Test
    public void test_BasicDecimal32Vector_add() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        tmp_32_v.add(1.11223);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_add_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        tmp_32_v.add("1.11223");
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_32_v.getString());
        tmp_32_v.add("0.0");
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122,0.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_add_0() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        tmp_32_v.add(0.0);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_zero_size_add() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        tmp_32_v.add(12.42);
        assertEquals("[12.4200]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_addRange() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }


    @Test
    public void test_BasicDecimal32Vector_addRange2() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_addRange_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        double[] tmp = new double[]{};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_addRange_string_1() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        tmp_32_v.addRange(tmp_string_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_addRange_string_2() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        tmp_32_v.addRange(tmp_string_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_addRange_string_null() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        String[] tmp_string1_v = {};
        tmp_32_v.addRange(tmp_string1_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_append_scale_notMatch() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 a = new BasicDecimal32(1.11223,2);
        tmp_32_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_append() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_double_v,4);
        BasicDecimal32 a = new BasicDecimal32(1.11223,4);
        tmp_32_v.Append(a);
    }
    @Test
    public void test_BasicDecimal32Vector_append_null() throws Exception {
        BasicDecimal32 basicDecimal32 = new BasicDecimal32(0,0);
        basicDecimal32.setNull();
        BasicDecimal32Vector basicDecimal32Vector = new BasicDecimal32Vector(0,0);
        basicDecimal32Vector.Append(basicDecimal32);
        System.out.println(((Scalar)(basicDecimal32Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal32Vector.get(0))).isNull());
    }

    @Test
    public void test_BasicDecimal32Vector_Append_vector_scale_notMatch() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,3);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_32_v.getString());

    }

    @Test
    public void test_BasicDecimal32Vector_Append_vector() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_append_string_scale_notMatch() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        BasicDecimal32 a = new BasicDecimal32("1.11223",2);
        tmp_32_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_append_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        BasicDecimal32 a = new BasicDecimal32("1.11223",4);
        tmp_32_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_Append_string_vector_scale_notMatch() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_string_v,3);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_32_v.getString());
    }
    @Test
    public void test_BasicDecimal32Vector_Append_string_vector() throws Exception {
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_string_v,4);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal32Vector_getScale() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        int a = tmp_32_v2.getScale();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal32Vector_getDataCategory() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        Entity.DATA_CATEGORY a = tmp_32_v2.getDataCategory();
        assertEquals("DENARY",a.toString());
    }
    @Test
    public void test_BasicDecimal32Vector_getdataArray() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        int[] a = tmp_32_v2.getdataArray();
        assertEquals("[0, -1230043, 1322042, 1000000]",Arrays.toString(a));
    }
    @Test
    public void test_BasicDecimal32Vector_getDataType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        Entity.DATA_TYPE a = tmp_32_v2.getDataType();
        assertEquals("DT_DECIMAL32",a.toString());
    }

    @Test
    public void test_BasicDecimal32Vector_rows() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        int a = tmp_32_v2.rows();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal32Vector_getExtraParamForType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        int a = tmp_32_v2.getExtraParamForType();
        assertEquals(4,a);
    }
    @Test
    public void testBasicDecimal32_run_arrayVector_add() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("arr = array(DECIMAL32(2)[], 0, 10).append!([[9999999.99, NULL, 1000000.01, NULL, -9999998.99, -1000000.01], [], [00i], [1000000.01]]);arr1=add(arr, 1);arr1;");
        assertEquals("[[10000000.99,,1000001.01,,-9999997.99,-999999.01],[],[],[1000001.01]]",re1.getString());
    }
    @Test
    public void testBasicDecimal32Vector_toJsonString() throws IOException {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector re1 = new BasicDecimal32Vector(tmp_string_v,4);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL32\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal32\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":4,\"string\":\"[0.0000,-123.0043,132.2042,100.0000]\",\"table\":false,\"unitLength\":4,\"unscaledValues\":[0,-1230043,1322042,1000000],\"vector\":true}", re);
    }
    @Test
    public void testBasicDecimal32Vector_toJsonString_null() throws IOException {
        BasicDecimal32Vector re1 = new BasicDecimal32Vector(2,2);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL32\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal32\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":2,\"string\":\"[0.00,0.00]\",\"table\":false,\"unitLength\":4,\"unscaledValues\":[0,0],\"vector\":true}", re);
    }
    @Test
    public void testBasicDecimal32Vector_getValue() throws IOException {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector re1 = new BasicDecimal32Vector(tmp_string_v,4);
        int[] re = re1.getUnscaledValues();
        System.out.println(Arrays.toString(re));
        assertEquals("[0, -1230043, 1322042, 1000000]",Arrays.toString(re));
    }
    @Test
    public void test_BasicDecimal32Vector_asof() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        Scalar sc = new BasicDecimal32("1",2);
        assertEquals(0,tmp_32_v.asof(sc));
    }
    @Test
    public void test_BasicDecimal32Vector_combine() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal32Vector tmp_32_v = new BasicDecimal32Vector(tmp_string_v,4);
        BasicDecimal32Vector tmp_32_v1 = new BasicDecimal32Vector(0,4);
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(0,5);
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);

        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.combine(tmp_32_v).getString());
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.combine(tmp_32_v1).getString());
        String re = null;
        try{
            tmp_32_v.combine(tmp_32_v2);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The scale of the vector to be combine does not match the scale of the current vector.",re);
        String re1 = null;
        try{
            tmp_32_v.combine(tmp_64_v);
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        assertEquals("com.xxdb.data.BasicDecimal64Vector cannot be cast to com.xxdb.data.BasicDecimal32Vector",re1);
    }
}
