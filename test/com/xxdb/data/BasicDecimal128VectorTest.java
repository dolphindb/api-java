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

public class BasicDecimal128VectorTest {
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
    public void test_BasicDecimal128Vector_scale_not_true() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,39);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].",ex);
        String ex1 = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,-1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].",ex1);
    }
    @Test
    public void test_BasicDecimal128Vector_scale_not_true_1() throws Exception {
        BigInteger[] tmp_string_v = {new BigInteger("1")};
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v,39);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].",ex);
        String ex1 = null;
        try{
            BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].",ex1);
    }

    @Test
    public void test_BasicDecimal128Vector_list_scale_not_true() throws Exception {
        List<String> tmp_string_v =  Arrays.asList("0.0","-123.00432","132.204234","100.0");
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,39);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].",ex);
        String ex1 = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,-1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].",ex1);
    }

    @Test
    public void test_BasicDecimal128Vector_scale_not_true_2() throws Exception {
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(2,39);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].",ex);
        String ex1 = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(2,-1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].",ex1);
    }
    @Test//AJ-583
    public void test_BasicDecimal128Vector_dataValue_not_true() throws Exception {
        String[] tmp_string_v = {"-170141183460469231731687303715884105729"};
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,5);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Decimal128 overflow -170141183460469231731687303715884105729.00000",ex);
        String ex1 = null;
        String[] tmp_string_v1 = {"170141183460469231731687303715884105729"};
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v1,5);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Decimal128 overflow 170141183460469231731687303715884105729.00000",ex1);
    }

    @Test
    public void test_BasicDecimal128Vector_dataValue_not_true_1() throws Exception {
        BigInteger[] tmp_string_v = {new BigInteger("-170141183460469231731687303715884105729")};
        String ex = null;
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,5);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Decimal128 -170141183460469231731687303715884105729 cannot be less than -170141183460469231731687303715884105728",ex);
        String ex1 = null;

        BigInteger[] tmp_string_v1 = {new BigInteger("170141183460469231731687303715884105729")};
        try{
            BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v1,5);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Decimal128 170141183460469231731687303715884105729 cannot exceed 170141183460469231731687303715884105728",ex1);
    }

    @Test
    public void test_BasicDecimal128Vector_capacity_lt_size() throws Exception {
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(6,1,3);
        bbv.set(0, (Object)null);
        bbv.set(1, null);
        bbv.set(2, new BigDecimal("1.111"));
        bbv.set(3, new BigDecimal("-1.111"));
        bbv.set(4, "999.9999");
        bbv.set(5, "-999");
        Assert.assertEquals("[,,1.111,-1.111,999.999,-999.000]", bbv.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_size_capacity_set() throws Exception {
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(6,6,3);
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
    public void test_BasicDecimal128Vector_size_capacity_add() throws Exception {
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(0,6,3);
        Assert.assertEquals("[]", bbv.getString());
        bbv.add((Object)null);
        bbv.add((String) null);
        bbv.add(new BigDecimal("1.111"));
        bbv.add(new BigDecimal("-1.111"));
        bbv.add("999.9999");
        bbv.add("-999");
        Assert.assertEquals("[,,1.111,-1.111,999.999,-999.000]", bbv.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_set_type_not_match() throws Exception {
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(1,1,1);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only String, BigDecimal or null is supported.", re);
    }

    @Test
    public void test_BasicDecimal128Vector_add_type_not_match() throws Exception {
        BasicDecimal128Vector bbv = new BasicDecimal128Vector(1,1,1);
        String re = null;
        try{
            bbv.add((Object)1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("Unsupported type: java.lang.Integer. Only String, BigDecimal or null is supported.", re);
    }

    @Test
    public void test_BasicDecimal128Vector_run_vector() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([1.232,-12.43,123.53],6)");
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_run_vector_has_NULL() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([1.232,-12.43,NULL],6)");
        assertEquals("[1.232000,-12.430000,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_run_vector_all_NULL() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([int(),NULL,NULL],6)");
        assertEquals("[,,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_basicFunction() throws Exception {
        BasicDecimal128Vector bd128v = new BasicDecimal128Vector(2,2);
        bd128v.set(0,new BasicDecimal128("11.0",2));
        bd128v.set(1,new BasicDecimal128("17.0",2));
        assertFalse(bd128v.isNull(1));
        assertEquals(BasicDecimal128.class,bd128v.getElementClass());
        bd128v.setNull(0);
        assertTrue(bd128v.isNull(0));
    }
    @Test
    public void test_BasicDecimal128_isNull() throws Exception{
        DBConnection connection = new DBConnection(false, false, false);
        connection.connect(HOST, PORT, "admin", "123456");
        BasicDecimal128Vector b128v1 = (BasicDecimal128Vector) connection.run("decimal128(1..10, 4)");
        BasicDecimal128Vector b128v2 = (BasicDecimal128Vector) connection.run("decimal128(1..5, 4)");
        Vector b1 = b128v1.getSubVector(new int[]{1,3,4});
        assertFalse(b128v1.isNull(1));
        b128v1.setNull(1);
        assertTrue(b128v1.isNull(1));
        BasicDecimal128 b32 = new BasicDecimal128("11", 4);
        b32.setNull();
        b128v1.set(2, b32);
        assertTrue(b128v1.isNull(2));
        BasicDecimal128 bd = new BasicDecimal128("1", 2);
        try {
            b128v1.set(0, b32);
        }catch (Exception e){
            assertEquals("Value's scale is not the same as the vector's!", e.getMessage());
        }
    }

    @Test
    public void test_BasicDecimal128Vector_create_Decimal128Vector() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());

        String[] tmp_string_v1 = {"0.49","-123.49","132.99","-0.51"};
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v1,0);
        assertEquals("[0,-123,133,-1]",tmp_128_v1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_create_Decimal128Vector_null() throws Exception {
        String[] tmp_string_v = {};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        assertEquals("[]",tmp_128_v.getString());
        BigInteger[] tmp_string_v1 = {};
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v1,4);
        assertEquals("[]",tmp_128_v1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_create_string_list() throws Exception {
        List<String>  tmp_string_v1 =  Arrays.asList("0.0","-123.00432","132.204234","100.0");
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v1,0);
        assertEquals("[0,-123,132,100]",tmp_128_v1.getString());

        List<String>  tmp_string_v =  Arrays.asList("0.0","-123.00432","132.204234","100.0");
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());

        List<String>  tmp_string_v2 =  Arrays.asList("0.1","-3.00432","1.204234","0.0");
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v2,37);
        assertEquals("[0.1000000000000000000000000000000000000,-3.0043200000000000000000000000000000000,1.2042340000000000000000000000000000000,0.0000000000000000000000000000000000000]",tmp_128_v2.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_create_string_list_null() throws Exception {
        List<String> tmp_string_v = Arrays.asList("","-123.00432",null,"100.0");
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        assertEquals("[,-123.0043,,100.0000]",tmp_128_v.getString());

        List<String> tmp_string_v1 = new ArrayList<>();
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v1,4);
        assertEquals("[]",tmp_128_v1.getString());

        List<String> tmp_string_v2 = Arrays.asList(  "-170141183460469231731687303715884105728", "-170141183460469231731687303715884105728");
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v2,0);
        assertEquals("[,]",tmp_128_v2.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_isNull() throws Exception {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([1.232,-12.43,NULL],6)");
        assertEquals(true,re1.isNull(2));
        assertEquals(false,re1.isNull(0));
        BigInteger[] tmp_string_v1 = new BigInteger[5];
        tmp_string_v1[0] = BigInteger.ONE;
        tmp_string_v1[1] = null;
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(tmp_string_v1,4);
        assertEquals("[0.0001,0.0000,0.0000,0.0000,0.0000]",tmp_128_v1.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_setNUll() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.setNull(2);
        assertEquals("[0.0000,-123.0043,,100.0000]",tmp_128_v.getString());
        assertEquals(true,tmp_128_v.isNull(2));
    }
    @Test
    public void test_BasicDecimal128Vector_set_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.set(0,new BasicDecimal128("2",2));
        assertEquals("[2.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
        String ex = null;
        try{
            tmp_128_v.set(0,new BasicString("2"));
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("The value type is not BasicDecimal128!",ex);
    }
    @Test
    public void test_BasicDecimal128Vector_new() throws Exception {
        BasicDecimal128Vector v=new BasicDecimal128Vector(2,2);
        System.out.println(v.getString());
        BasicDecimal128 b=new BasicDecimal128("-1441050.00",0);
        System.out.println(b.getString());
        BasicDecimal128 c=new BasicDecimal128("-1441050.00",2);
        System.out.println(c.getString());

    }
    @Test
    public void test_BasicDecimal128Vector_get() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 ex = new BasicDecimal128("-123.00432",4);
        assertEquals(ex.getString(),tmp_128_v.get(1).getString());
    }

    @Test
    public void test_BasicDecimal128Vector_set() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 tmp_32 = new BasicDecimal128("3.032",4);
        tmp_128_v.set(0,tmp_32);
        assertEquals("[3.0320,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_set_null() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 tmp_32 = (BasicDecimal128) conn.run("decimal128(NULL,4)");
        tmp_128_v.set(0,tmp_32);
        assertEquals("[,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_set_int() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        String RE = null;
        try{
            tmp_128_v.set(0,new BasicInt(2));
        }catch(Exception E){
            RE = E.getMessage();
        }
        assertEquals("The value type is not BasicDecimal128!",RE);
    }

    @Test
    public void test_BasicDecimal128Vector_set_error_scale() throws Exception {
        BasicDecimal128Vector bd128v = new BasicDecimal128Vector(2,2);
        String a = bd128v.getElementClass().getName();
        assertEquals("com.xxdb.data.BasicDecimal128",a);
    }

    @Test
    public void test_BasicDecimal128Vector_getUnitLength() throws Exception {
        BasicDecimal128Vector bd128v = new BasicDecimal128Vector(2,2);
        int a = bd128v.getUnitLength();
        assertEquals(16,a);
    }

    @Test
    public void test_BasicDecimal128Vector_add_1() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.add(new BigDecimal("1.1122"));
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_128_v.getString());
    }
    @Test
    public void test_BasicDecimal128Vector_add_2() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.add("1.1122");
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_add_0() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.add(BigDecimal.valueOf(0.0));
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_zero_size_add() throws Exception {
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(0,4);
        tmp_128_v.add(BigDecimal.valueOf(12.42));
        assertEquals("[12.4200]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_addRange() throws Exception {
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(0,4);
        BigDecimal[] tmp = new BigDecimal[]{BigDecimal.valueOf(0),BigDecimal.valueOf(-123.00432),BigDecimal.valueOf(132.204234),BigDecimal.valueOf(100.0000)};
        tmp_128_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_addRange2() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        String[] tmp = new String[]{"0.0","-123.00432","132.204234","100.0"};
        tmp_128_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_addRange_null() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        String[] tmp = new String[]{};
        tmp_128_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_append_error() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 a = new BasicDecimal128("1.11223",2);
        tmp_128_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_append() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 a = new BasicDecimal128("1.11223",4);
        tmp_128_v.Append(a);
    }

    @Test
    public void test_BasicDecimal128Vector_append_vector_scale_notMatch() throws Exception {
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,3);
        tmp_128_v.Append(tmp_128_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_128_v.getString());
    }

    @Test
    public void test_BasicDecimal128Vector_append_vector() throws Exception {
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        tmp_128_v.Append(tmp_128_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }
    @Test
    public void test_BasicDecimal128Vector_append_null() throws Exception {
        BasicDecimal128 basicDecimal128 = new BasicDecimal128(String.valueOf(0),0);
        basicDecimal128.setNull();
        BasicDecimal128Vector basicDecimal128Vector = new BasicDecimal128Vector(0,0);
        basicDecimal128Vector.Append(basicDecimal128);
        System.out.println(((Scalar)(basicDecimal128Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal128Vector.get(0))).isNull());
    }
    @Test
    public void test_BasicDecimal128Vector_getScale() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        int a = tmp_128_v2.getScale();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal128Vector_getdataArray() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        Entity.DATA_CATEGORY a = tmp_128_v2.getDataCategory();
        assertEquals("DENARY",a.toString());
    }

    @Test
    public void test_BasicDecimal128Vector_getDataType() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        Entity.DATA_TYPE a = tmp_128_v2.getDataType();
        assertEquals("DT_DECIMAL128",a.toString());
    }

    @Test
    public void test_BasicDecimal128Vector_rows() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        int a = tmp_128_v2.rows();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal128Vector_getExtraParamForType() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(tmp_string_v,4);
        int a = tmp_128_v2.getExtraParamForType();
        assertEquals(4,a);
    }

    @Test
    public void testBasicDecimal128_run_bigvector() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("bigarray(DECIMAL128(2)[], 0, 3000000).append!(take([[92233720368547758, NULL, 100000000000000, NULL, -92233720368547758, -100000000000000], [], [00i], [92233720368547758]], 30000)) * 10");
        assertEquals("[[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],[],[922337203685477580.00],[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],[],[922337203685477580.00],[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],...]",re1.getString());
    }
    @Test
    public void testBasicDecimal128_run_vector() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([1.232,-12.43,123.53],6)");
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
    }

    @Test
    public void testBasicDecimal128_run_vector_has_NULL() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([1.232,-12.43,NULL],6)");
        assertEquals("[1.232000,-12.430000,]",re1.getString());
    }

    @Test
    public void testBasicDecimal128_run_vector_all_NULL() throws IOException {
        BasicDecimal128Vector re1 =(BasicDecimal128Vector) conn.run("decimal128([int(),NULL,NULL],6)");
        assertEquals("[,,]",re1.getString());
    }
    @Test
    public void testBasicDecimal128_run_bigvector1() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("bigarray(DECIMAL128(2)[], 0, 3000000).append!(take([[92233720368547758, NULL, 100000000000000, NULL, -92233720368547758, -100000000000000], [], [00i], [92233720368547758]], 30000)) * 10");
        System.out.println(re1.getString());
        assertEquals("[[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],[],[922337203685477580.00],[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],[],[922337203685477580.00],[922337203685477580.00,,1000000000000000.00,,-922337203685477580.00,-1000000000000000.00],[],...]",re1.getString());
    }
    @Test
    public void testBasicDecimal128_run_bigvector2() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("bigarray(DECIMAL128(36)[], 0, 3000000).append!([take(1..9, 3000000), []])*10");
        System.out.println(re1.getVectorValue(1).getString(0));
        assertEquals("",re1.getVectorValue(1).getString(0));
        System.out.println(re1.getVectorValue(0).getString(1));
        assertEquals("20.000000000000000000000000000000000000",re1.getVectorValue(0).getString(1));
    }
    @Test
    public void testBasicDecimal128_run_arrayVector_add() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("arr = array(DECIMAL128(2)[], 0, 10).append!([[92233720368547758, NULL, 100000000000000, NULL, -92233720368547758, -100000000000000], [], [00i], [92233720368547758]]);arr1=add(arr, 1);arr1;");
        assertEquals("[[92233720368547759.00,,100000000000001.00,,-92233720368547757.00,-99999999999999.00],[],[],[92233720368547759.00]]",re1.getString());
    }
    @Test
    public void testBasicDecimal128Vector_toJsonString() throws IOException {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector re1 = new BasicDecimal128Vector(tmp_string_v,4);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL128\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal128\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":4,\"string\":\"[0.0000,-123.0043,132.2042,100.0000]\",\"table\":false,\"unitLength\":16,\"unscaledValues\":[0,-1230043,1322042,1000000],\"vector\":true}", re);
    }
    @Test
    public void testBasicDecimal128Vector_toJsonString_null() throws IOException {
        BasicDecimal128Vector re1 = new BasicDecimal128Vector(2,2);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL128\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal128\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":2,\"string\":\"[0.00,0.00]\",\"table\":false,\"unitLength\":16,\"unscaledValues\":[0,0],\"vector\":true}", re);
    }
//    @Test
//    public void testBasicDecimal128Vector_getValue() throws IOException {
//        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
//        BasicDecimal128Vector re1 = new BasicDecimal128Vector(tmp_string_v,4);
//        BigInteger[] re = re1.getValues();
//        System.out.println(Arrays.toString(re));
//        assertEquals("[0, -1230043, 1322042, 1000000]",Arrays.toString(re));
//    }
    @Test
    public void test_BasicDecimal128Vector_deserialize() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128 tmp_32 = new BasicDecimal128("3.032",4);
        tmp_128_v.set(0,tmp_32);
        assertEquals("[3.0320,-123.0043,132.2042,100.0000]",tmp_128_v.getString());
    }
    @Test
    public void test_BasicDecimal128Vector_asof() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        Scalar sc = new BasicDecimal128("1",2);
        assertEquals(0,tmp_128_v.asof(sc));
    }
    @Test
    public void test_BasicDecimal128Vector_combine() throws Exception {
        String[] tmp_string_v = {"0.0","-123.004128","1128.204234","100.0"};
        BasicDecimal128Vector tmp_128_v = new BasicDecimal128Vector(tmp_string_v,4);
        BasicDecimal128Vector tmp_128_v1 = new BasicDecimal128Vector(0,4);
        BasicDecimal128Vector tmp_128_v2 = new BasicDecimal128Vector(0,5);
        BigInteger[] tmp_string_v1 = {new BigInteger("17014118346046923173168730371588410")};
        BasicDecimal128Vector tmp_128_v3 = new BasicDecimal128Vector(tmp_string_v1,4);

        assertEquals("[0.0000,-123.0041,1128.2042,100.0000,0.0000,-123.0041,1128.2042,100.0000]",tmp_128_v.combine(tmp_128_v).getString());
        assertEquals("[0.0000,-123.0041,1128.2042,100.0000]",tmp_128_v.combine(tmp_128_v1).getString());
        assertEquals("[0.0000,-123.0041,1128.2042,100.0000]",tmp_128_v1.combine(tmp_128_v).getString());
        assertEquals("[0.0000,-123.0041,1128.2042,100.0000,1701411834604692317316873037158.8410]",tmp_128_v.combine(tmp_128_v3).getString());
        tmp_128_v.setNull(0);
        assertEquals("[,-123.0041,1128.2042,100.0000,,-123.0041,1128.2042,100.0000]",tmp_128_v.combine(tmp_128_v).getString());
        String re = null;
        try{
            tmp_128_v.combine(tmp_128_v2);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("The scale of the vector to be combine does not match the scale of the current vector.",re);
    }
}
