package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicDecimal64VectorTest {
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
    public void test_BasicDecimal64Vector_scale_not_true() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        String ex = null;
        try{
            BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,19);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 19 is out of bounds, it must be in [0,18].",ex);
        String ex1 = null;
        try{
            BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,18].",ex1);
    }
    @Test
    public void test_BasicDecimal64Vector_scale_not_true_1() throws Exception {
        double[] tmp_double_v = {1.1};
        String ex = null;
        try{
            BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,19);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 19 is out of bounds, it must be in [0,18].",ex);
        String ex1 = null;
        try{
            BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,-1);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,18].",ex1);
    }
    @Test
    public void test_BasicDecimal64Vector_run_vector() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([1.232,-12.43,123.53],6)");
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_run_vector_has_NULL() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([1.232,-12.43,NULL],6)");
        assertEquals("[1.232000,-12.430000,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_run_vector_all_NULL() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([int(),NULL,NULL],6)");
        assertEquals("[,,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_basicFunction() throws Exception {
        BasicDecimal64Vector bd32v = new BasicDecimal64Vector(2,2);
        bd32v.set(0,new BasicDecimal64(11.0,2));
        bd32v.set(1,new BasicDecimal64(17.0,2));
        assertFalse(bd32v.isNull(1));
        assertEquals(BasicDecimal64.class,bd32v.getElementClass());
        bd32v.setNull(0);
        assertTrue(bd32v.isNull(0));
    }

    @Test
    public void test_BasicDecimal64Vector_create_Decimal64Vector() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_create_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_create_string_scale_0() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,0);
        assertEquals("[0,-123,132,100]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_create_string_scale_17() throws Exception {
        String[] tmp_string_v = {"0.0","-1.00000000000000001","1.00000000000000001","9.99999999999999999","-9.99999999999999999"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,17);
        assertEquals("[0.00000000000000000,-1.00000000000000001,1.00000000000000001,9.99999999999999999,-9.99999999999999999]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_create_Decimal64Vector_null() throws Exception {
        double[] tmp_double_v = {};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        assertEquals("[]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_create_string_null() throws Exception {
        String[] tmp_string_v = {};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        assertEquals("[]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_isNull() throws Exception {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([1.232,-12.43,NULL],6)");
        assertEquals(true,re1.isNull(2));
        assertEquals(false,re1.isNull(0));
    }

    @Test
    public void test_BasicDecimal64Vector_setNUll() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_64_v.setNull(2);
        assertEquals("[0.0000,-123.0043,,100.0000]",tmp_64_v.getString());
        assertEquals(true,tmp_64_v.isNull(2));
    }

    @Test
    public void test_BasicDecimal64Vector_get() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 ex = new BasicDecimal64(-123.00432,4);
        assertEquals(ex.getString(),tmp_64_v.get(1).getString());
    }

    @Test
    public void test_BasicDecimal64Vector_set() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 tmp_32 = new BasicDecimal64(3.032,4);
        tmp_64_v.set(0,tmp_32);
        assertEquals("[3.0320,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_set_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 tmp_32 = (BasicDecimal64) conn.run("decimal64(NULL,4)");
        tmp_64_v.set(0,tmp_32);
        assertEquals("[,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_set_int() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        String RE = null;
        try{
            tmp_64_v.set(0,new BasicInt(2));
        }catch(Exception E){
            RE = E.getMessage();
        }
        assertEquals("value type is not BasicDecimal64!",RE);
    }
    @Test
    public void test_BasicDecimal64Vector_set_string() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_64_v.set(0,new BasicDecimal64("2",2));
        assertEquals("[2.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
        String ex = null;
        try{
            tmp_64_v.set(0,new BasicString("2"));
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("value type is not BasicDecimal64!",ex);
    }
    @Test
    public void test_BasicDecimal64Vector_set_error_scale() throws Exception {
        BasicDecimal64Vector bd32v = new BasicDecimal64Vector(2);
        String a = bd32v.getElementClass().getName();
        assertEquals("com.xxdb.data.BasicDecimal64",a);
    }

    @Test
    public void test_BasicDecimal64Vector_getUnitLength() throws Exception {
        BasicDecimal64Vector bd32v = new BasicDecimal64Vector(2);
        int a = bd32v.getUnitLength();
        assertEquals(8,a);
    }


    @Test
    public void test_BasicDecimal64Vector_add() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_64_v.add(1.11223);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_add_0() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_64_v.add(0.0);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_add_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        tmp_64_v.add("1.11223");
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_64_v.getString());
        tmp_64_v.add("0.0");
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122,0.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_zero_size_add() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        tmp_64_v.add(12.42);
        assertEquals("[12.4200]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_64_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange2() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_64_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        double[] tmp = new double[]{};
        tmp_64_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_addRange_string_1() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        tmp_64_v.addRange(tmp_string_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_addRange_string_2() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        tmp_64_v.addRange(tmp_string_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_addRange_string_null() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        String[] tmp_string1_v = {};
        tmp_64_v.addRange(tmp_string1_v);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_addRange_string_decimal_min_or_max_value() throws Exception {
        String[] tmp_string_v = {"-9223372036854775808","9223372036854775807"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,0);
        String[] tmp_string1_v = {};
        tmp_64_v.addRange(tmp_string1_v);
        assertEquals("[0,0]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_append_error() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 a = new BasicDecimal64(1.11223,2);
        tmp_64_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_append() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 a = new BasicDecimal64(1.11223,4);
        tmp_64_v.Append(a);
    }
    @Test
    public void test_BasicDecimal64Vector_append_null() throws Exception {
        BasicDecimal64 basicDecimal64 = new BasicDecimal64(0L,0);
        basicDecimal64.setNull();
        BasicDecimal64Vector basicDecimal64Vector = new BasicDecimal64Vector(0,0);
        basicDecimal64Vector.Append(basicDecimal64);
        System.out.println(((Scalar)(basicDecimal64Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal64Vector.get(0))).isNull());
    }
    @Test
    public void test_BasicDecimal64Vector_append_vector_scale_notMatch() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,3);
        tmp_64_v.Append(tmp_64_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_64_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_append_vector() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_64_v.Append(tmp_64_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_append_string_scale_notMatch() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        BasicDecimal64 a = new BasicDecimal64("1.11223",2);
        tmp_64_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_append_string() throws Exception {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(tmp_string_v,4);
        BasicDecimal64 a = new BasicDecimal64("1.11223",4);
        tmp_64_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_Append_string_vector_scale_notMatch() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_string_v,3);
        tmp_64_v.Append(tmp_64_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_Append_string_vector() throws Exception {
        BasicDecimal64Vector tmp_64_v = new BasicDecimal64Vector(0,4);
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_string_v,4);
        tmp_64_v.Append(tmp_64_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_64_v.getString());
    }
    @Test
    public void test_BasicDecimal64Vector_getScale() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_64_v2.getScale();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal64Vector_getDataCategory() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        Entity.DATA_CATEGORY a = tmp_64_v2.getDataCategory();
        assertEquals("DENARY",a.toString());
    }
    @Test
    public void test_BasicDecimal64Vector_getdataArray() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal32Vector tmp_32_v2 = new BasicDecimal32Vector(tmp_double_v,4);
        int[] a = tmp_32_v2.getdataArray();
        assertEquals("[0, -1230043, 1322042, 1000000]", Arrays.toString(a));
    }

    @Test
    public void test_BasicDecimal64Vector_getDataType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        Entity.DATA_TYPE a = tmp_64_v2.getDataType();
        assertEquals("DT_DECIMAL64",a.toString());
    }

    @Test
    public void test_BasicDecimal64Vector_rows() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_64_v2.rows();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal64Vector_getExtraParamForType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_64_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_64_v2.getExtraParamForType();
        assertEquals(4,a);
    }
    @Test
    public void testBasicDecimal64Vector_run_arrayVector_add() throws IOException {
        BasicArrayVector re1 =(BasicArrayVector) conn.run("arr = array(DECIMAL64(2)[], 0, 10).append!([[92233720368547758, NULL, 100000000000000, NULL, -92233720368547758, -100000000000000], [], [00i], [92233720368547758]]);arr1=add(arr, 1);arr1;");
        assertEquals("[[92233720368547759.00,,100000000000001.00,,-92233720368547757.00,-99999999999999.00],[],[],[92233720368547759.00]]",re1.getString());
    }
    @Test
    public void testBasicDecimal64Vector_toJsonString() throws IOException {
        String[] tmp_string_v = {"0.0","-123.00432","132.204234","100.0"};
        BasicDecimal64Vector re1 = new BasicDecimal64Vector(tmp_string_v,4);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL64\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal64\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":4,\"string\":\"[0.0000,-123.0043,132.2042,100.0000]\",\"table\":false,\"unitLength\":8,\"vector\":true}", re);
    }
    @Test
    public void testBasicDecimal64Vector_toJsonString_null() throws IOException {
        BasicDecimal64Vector re1 = new BasicDecimal64Vector(2,2);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_DECIMAL64\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicDecimal64\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"scale\":2,\"string\":\"[0.00,0.00]\",\"table\":false,\"unitLength\":8,\"vector\":true}", re);
    }
}
