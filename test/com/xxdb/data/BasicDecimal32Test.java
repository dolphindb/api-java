package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.*;

public class BasicDecimal32Test {
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
    public void test_BasicDecimal32_value_int(){
        BasicDecimal32 Decimal32 = new BasicDecimal32(1232,4);
        assertEquals("1232.0000",Decimal32.getString());
    }

    @Test
    public void test_BasicDecimal32_value_double(){
        BasicDecimal32 Decimal32 = new BasicDecimal32((float) 1.23, 6);
        assertEquals("1.230000",Decimal32.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_1(){
        String re = null;
        try{
            BasicDecimal32 Decimal32 = new BasicDecimal32("",4);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("value is empty!",re);
    }
    @Test
    public void test_BasicDecimal32_value_string_2(){
        BasicDecimal32 Decimal32 = new BasicDecimal32("0",4);
        assertEquals("0.0000",Decimal32.getString());

        BasicDecimal32 re1 = new BasicDecimal32("-0.5900",0);
        assertEquals("-1",re1.getString());

        BasicDecimal32 re2 = new BasicDecimal32("0.6900",0);
        assertEquals("1",re2.getString());

        BasicDecimal32 re3 = new BasicDecimal32("0.4900",0);
        assertEquals("0",re3.getString());

        BasicDecimal32 re4 = new BasicDecimal32("-0.4949",2);
        assertEquals("-0.49",re4.getString());

        BasicDecimal32 re5 = new BasicDecimal32("-0.4950",2);
        assertEquals("-0.50",re5.getString());

        BasicDecimal32 re6 = new BasicDecimal32("-0.4960",2);
        assertEquals("-0.50",re6.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_negative(){
        String ex = null;
        try{
            BasicDecimal32 Decimal32 = new BasicDecimal32("1232",-1);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,9].",ex);
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_0(){
        BasicDecimal32 Decimal32 = new BasicDecimal32("999999999",0);
        assertEquals("999999999",Decimal32.getString());
        BasicDecimal32 Decimal32_1 = new BasicDecimal32("-999999999",0);
        assertEquals("-999999999",Decimal32_1.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_4(){
        BasicDecimal32 Decimal32 = new BasicDecimal32("0.000001",4);
        assertEquals("0.0000",Decimal32.getString());
        BasicDecimal32 Decimal32_1 = new BasicDecimal32("-0.000001",4);
        assertEquals("0.0000",Decimal32_1.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_8(){
        BasicDecimal32 Decimal32 = new BasicDecimal32("0.999999999",8);
        assertEquals("1.00000000",Decimal32.getString());
        BasicDecimal32 Decimal32_1 = new BasicDecimal32("-0.999999999",8);
        assertEquals("-1.00000000",Decimal32_1.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_9(){
        BasicDecimal32 Decimal32 = new BasicDecimal32("0.000000001",9);
        assertEquals("0.000000001",Decimal32.getString());
        BasicDecimal32 Decimal32_1 = new BasicDecimal32("-0.000000001",9);
        assertEquals("-0.000000001",Decimal32_1.getString());
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_9_overflow(){
        String re = null;
        try{
            BasicDecimal32 Decimal32_2 = new BasicDecimal32("9.99999999",9);
        }
        catch(Exception e){
            re = e.getMessage();
        }
        assertEquals(re,"Decimal math overflow!");

        String re1 = null;
        try{
            BasicDecimal32 Decimal32_3 = new BasicDecimal32("-9.99999999",9);
        }
        catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals(re1,"Decimal math overflow!");
    }
    @Test
    public void test_BasicDecimal32_value_string_scale_10(){
        String ex = null;
        try{
            BasicDecimal32 Decimal32 = new BasicDecimal32("1232",10);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 10 is out of bounds, it must be in [0,9].",ex);
    }

    @Test
    public void test_BasicDecimal32_scale_0() throws Exception {
        BasicDecimal32 Decimal32 = new BasicDecimal32(123.2423,0);
        assertEquals("123",Decimal32.getString());
        assertEquals(123,Decimal32.getNumber());
    }

    @Test
    public void test_BasicDecimal32_scale_9() throws Exception {
        BasicDecimal32 Decimal32 = new BasicDecimal32(1.2312,9);
        assertEquals("1.231200000",Decimal32.getString());
        assertEquals(1.2312,Decimal32.getNumber());
    }

    @Test
    public void test_BasicDecimal32_scale_10() throws Exception {
        String re = null;
        try{
            BasicDecimal32 decimal32 = new BasicDecimal32(2.11,10);
        }
        catch(Exception e){
            re = e.getMessage();
        }
        assertEquals(re,"Decimal math overflow!");

    }

    @Test
    public void test_BasicDecimal32_getString1() {
        BasicDecimal32 Decimal32_a = new BasicDecimal32(103, 6);
        assertEquals("103.000000", Decimal32_a.getString());
        assertEquals(Entity.DATA_CATEGORY.DENARY,Decimal32_a.getDataCategory());
    }
    @Test
    public void test_BasicDecimal32_getString2() {
        BasicDecimal32 Decimal32_b = new BasicDecimal32(-103, 6);
        assertEquals("-103.000000", Decimal32_b.getString());
    }
    @Test
    public void test_BasicDecimal32_getString3() {
        BasicDecimal32 Decimal32_c = new BasicDecimal32((float) 0, 6);
        assertEquals("0.000000", Decimal32_c.getString());
    }
    @Test
    public void test_BasicDecimal32_getString4() {
        BasicDecimal32 Decimal32 = new BasicDecimal32((float) 1.23, 6);
        assertEquals("1.230000", Decimal32.getString());
    }
    @Test
    public void test_BasicDecimal32_getString5() {
        BasicDecimal32 Decimal32_2 = new BasicDecimal32((float) 0.003, 6);
        assertEquals("0.003000", Decimal32_2.getString());
    }
    @Test
    public void test_BasicDecimal32_getString6() {
        BasicDecimal32 Decimal32_3 = new BasicDecimal32((float) -1.23, 6);
        assertEquals("-1.230000", Decimal32_3.getString());
    }
    @Test
    public void test_BasicDecimal32_getString7(){
        BasicDecimal32 Decimal32_4 = new BasicDecimal32((float) -0.003, 6);
        assertEquals("-0.003000",Decimal32_4.getString());
    }
    @Test
    public void test_BasicDecimal32_getString8() {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 2);
        assertEquals("1.23", Decimal32_5.getString());
    }
    @Test
    public void test_BasicDecimal32_getString9(){
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) -1.23231, 2);
        assertEquals("-1.23",Decimal32_6.getString());
    }
    @Test
    public void test_BasicDecimal32_getString10(){
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) -1.23231, 0);
        assertEquals("-1",Decimal32_6.getString());
    }
    @Test
    public void test_BasicDecimal32_getString11(){
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) 1.23231, 0);
        assertEquals("1",Decimal32_6.getString());
    }
    @Test
    public void test_BasicDecimal32_getString12(){
        BasicDecimal32 Decimal32_6 = new BasicDecimal32( "1.23231", 0);
        assertEquals("1",Decimal32_6.getString());
    }
    @Test
    public void test_BasicDecimal32_getNumber1() throws Exception {
        BasicDecimal32 Decimal32_a = new BasicDecimal32(103, 6);
        assertEquals(103, Decimal32_a.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber2() throws Exception {
        BasicDecimal32 Decimal32_b = new BasicDecimal32(-103, 6);
        assertEquals(-103, Decimal32_b.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber3() throws Exception {
        BasicDecimal32 Decimal32_c = new BasicDecimal32((float) 0, 6);
        assertEquals(0, Decimal32_c.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber4() throws Exception {
        BasicDecimal32 Decimal32 = new BasicDecimal32((float) 1.23, 6);
        assertEquals(1.23, Decimal32.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber5() throws Exception {
        BasicDecimal32 Decimal32_2 = new BasicDecimal32((float) 0.003, 6);
        assertEquals(0.003, Decimal32_2.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber6() throws Exception {
        BasicDecimal32 Decimal32_3 = new BasicDecimal32((float) -1.23, 6);
        assertEquals(-1.23, Decimal32_3.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber7() throws Exception {
        BasicDecimal32 Decimal32_4 = new BasicDecimal32((float) -0.003, 6);
        assertEquals(-0.003,Decimal32_4.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber8() throws Exception {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 2);
        assertEquals(1.23, Decimal32_5.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber9() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) -1.23231, 2);
        assertEquals(-1.23,Decimal32_6.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber10() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) -1.23231, 0);
        assertEquals(-1,Decimal32_6.getNumber());
    }
    @Test
    public void test_BasicDecimal32_getNumber11() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) 1.23231, 0);
        assertEquals(1,Decimal32_6.getNumber());
    }

    @Test
    public void test_BasicDecimal32_getNumber12() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) -1.23231, 9);
        assertEquals(-1.23231, Decimal32_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal32_getNumber13() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32((float) 1.23231, 9);
        assertEquals(1.23231, Decimal32_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal32_getNumber14() throws Exception {
        BasicDecimal32 Decimal32_6 = new BasicDecimal32("1.23231", 9);
        assertEquals(1.23231, Decimal32_6.getNumber().doubleValue(),5);
    }

    @Test
    public void test_BasicDecimal32_run() throws Exception {
        BasicDecimal32 re1 =(BasicDecimal32) conn.run("decimal32('1.003',4)");
        assertEquals("1.0030",re1.getString());
        assertEquals(1.003,re1.getNumber());
        assertEquals("DT_DECIMAL32",re1.getDataType().toString());
    }

    @Test
    public void test_BasicDecimal32_run2() throws Exception {
        BasicDecimal32 re1 =(BasicDecimal32) conn.run("decimal32(-12.332,6)");
        assertEquals("-12.332000",re1.getString());
        assertEquals(-12.332,re1.getNumber());
        assertEquals("DT_DECIMAL32",re1.getDataType().toString());
    }

    @Test
    public void test_BasicDecimal32_run3() throws Exception {
        BasicDecimal32 re1 =(BasicDecimal32) conn.run("decimal32(0,6)");
        assertEquals("0.000000",re1.getString());
        assertEquals(0,re1.getNumber());
        assertEquals("DT_DECIMAL32",re1.getDataType().toString());
    }

    @Test
    public void test_BasicDecimal32_run_NULL() throws Exception {
        BasicDecimal32 re1 =(BasicDecimal32) conn.run("decimal32(NULL,6)");
        assertEquals("",re1.getString());
        assertEquals(true,re1.isNull());
        assertEquals(-2147483648,re1.getNumber());
        assertEquals("DT_DECIMAL32",re1.getDataType().toString());
    }

    @Test
    public void test_BasicDecimal32_setNULL() throws IOException {
        BasicDecimal32 re1 =(BasicDecimal32) conn.run("decimal32(1,6)");
        re1.setNull();
        assertEquals(true,re1.isNull());
    }

    @Test
    public void test_BasicDecimal32_getScale() throws Exception {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 2);
        assertEquals(2, Decimal32_5.getScale());
    }

    @Test
    public void test_BasicDecimal32_getScale_0() throws Exception {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 0);
        assertEquals(0, Decimal32_5.getScale());
    }

    @Test
    public void test_BasicDecimal32_getScale_9() throws Exception {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 9);
        assertEquals(9, Decimal32_5.getScale());
    }

    @Test(expected = java.lang.Exception.class)
    public void test_BasicDecimal32_getTemporal() throws Exception {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 9);
        assertEquals(9, Decimal32_5.getTemporal());
    }

    @Test
    public void test_BasicDecimal32_hashBucket()  {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23231, 9);
        assertEquals(0, Decimal32_5.hashBucket(1));
    }

    @Test
    public void test_BasicDecimal32_getJsonString()  {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32((float) 1.23, 4);
        assertEquals("1.2300", Decimal32_5.getJsonString());
    }
    @Test
    public void test_BasicDecimal32_getJsonString_1()  {
        BasicDecimal32 Decimal32_5 = new BasicDecimal32("1.23", 4);
        assertEquals("1.2300", Decimal32_5.getJsonString());
    }
    @Test
    public void test_BasicDecimal32_compareTo_1()  {
        BasicDecimal32 re1 = new BasicDecimal32((float) 2.23, 4);
        BasicDecimal32 re2 = new BasicDecimal32((float) 1.23, 6);
        assertEquals(1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal32_compareTo_0()  {
        BasicDecimal32 re1 = new BasicDecimal32((float) 1.23, 4);
        BasicDecimal32 re2 = new BasicDecimal32((float) 1.23, 6);
        assertEquals(0, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal32_compareTo_minus()  {
        BasicDecimal32 re1 = new BasicDecimal32((float) 1.23, 4);
        BasicDecimal32 re2 = new BasicDecimal32((float) 2.23, 6);
        assertEquals(-1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal32_compareTo_upload() throws IOException {
        BasicDecimal32 re1 = new BasicDecimal32((float) 1.23, 4);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal32(1.23,4)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal32_compareTo_upload2() throws IOException {
        BasicDecimal32 re1 = new BasicDecimal32((float) 12341.23, 3);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal32(12341.23,3)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal32_compareTo_upload_NULL() throws IOException {
        BasicDecimal32 re1 = new BasicDecimal32((float) 12341.23, 3);
        re1.setNull();
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal32(NULL,3)");
        assertEquals(true, res.getBoolean());
    }
    @Test
    public void test_BasicDecimal32_toJsonString() throws IOException {
        BasicDecimal32 re1 = new BasicDecimal32("12341.23", 3);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL32\",\"dictionary\":false,\"int\":12341230,\"jsonString\":\"12341.230\",\"matrix\":false,\"null\":false,\"number\":12341.23,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"12341.230\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicDecimal32_toJsonString_null() throws IOException {
        BasicDecimal32 re1 = new BasicDecimal32("12341.23", 3);
        re1.setNull();
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL32\",\"dictionary\":false,\"int\":-2147483648,\"jsonString\":\"null\",\"matrix\":false,\"null\":true,\"number\":-2147483648,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }
}
