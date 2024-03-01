package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BasicDecimal64Test {

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
    public void test_BasicDecimal64_value_int(){
        BasicDecimal64 Decimal64 = new BasicDecimal64((long)1232,4);
        assertEquals("1232.0000",Decimal64.getString());
    }

    @Test
    public void test_BasicDecimal64_value_double(){
        BasicDecimal64 Decimal64 = new BasicDecimal64(121.23, 6);
        assertEquals("121.230000",Decimal64.getString());
    }
    @Test
    public void test_BasicDecimal64_value_string_1(){
        String re = null;
        try{
            BasicDecimal64 Decimal64 = new BasicDecimal64("",4);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("value is empty!",re);
    }
    @Test
    public void test_BasicDecimal64_value_string_2(){
        BasicDecimal64 Decimal64 = new BasicDecimal64("0",4);
        assertEquals("0.0000",Decimal64.getString());

        BasicDecimal64 re1 = new BasicDecimal64("-0.5900",0);
        assertEquals("-1",re1.getString());

        BasicDecimal64 re2 = new BasicDecimal64("0.6900",0);
        assertEquals("1",re2.getString());

        BasicDecimal64 re3 = new BasicDecimal64("0.4900",0);
        assertEquals("0",re3.getString());

        BasicDecimal64 re4 = new BasicDecimal64("-0.4949",2);
        assertEquals("-0.49",re4.getString());

        BasicDecimal64 re5 = new BasicDecimal64("-0.4950",2);
        assertEquals("-0.50",re5.getString());

        BasicDecimal64 re6 = new BasicDecimal64("-0.4960",2);
        assertEquals("-0.50",re6.getString());
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_negative(){
        BasicDecimal64 Decimal64 = new BasicDecimal64("999999999999999999",0);
        assertEquals("999999999999999999",Decimal64.getString());
        BasicDecimal64 Decimal64_1 = new BasicDecimal64("-999999999999999999",0);
        assertEquals("-999999999999999999",Decimal64_1.getString());
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_0(){
        String ex = null;
        try{
            BasicDecimal64 Decimal64 = new BasicDecimal64("1232",-1);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,18].",ex);
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_4(){
        BasicDecimal64 Decimal64 = new BasicDecimal64("0.000001",4);
        assertEquals("0.0000",Decimal64.getString());
        BasicDecimal64 Decimal64_1 = new BasicDecimal64("-0.000001",4);
        assertEquals("0.0000",Decimal64_1.getString());
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_18(){
        BasicDecimal64 Decimal64 = new BasicDecimal64("0.000000000000000001",18);
        assertEquals("0.000000000000000001",Decimal64.getString());
        BasicDecimal64 Decimal64_1 = new BasicDecimal64("-0.000000000000000001",18);
        assertEquals("-0.000000000000000001",Decimal64_1.getString());
        BasicDecimal64 Decimal64_2 = new BasicDecimal64("0.999999999999999999",18);
        System.out.println(Decimal64_2.getString());
        assertEquals("0.999999999999999999",Decimal64_2.getString());
        BasicDecimal64 Decimal64_3 = new BasicDecimal64("-0.999999999999999999",18);
        assertEquals("-0.999999999999999999",Decimal64_3.getString());
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_18_overflow(){
        String re = null;
        try{
            BasicDecimal64 Decimal64_2 = new BasicDecimal64("9.9999999999999999999",18);
        }
        catch(Exception e){
            re = e.getMessage();
        }
        assertEquals(re,"Decimal math overflow: 9.9999999999999999999");

        String re1 = null;
        try{
            BasicDecimal64 Decimal64_3 = new BasicDecimal64("-9.9999999999999999999",18);
        }
        catch(Exception e){
            re1 = e.getMessage();
        }
        assertEquals("Decimal math overflow: -9.9999999999999999999",re1);
    }
    @Test
    public void test_BasicDecimal64_value_string_scale_19(){
        String ex = null;
        try{
            BasicDecimal64 Decimal64 = new BasicDecimal64("1232",19);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 19 is out of bounds, it must be in [0,18].",ex);
    }

    @Test
    public void test_BasicDecimal64_scale_0() throws Exception {
        BasicDecimal64 Decimal64 = new BasicDecimal64(123.2,0);
        assertEquals("123",Decimal64.getString());
        assertEquals((long)123,Decimal64.getNumber());
    }

    @Test
    public void test_BasicDecimal64_scale_18() throws Exception {
        BasicDecimal64 Decimal64 = new BasicDecimal64(1.2312,18);
        assertEquals("1.231200000000000000",Decimal64.getString());
        assertEquals(1.2312,Decimal64.getNumber());
    }

    @Test
    public void test_BasicDecimal64_scale_19() throws Exception {
        BasicDecimal64 Decimal64 = new BasicDecimal64(1.2312,19);
       // assertEquals("1.2312000000000000000",Decimal64.getString());
        assertNotEquals(1.2312,Decimal64.getNumber());
    }

    @Test
    public void test_BasicDecimal64_getString1() {
        BasicDecimal64 Decimal64_a = new BasicDecimal64((long)103, 6);
        assertEquals("103.000000", Decimal64_a.getString());
    }
    @Test
    public void test_BasicDecimal64_getString2() {
        BasicDecimal64 Decimal64_b = new BasicDecimal64((long)-103, 6);
        assertEquals("-103.000000", Decimal64_b.getString());
    }
    @Test
    public void test_BasicDecimal64_getString3() {
        BasicDecimal64 Decimal64_c = new BasicDecimal64((double) 0, 6);
        assertEquals("0.000000", Decimal64_c.getString());
    }
    @Test
    public void test_BasicDecimal64_getString4() {
        BasicDecimal64 Decimal64 = new BasicDecimal64( 1.23, 6);
        assertEquals("1.230000", Decimal64.getString());
    }
    @Test
    public void test_BasicDecimal64_getString5() {
        BasicDecimal64 Decimal64_2 = new BasicDecimal64( 0.003, 6);
        assertEquals("0.003000", Decimal64_2.getString());
    }
    @Test
    public void test_BasicDecimal64_getString6() {
        BasicDecimal64 Decimal64_3 = new BasicDecimal64(-1.23, 6);
        assertEquals("-1.230000", Decimal64_3.getString());
    }
    @Test
    public void test_BasicDecimal64_getString7(){
        BasicDecimal64 Decimal64_4 = new BasicDecimal64(-0.003, 6);
        assertEquals("-0.003000",Decimal64_4.getString());
    }
    @Test
    public void test_BasicDecimal64_getString8() {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23231, 2);
        assertEquals("1.23", Decimal64_5.getString());
    }
    @Test
    public void test_BasicDecimal64_getString9(){
        BasicDecimal64 Decimal64_6 = new BasicDecimal64(-1.23231, 2);
        assertEquals("-1.23",Decimal64_6.getString());
    }
    @Test
    public void test_BasicDecimal64_getString10(){
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( -1.23231, 0);
        assertEquals("-1",Decimal64_6.getString());
    }
    @Test
    public void test_BasicDecimal64_getString11(){
        BasicDecimal64 Decimal64_6 = new BasicDecimal64(1.23231, 0);
        assertEquals("1",Decimal64_6.getString());
    }
    @Test
    public void test_BasicDecimal64_getString12(){
        BasicDecimal64 Decimal64_6 = new BasicDecimal64("1.23231", 0);
        assertEquals("1",Decimal64_6.getString());
    }
    @Test
    public void test_BasicDecimal64_getNumber1() throws Exception {
        BasicDecimal64 Decimal64_a = new BasicDecimal64((long)103, 6);
        assertEquals((long)103, Decimal64_a.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber2() throws Exception {
        BasicDecimal64 Decimal64_b = new BasicDecimal64((long)-103, 6);
        assertEquals((long)-103, Decimal64_b.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber3() throws Exception {
        BasicDecimal64 Decimal64_c = new BasicDecimal64((double) 0, 6);
        assertEquals((long)0, Decimal64_c.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber4() throws Exception {
        BasicDecimal64 Decimal64 = new BasicDecimal64( 1.23, 6);
        assertEquals(1.23, Decimal64.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber5() throws Exception {
        BasicDecimal64 Decimal64_2 = new BasicDecimal64( 0.003, 6);
        assertEquals(0.003, Decimal64_2.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber6() throws Exception {
        BasicDecimal64 Decimal64_3 = new BasicDecimal64(-1.23, 6);
        assertEquals(-1.23, Decimal64_3.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber7() throws Exception {
        BasicDecimal64 Decimal64_4 = new BasicDecimal64(-0.003, 6);
        assertEquals(-0.003,Decimal64_4.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber8() throws Exception {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64(1.23231, 2);
        assertEquals(1.23, Decimal64_5.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber9() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( -1.23231, 2);
        assertEquals(-1.23,Decimal64_6.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber10() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( -1.23231, 0);
        assertEquals((long)-1,Decimal64_6.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber11() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( 1.23231, 0);
        assertEquals((long)1,Decimal64_6.getNumber());
    }
    @Test
    public void test_BasicDecimal64_getNumber12() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( -1.23231, 18);
        assertEquals(-1.23231, Decimal64_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal64_getNumber13() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( 1.23231, 18);
        assertEquals(1.23231, Decimal64_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal64_getNumber14() throws Exception {
        BasicDecimal64 Decimal64_6 = new BasicDecimal64( "1.23231", 18);
        assertEquals(1.23231, Decimal64_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal64_run() throws Exception {
        BasicDecimal64 re1 =(BasicDecimal64) conn.run("decimal64('1.003',4)");
        assertEquals("1.0030",re1.getString());
        assertEquals(1.003,re1.getNumber());
    }

    @Test
    public void test_BasicDecimal64_run2() throws Exception {
        BasicDecimal64 re1 =(BasicDecimal64) conn.run("decimal64(-12.332,6)");
        assertEquals("-12.332000",re1.getString());
        assertEquals(-12.332,re1.getNumber());
    }

    @Test
    public void test_BasicDecimal64_run3() throws Exception {
        BasicDecimal64 re1 =(BasicDecimal64) conn.run("decimal64(0,6)");
        assertEquals("0.000000",re1.getString());
        assertEquals((long)0,re1.getNumber());
    }

    @Test
    public void test_BasicDecimal64_run_NULL() throws Exception {
        BasicDecimal64 re1 =(BasicDecimal64) conn.run("decimal64(NULL,6)");
        assertEquals("",re1.getString());
        assertEquals(true,re1.isNull());
        assertEquals(-9223372036854775808L,re1.getNumber());
    }

    @Test
    public void test_BasicDecimal64_setNULL() throws IOException {
        BasicDecimal64 re1 =(BasicDecimal64) conn.run("decimal64(1,6)");
        re1.setNull();
        assertEquals(true,re1.isNull());
    }

    @Test
    public void test_BasicDecimal64_getScale() throws Exception {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23231, 2);
        assertEquals(2, Decimal64_5.getScale());
    }

    @Test
    public void test_BasicDecimal64_getScale_0() throws Exception {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64(1.23231, 0);
        assertEquals(0, Decimal64_5.getScale());
    }

    @Test
    public void test_BasicDecimal64_getScale_18() throws Exception {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23231, 18);
        assertEquals(18, Decimal64_5.getScale());
    }

    @Test(expected = java.lang.Exception.class)
    public void test_BasicDecimal64_getTemporal() throws Exception {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23231, 18);
        Decimal64_5.getTemporal();
    }

    @Test
    public void test_BasicDecimal64_hashBucket()  {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23231, 9);
        assertEquals(0, Decimal64_5.hashBucket(1));
    }

    @Test
    public void test_BasicDecimal64_getJsonString()  {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( 1.23, 4);
        assertEquals("1.2300", Decimal64_5.getJsonString());
    }
    @Test
    public void test_BasicDecimal64_getJsonString_1()  {
        BasicDecimal64 Decimal64_5 = new BasicDecimal64( "1.23", 4);
        assertEquals("1.2300", Decimal64_5.getJsonString());
    }
    @Test
    public void test_BasicDecimal64_compareTo_1()  {
        BasicDecimal64 re1 = new BasicDecimal64( 122.23, 4);
        BasicDecimal64 re2 = new BasicDecimal64( 121.23, 6);
        assertEquals(1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal64_compareTo_0()  {
        BasicDecimal64 re1 = new BasicDecimal64(1.23, 4);
        BasicDecimal64 re2 = new BasicDecimal64( 1.23, 6);
        assertEquals(0, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal64_compareTo_minus()  {
        BasicDecimal64 re1 = new BasicDecimal64( 1.23, 4);
        BasicDecimal64 re2 = new BasicDecimal64( 2.23, 6);
        assertEquals(-1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal64_compareTo_upload() throws IOException {
        BasicDecimal64 re1 = new BasicDecimal64( 1.23, 4);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal64(1.23,4)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal64_compareTo_upload2() throws IOException {
        BasicDecimal64 re1 = new BasicDecimal64( 12341.23, 3);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal64(12341.23,3)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal64_compareTo_upload_NULL() throws IOException {
        BasicDecimal64 re1 = new BasicDecimal64( 12341.23, 3);
        re1.setNull();
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal64(NULL,3)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal64_run_vector() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([1.232,-12.43,123.53],6)");
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
        assertEquals("[1.232000,-12.430000,123.530000]",re1.getString());
    }

    @Test
    public void test_BasicDecimal64_run_vector_has_NULL() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([1.232,-12.43,NULL],6)");
        assertEquals("[1.232000,-12.430000,]",re1.getString());
    }

    @Test
    public void test_BasicDecimal64_run_vector_all_NULL() throws IOException {
        BasicDecimal64Vector re1 =(BasicDecimal64Vector) conn.run("decimal64([int(),NULL,NULL],6)");
        assertEquals("[,,]",re1.getString());
    }
    @Test
    public void test_BasicDecimal64_toJsonString() throws IOException {
        BasicDecimal64 re1 = new BasicDecimal64("12341.23", 3);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL64\",\"dictionary\":false,\"jsonString\":\"12341.230\",\"long\":12341230,\"matrix\":false,\"null\":false,\"number\":12341.23,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"12341.230\",\"table\":false,\"vector\":false}",re);
    }
    @Test
    public void test_BasicDecimal64_toJsonString_null() throws IOException {
        BasicDecimal64 re1 = new BasicDecimal64("12341.23", 3);
        re1.setNull();
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL64\",\"dictionary\":false,\"jsonString\":\"null\",\"long\":-9223372036854775808,\"matrix\":false,\"null\":true,\"number\":-9223372036854775808,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"\",\"table\":false,\"vector\":false}",re);
    }
}
