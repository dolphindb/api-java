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
import java.math.BigDecimal;
import static org.junit.Assert.*;

public class BasicDecimal128Test {

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
    public void test_BasicDecimal128_value_string(){
        BasicDecimal128 Decimal128 = new BasicDecimal128("1232",4);
        assertEquals("1232.0000",Decimal128.getString());

        BasicDecimal128 re1 = new BasicDecimal128("-0.5900",0);
        assertEquals("-1",re1.getString());

        BasicDecimal128 re2 = new BasicDecimal128("0.6900",0);
        assertEquals("1",re2.getString());

        BasicDecimal128 re3 = new BasicDecimal128("0.4900",0);
        assertEquals("0",re3.getString());

        BasicDecimal128 re4 = new BasicDecimal128("-0.4949",2);
        assertEquals("-0.49",re4.getString());

        BasicDecimal128 re5 = new BasicDecimal128("-0.4950",2);
        assertEquals("-0.50",re5.getString());

        BasicDecimal128 re6 = new BasicDecimal128("-0.4960",2);
        assertEquals("-0.50",re6.getString());
    }

    @Test
    public void test_BasicDecimal128_value_string_1(){
        BasicDecimal128 Decimal128 = new BasicDecimal128("121.23", 6);
        assertEquals("121.230000",Decimal128.getString());
    }

    @Test
    public void test_BasicDecimal128_scale_0() throws Exception {
        BasicDecimal128 Decimal128 = new BasicDecimal128("123.2",0);
        assertEquals("123",Decimal128.getString());
        assertEquals(new BigDecimal("123"),Decimal128.getNumber());
        BasicDecimal128 Decimal1281 = new BasicDecimal128("-123.2",0);
        assertEquals("-123",Decimal1281.getString());
        assertEquals(new BigDecimal("-123"),Decimal1281.getNumber());
        BasicDecimal128 Decimal1282 = new BasicDecimal128("0",0);
        assertEquals("0",Decimal1282.getString());
        assertEquals(new BigDecimal("0"),Decimal1282.getNumber());
    }

    @Test
    public void test_BasicDecimal128_scale_18() throws Exception {

        BasicDecimal128 Decimal128 = new BasicDecimal128("1.2312",18);
        assertEquals("1.231200000000000000",Decimal128.getString());
        assertEquals(new BigDecimal("1.2312"),Decimal128.getNumber());
        BasicDecimal128 Decimal1281 = new BasicDecimal128("-1.2312",18);
        assertEquals("-1.231200000000000000",Decimal1281.getString());
        assertEquals(new BigDecimal("-1.2312"),Decimal1281.getNumber());
    }

    @Test
    public void test_BasicDecimal128_scale_19() throws Exception {
        BasicDecimal128 Decimal128 = new BasicDecimal128("1.2312",19);
        assertEquals("1.2312000000000000000",Decimal128.getString());
        assertEquals(new BigDecimal("1.2312"),Decimal128.getNumber());
        BasicDecimal128 Decimal1281 = new BasicDecimal128("-1.2312",19);
        assertEquals("-1.2312000000000000000",Decimal1281.getString());
        assertEquals(new BigDecimal("-1.2312"),Decimal1281.getNumber());
        BasicDecimal128 Decimal1282 = new BasicDecimal128("1.2312000000000000001123456",19);
        assertEquals("1.2312000000000000001",Decimal1282.getString());
        assertEquals(new BigDecimal("1.2312000000000000001"),Decimal1282.getNumber());
    }
    @Test
    public void test_BasicDecimal128_scale_30() throws Exception {
        BasicDecimal128 Decimal128 = new BasicDecimal128("99999999.999999999999999999999999999999",30);
        assertEquals("99999999.999999999999999999999999999999",Decimal128.getString());
        BasicDecimal128 Decimal1281 = new BasicDecimal128("99999999.000000000000000000000000000009",30);
        assertEquals("99999999.000000000000000000000000000009",Decimal1281.getString());
        BasicDecimal128 Decimal1282 = new BasicDecimal128("-99999999.999999999999999999999999999999",30);
        assertEquals("-99999999.999999999999999999999999999999",Decimal1282.getString());
        BasicDecimal128 Decimal1283 = new BasicDecimal128("-99999999.000000000000000000000000000009",30);
        assertEquals("-99999999.000000000000000000000000000009",Decimal1283.getString());
    }
    @Test
    public void test_BasicDecimal128_scale_37() throws Exception {
        BasicDecimal128 Decimal128 = new BasicDecimal128("9.9999999999999999999999999999999999999",37);
        assertEquals("9.9999999999999999999999999999999999999",Decimal128.getString());
        BasicDecimal128 Decimal1281 = new BasicDecimal128("9.9999999000000000000000000000000000009",37);
        assertEquals("9.9999999000000000000000000000000000009",Decimal1281.getString());
        BasicDecimal128 Decimal1282 = new BasicDecimal128("-9.9999999999999999999999999999999999999",37);
        assertEquals("-9.9999999999999999999999999999999999999",Decimal1282.getString());
        BasicDecimal128 Decimal1283 = new BasicDecimal128("-9.9999999000000000000000000000000000009",37);
        assertEquals("-9.9999999000000000000000000000000000009",Decimal1283.getString());
        BasicDecimal128 Decimal1284 = new BasicDecimal128("0",37);
        assertEquals("0.0000000000000000000000000000000000000",Decimal1284.getString());
    }
    @Test
    public void test_BasicDecimal128_scale_not_true() throws Exception {
        String ex = null;
        try{
            BasicDecimal128 Decimal128 = new BasicDecimal128("9.9999999999999999999999999999999999999",39);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale 39 is out of bounds, it must be in [0,38].",ex);
        try{
            BasicDecimal128 Decimal128 = new BasicDecimal128("9.999",-1);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Scale -1 is out of bounds, it must be in [0,38].",ex);
    }
    @Test
    public void test_BasicDecimal128_dataValue_not_true() throws Exception {
        String ex = null;
        try{
            BasicDecimal128 tmp_128 = new BasicDecimal128("-170141183460469231731687303715884105729",5);
        }catch(Exception E){
            ex=E.getMessage();
        }
        assertEquals("Decimal128 overflow -170141183460469231731687303715884105729.00000",ex);
        String ex1 = null;
        try{
            BasicDecimal128 tmp_128 = new BasicDecimal128("170141183460469231731687303715884105729",5);
        }catch(Exception E){
            ex1 = E.getMessage();
        }
        assertEquals("Decimal128 overflow 170141183460469231731687303715884105729.00000",ex1);
    }
//    @Test
//    public void test_BasicDecimal128_dataValue_not_true_1() throws Exception {
//        BasicDecimal128 tmp_1281 = new BasicDecimal128(new BigInteger("-170141183460469231731687303715884105728"),5);
//
//        String ex = null;
//        try{
//            BasicDecimal128 tmp_128 = new BasicDecimal128(new BigInteger("-170141183460469231731687303715884105728"),5);
//        }catch(Exception E){
//            ex=E.getMessage();
//        }
//        assertEquals("Decimal128 -170141183460469231731687303715884105729 cannot be less than -170141183460469231731687303715884105728",ex);
//        String ex1 = null;
//
//        try{
//            BasicDecimal128 tmp_128 = new BasicDecimal128(new BigInteger("170141183460469231731687303715884105728"),5);
//        }catch(Exception E){
//            ex1 = E.getMessage();
//        }
//        assertEquals("Decimal128 170141183460469231731687303715884105729 cannot exceed 170141183460469231731687303715884105728",ex1);
//    }

    @Test
    public void test_BasicDecimal128_getString1() {
        BasicDecimal128 Decimal128_a = new BasicDecimal128("103", 6);
        assertEquals("103.000000", Decimal128_a.getString());
    }
    @Test
    public void test_BasicDecimal128_getString2() {
        BasicDecimal128 Decimal128_b = new BasicDecimal128("-103", 6);
        assertEquals("-103.000000", Decimal128_b.getString());
    }
    @Test
    public void test_BasicDecimal128_getString3() {
        BasicDecimal128 Decimal128_c = new BasicDecimal128("0", 6);
        assertEquals("0.000000", Decimal128_c.getString());
    }
    @Test
    public void test_BasicDecimal128_getString4() {
        BasicDecimal128 Decimal128 = new BasicDecimal128( "1.23", 6);
        assertEquals("1.230000", Decimal128.getString());
    }
    @Test
    public void test_BasicDecimal128_getString5() {
        BasicDecimal128 Decimal128_2 = new BasicDecimal128( "0.003", 6);
        assertEquals("0.003000", Decimal128_2.getString());
    }
    @Test
    public void test_BasicDecimal128_getString6() {
        BasicDecimal128 Decimal128_3 = new BasicDecimal128("-1.23", 6);
        assertEquals("-1.230000", Decimal128_3.getString());
    }
    @Test
    public void test_BasicDecimal128_getString7(){
        BasicDecimal128 Decimal128_4 = new BasicDecimal128("-0.003", 6);
        assertEquals("-0.003000",Decimal128_4.getString());
    }
    @Test
    public void test_BasicDecimal128_getString8() {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23231", 2);
        assertEquals("1.23", Decimal128_5.getString());
    }
    @Test
    public void test_BasicDecimal128_getString9(){
        BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 2);
        assertEquals("-1.23",Decimal128_6.getString());
    }
    @Test
    public void test_BasicDecimal128_getString10(){
        BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 0);
        assertEquals("-1",Decimal128_6.getString());
    }
    @Test
    public void test_BasicDecimal128_getString11(){
        BasicDecimal128 Decimal128_6 = new BasicDecimal128("1.23231", 0);
        assertEquals("1",Decimal128_6.getString());
    }
    @Test
    public void test_BasicDecimal128_getString12(){
        BasicDecimal128 Decimal128_6 = new BasicDecimal128("1.23231", 37);
        assertEquals("1.2323100000000000000000000000000000000",Decimal128_6.getString());
    }
    @Test
    public void test_BasicDecimal128_getString13(){
        BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 37);
        assertEquals("-1.2323100000000000000000000000000000000",Decimal128_6.getString());
    }
    @Test
    public void test_BasicDecimal128_getNumber1() throws Exception {
        BasicDecimal128 Decimal128_a = new BasicDecimal128("103", 6);
        assertEquals(new BigDecimal(103.000000), Decimal128_a.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber2() throws Exception {
        BasicDecimal128 Decimal128_b = new BasicDecimal128("-103", 6);
        assertEquals(new BigDecimal(-103), Decimal128_b.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber3() throws Exception {
        BasicDecimal128 Decimal128_c = new BasicDecimal128("0", 6);
        assertEquals(new BigDecimal(0), Decimal128_c.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber4() throws Exception {
        BasicDecimal128 Decimal128 = new BasicDecimal128( "1.23", 6);
        assertEquals(new BigDecimal("1.23"), Decimal128.getNumber());
        assertEquals("1.230000", Decimal128.getString());

    }
    @Test
    public void test_BasicDecimal128_getNumber5() throws Exception {
        BasicDecimal128 Decimal128_2 = new BasicDecimal128( "0.003", 6);
        assertEquals(new BigDecimal("0.003"), Decimal128_2.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber6() throws Exception {
        BasicDecimal128 Decimal128_3 = new BasicDecimal128("-1.23", 6);
        assertEquals(new BigDecimal("-1.23"), Decimal128_3.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber7() throws Exception {
        BasicDecimal128 Decimal128_4 = new BasicDecimal128("-0.003", 6);
        assertEquals(new BigDecimal("-0.003"),Decimal128_4.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber8() throws Exception {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 2);
        assertEquals(new BigDecimal("1.23"), Decimal128_5.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber9() throws Exception {
        BasicDecimal128 Decimal128_6 = new BasicDecimal128( "-1.23231", 2);
        assertEquals(new BigDecimal("-1.23"),Decimal128_6.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber10() throws Exception {
        BasicDecimal128 Decimal128_6 = new BasicDecimal128( "-1.23231", 0);
        assertEquals(new BigDecimal("-1"),Decimal128_6.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber11() throws Exception {
        BasicDecimal128 Decimal128_6 = new BasicDecimal128( "1.23231", 0);
        assertEquals(new BigDecimal("1"),Decimal128_6.getNumber());
    }
    @Test
    public void test_BasicDecimal128_getNumber12() throws Exception {
        BasicDecimal128 Decimal128_6 = new BasicDecimal128( "-1.23231", 18);
        assertEquals(-1.23231, Decimal128_6.getNumber().doubleValue(),5);
    }
    @Test
    public void test_BasicDecimal128_getNumber13() throws Exception {
        BasicDecimal128 Decimal128_6 = new BasicDecimal128( "1.23231", 18);
        assertEquals(1.23231, Decimal128_6.getNumber().doubleValue(),5);
    }

    @Test
    public void test_BasicDecimal128_run() throws Exception {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("decimal128('1.003',4)");
        assertEquals("1.0030",re1.getString());
        assertEquals(new BigDecimal("1.003"),re1.getNumber());
    }

    @Test
    public void test_BasicDecimal128_run2() throws Exception {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("decimal128(-12.332,6)");
        assertEquals("-12.332000",re1.getString());
        assertEquals(new BigDecimal("-12.332"),re1.getNumber());
    }

    @Test
    public void test_BasicDecimal128_run3() throws Exception {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("decimal128(0,6)");
        assertEquals("0.000000",re1.getString());
        assertEquals(new BigDecimal("0"),re1.getNumber());
    }

    @Test
    public void test_BasicDecimal128_run_NULL() throws Exception {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("decimal128(NULL,6)");
        assertEquals("",re1.getString());
        assertEquals(true,re1.isNull());
        assertEquals("-170141183460469231731687303715884105728",re1.getNumber().toString());
    }
    @Test
    public void test_BasicDecimal128_run_NULL_1() throws Exception {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("decimal64(NULL,18) * decimal64(NULL,1)");
        assertEquals("",re1.getString());
        assertEquals(true,re1.isNull());
        assertEquals("-170141183460469231731687303715884105728",re1.getNumber().toString());
    }

    @Test
    public void test_BasicDecimal128_setNULL() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128( "0.003", 6);
        re1.setNull();
        assertEquals(true,re1.isNull());
        BasicDecimal128 re2 = (BasicDecimal128)conn.run("decimal128(NULL,3)");
        assertEquals(true,re2.isNull());
    }
    @Test
    public void test_BasicDecimal128_getScale() throws Exception {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23231", 2);
        assertEquals(2, Decimal128_5.getScale());
    }

    @Test
    public void test_BasicDecimal128_getScale_0() throws Exception {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 0);
        assertEquals(0, Decimal128_5.getScale());
    }

    @Test
    public void test_BasicDecimal128_getScale_18() throws Exception {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23231", 18);
        assertEquals(18, Decimal128_5.getScale());
    }

    @Test(expected = Exception.class)
    public void test_BasicDecimal128_getTemporal() throws Exception {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23231", 18);
        Decimal128_5.getTemporal();
    }

    @Test
    public void test_BasicDecimal128_hashBucket()  {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23231", 9);
        String re=null;
        assertEquals(0, Decimal128_5.hashBucket(1));
//        try{
//            Decimal128_5.hashBucket(1);
//        }catch(Exception e){
//            re=e.getMessage();
//        }
//        assertEquals("BasicDecimal128 not support hashBucket yet!", re);
    }

    @Test
    public void test_BasicDecimal128_getJsonString()  {
        BasicDecimal128 Decimal128_5 = new BasicDecimal128( "1.23", 4);
        assertEquals("1.2300", Decimal128_5.getJsonString());
    }

    @Test
    public void test_BasicDecimal128_compareTo_1()  {
        BasicDecimal128 re1 = new BasicDecimal128( "122.23", 4);
        BasicDecimal128 re2 = new BasicDecimal128( "121.23", 6);
        assertEquals(1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal128_compareTo_0()  {
        BasicDecimal128 re1 = new BasicDecimal128("1.23", 4);
        BasicDecimal128 re2 = new BasicDecimal128( "1.23", 6);
        assertEquals(0, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal128_compareTo_minus()  {
        BasicDecimal128 re1 = new BasicDecimal128( "1.23", 4);
        BasicDecimal128 re2 = new BasicDecimal128( "2.23", 6);
        assertEquals(-1, re1.compareTo(re2));
    }

    @Test
    public void test_BasicDecimal128_compareTo_upload() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128( "1.23", 4);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal128(1.23,4)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal128_compareTo_upload2() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128( "12341.23", 3);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        System.out.println(re1);
//        BasicDecimal128 res11 = (BasicDecimal128) conn.run("a");
//        System.out.println(res11.getString());
        BasicString res1 = (BasicString) conn.run("typestr(a)");
        assertEquals("DECIMAL128", res1.getString());
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal128(12341.23,3)");
        assertEquals(true, res.getBoolean());
    }
    @Test
    public void test_BasicDecimal128_compareTo_upload3() throws IOException {
        BasicDecimal128 b=new BasicDecimal128("-1441050.00",0);
        System.out.println(b.getString());
        BasicDecimal128 c=new BasicDecimal128("-1441050.00",2);
        System.out.println(c.getString());
        BasicDecimal128 re1 = new BasicDecimal128( "-12341.23", 3);
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal128(-12341.23,3)");
        assertEquals(true, res.getBoolean());
    }

    @Test
    public void test_BasicDecimal128_compareTo_upload_NULL() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128( "12341.23", 3);
        re1.setNull();
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("a",re1);
        conn.upload(vars);
        BasicBoolean res = (BasicBoolean) conn.run("a == decimal128(NULL,3)");
        assertEquals(true, res.getBoolean());
    }
    @Test
    public void test_BasicDecimal128_compareTo(){
        BasicDecimal128 bd1 = new BasicDecimal128("15",2);
        BasicDecimal128 bd2 = new BasicDecimal128("15",2);
        assertEquals(0,bd1.compareTo(bd2));
        assertEquals(0,bd2.compareTo(bd1));
        BasicDecimal128 bd3 = new BasicDecimal128("15",3);
        assertEquals(0,bd1.compareTo(bd3));
        BasicDecimal128 bd4 = new BasicDecimal128("17",3);
        assertEquals(-1,bd1.compareTo(bd4));
        assertEquals(1,bd4.compareTo(bd1));
    }

    @Test
    public void test_BasicDecimal128_run_mul() throws IOException {
        BasicDecimal128 re1 =(BasicDecimal128) conn.run("m = decimal64(9223372036854775807, 0);t=m*m*NULL; t");
        assertEquals("",re1.getString());
    }
    @Test
    public void test_BasicDecimal128_toJsonString() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128("12341.23", 3);
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL128\",\"dictionary\":false,\"jsonString\":\"12341.230\",\"matrix\":false,\"null\":false,\"number\":12341.23,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"12341.230\",\"table\":false,\"vector\":false}", re);

    }
    @Test
    public void test_BasicDecimal128_toJsonString_null() throws IOException {
        BasicDecimal128 re1 = new BasicDecimal128("12341.23", 3);
        re1.setNull();
        String re = JSONObject.toJSONString(re1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"DENARY\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_DECIMAL128\",\"dictionary\":false,\"jsonString\":\"null\",\"matrix\":false,\"null\":true,\"number\":-170141183460469231731687303715884105728,\"pair\":false,\"scalar\":true,\"scale\":3,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }
}
