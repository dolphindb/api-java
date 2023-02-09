package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
    public void test_BasicDecimal64Vector_create_Decimal32Vector() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_create_Decimal32Vector_null() throws Exception {
        double[] tmp_double_v = {};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        assertEquals("[]",tmp_32_v.getString());
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
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_32_v.setNull(2);
        assertEquals("[0.0000,-123.0043,,100.0000]",tmp_32_v.getString());
        assertEquals(true,tmp_32_v.isNull(2));
    }

    @Test
    public void test_BasicDecimal64Vector_get() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 ex = new BasicDecimal64(-123.00432,4);
        assertEquals(ex.getString(),tmp_32_v.get(1).getString());
    }

    @Test
    public void test_BasicDecimal64Vector_set() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 tmp_32 = new BasicDecimal64(3.032,4);
        tmp_32_v.set(0,tmp_32);
        assertEquals("[3.0320,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_set_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 tmp_32 = (BasicDecimal64) conn.run("decimal64(NULL,4)");
        tmp_32_v.set(0,tmp_32);
        assertEquals("[,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicDecimal64Vector_set_int() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_32_v.set(0,new BasicInt(2));
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
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_32_v.add(1.11223);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1122]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_add_0() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_32_v.add(0.0);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_zero_size_add() throws Exception {
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(0,4);
        tmp_32_v.add(12.42);
        assertEquals("[12.4200]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange() throws Exception {
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(0,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange2() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        double[] tmp = new double[]{0.0,-123.00432,132.204234,100.0};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_addRange_null() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        double[] tmp = new double[]{};
        tmp_32_v.addRange(tmp);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_append_error() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 a = new BasicDecimal64(1.11223,2);
        tmp_32_v.Append(a);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000,1.1100]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_append() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(tmp_double_v,4);
        BasicDecimal64 a = new BasicDecimal64(1.11223,4);
        tmp_32_v.Append(a);
    }

    @Test
    public void test_BasicDecimal64Vector_append_vector_scale_notMatch() throws Exception {
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,3);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0040,132.2040,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_append_vector() throws Exception {
        BasicDecimal64Vector tmp_32_v = new BasicDecimal64Vector(0,4);
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        tmp_32_v.Append(tmp_32_v2);
        assertEquals("[0.0000,-123.0043,132.2042,100.0000]",tmp_32_v.getString());
    }

    @Test
    public void test_BasicDecimal64Vector_getScale() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_32_v2.getScale();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal64Vector_getdataArray() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        Entity.DATA_CATEGORY a = tmp_32_v2.getDataCategory();
        assertEquals("DENARY",a.toString());
    }

    @Test
    public void test_BasicDecimal64Vector_getDataType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        Entity.DATA_TYPE a = tmp_32_v2.getDataType();
        assertEquals("DT_DECIMAL64",a.toString());
    }

    @Test
    public void test_BasicDecimal64Vector_rows() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_32_v2.rows();
        assertEquals(4,a);
    }

    @Test
    public void test_BasicDecimal64Vector_getExtraParamForType() throws Exception {
        double[] tmp_double_v = {0.0,-123.00432,132.204234,100.0};
        BasicDecimal64Vector tmp_32_v2 = new BasicDecimal64Vector(tmp_double_v,4);
        int a = tmp_32_v2.getExtraParamForType();
        assertEquals(4,a);
    }


}
