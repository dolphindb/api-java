package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import static com.xxdb.data.Entity.DATA_CATEGORY.SYSTEM;
import static com.xxdb.data.Entity.DATA_TYPE.DT_MKTDATA;
import static org.junit.Assert.assertEquals;

public class BasicMktDataTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(HOST,PORT,"admin","123456")){
                throw new IOException("Failed to connect to 2xdb server");
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
    @Test
    public void test_BasicMktData_basic() throws IOException {
        String script = "aod = 2025.07.01\n" +
                "discountCurve = {\n" +
                "    \"mktDataType\": \"Curve\",\n" +
                "    \"curveType\": \"IrYieldCurve\",\n" +
                "    \"version\": 0, \n" +
                "    \"referenceDate\": aod,\n" +
                "    \"currency\": \"CNY\",\n" +
                "    \"dayCountConvention\": \"Actual365\",\n" +
                "    \"compounding\": \"Continuous\",\n" +
                "    \"interpMethod\": \"Linear\",\n" +
                "    \"extrapMethod\": \"Flat\",\n" +
                "    \"dates\":[2025.07.07,2025.07.10,2025.07.17,2025.07.24,2025.08.04,2025.09.03,2025.10.09,2026.01.05,\n" +
                "        2026.04.03,2026.07.03,2027.01.04,2027.07.05,2028.07.03],\n" +
                "    \"values\":[0.015785,0.015931,0.016183,0.016381,0.016493,0.016503,0.016478,0.016234,0.016321,\n" +
                "        0.016378,0.015508,0.015185,0.014901],\n" +
                "    \"settlement\": aod+2\n" +
                "}\n" +
                "mktdata = parseMktData(discountCurve);\n" +
                "mktdata;";
        BasicMktData MktData = (BasicMktData)conn.run(script);
        System.out.println(MktData.getString());
        assertEquals("MktData<detail invisible>",MktData.getString());
        assertEquals(DT_MKTDATA, MktData.getDataType());
        assertEquals(SYSTEM,MktData.getDataCategory());
        String re = null;
        try{
            MktData.compareTo(MktData);
        }catch(Exception ex){
            re =ex.getMessage();
        }
        assertEquals("BasicMktData.compareTo not supported.",re);
    }

    @Test
    public void test_BasicMktData_upload() throws IOException {
        String script = "aod = 2025.07.01\n" +
                "discountCurve = {\n" +
                "    \"mktDataType\": \"Curve\",\n" +
                "    \"curveType\": \"IrYieldCurve\",\n" +
                "    \"version\": 0, \n" +
                "    \"referenceDate\": aod,\n" +
                "    \"currency\": \"CNY\",\n" +
                "    \"dayCountConvention\": \"Actual365\",\n" +
                "    \"compounding\": \"Continuous\",\n" +
                "    \"interpMethod\": \"Linear\",\n" +
                "    \"extrapMethod\": \"Flat\",\n" +
                "    \"dates\":[2025.07.07,2025.07.10,2025.07.17,2025.07.24,2025.08.04,2025.09.03,2025.10.09,2026.01.05,\n" +
                "        2026.04.03,2026.07.03,2027.01.04,2027.07.05,2028.07.03],\n" +
                "    \"values\":[0.015785,0.015931,0.016183,0.016381,0.016493,0.016503,0.016478,0.016234,0.016321,\n" +
                "        0.016378,0.015508,0.015185,0.014901],\n" +
                "    \"settlement\": aod+2\n" +
                "}\n" +
                "mktdata = parseMktData(discountCurve);\n" +
                "mktdata;";
        BasicMktData MktData = (BasicMktData)conn.run(script);
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("MktData", (Entity) MktData);
        String re = null;
        try{
            conn.upload(upObj);
        }catch(Exception ex){
            re =ex.getMessage();
        }
        assertEquals("Not support yet",re);
    }
}
