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
import static com.xxdb.data.Entity.DATA_TYPE.DT_INSTRUMENT;
import static org.junit.Assert.assertEquals;

public class BasicInstrumentTest {
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
    public void test_BasicInstrument_basic() throws IOException {
        String script ="fixedRateBond = {\n" +
                "    \"productType\": \"Cash\",\n" +
                "    \"assetType\": \"Bond\",\n" +
                "    \"bondType\": \"FixedRate\",\n" +
                "    \"version\": 0, \n" +
                "    \"instrumentId\": \"240025.IB\",\n" +
                "    \"start\": 2024.12.25,\n" +
                "    \"maturity\": 2031.12.25,\n" +
                "    \"issuePrice\": 100.0,\n" +
                "    \"coupon\": 0.0149,\n" +
                "    \"frequency\": \"Annual\",\n" +
                "    \"dayCountConvention\": \"ActualActual\"\n" +
                "}\n" +
                "ins = parseInstrument(fixedRateBond)\n" +
                "ins;";
        BasicInstrument Instrument = (BasicInstrument)conn.run(script);
        System.out.println(Instrument.getString());
        assertEquals("Instrument<detail invisible>",Instrument.getString());
        assertEquals(DT_INSTRUMENT, Instrument.getDataType());
        assertEquals(SYSTEM, Instrument.getDataCategory());
        String re = null;
        try{
            Instrument.compareTo(Instrument);
        }catch(Exception ex){
            re =ex.getMessage();
        }
        assertEquals("BasicInstrument.compareTo not supported.",re);
    }

    @Test
    public void test_BasicInstrument_upload() throws IOException {
        String script ="fixedRateBond = {\n" +
                "    \"productType\": \"Cash\",\n" +
                "    \"assetType\": \"Bond\",\n" +
                "    \"bondType\": \"FixedRate\",\n" +
                "    \"version\": 0, \n" +
                "    \"instrumentId\": \"240025.IB\",\n" +
                "    \"start\": 2024.12.25,\n" +
                "    \"maturity\": 2031.12.25,\n" +
                "    \"issuePrice\": 100.0,\n" +
                "    \"coupon\": 0.0149,\n" +
                "    \"frequency\": \"Annual\",\n" +
                "    \"dayCountConvention\": \"ActualActual\"\n" +
                "}\n" +
                "ins = parseInstrument(fixedRateBond)\n" +
                "ins;";
        BasicInstrument Instrument = (BasicInstrument)conn.run(script);
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("Instrument", (Entity) Instrument);
        String re = null;
        try{
            conn.upload(upObj);
        }catch(Exception ex){
            re =ex.getMessage();
        }
        assertEquals("Not support yet",re);
    }
    @Test
    public void test_BasicInstrument_download_table() throws IOException {
        String script ="fixedRateBond = {\n" +
                "    \"productType\": \"Cash\",\n" +
                "    \"assetType\": \"Bond\",\n" +
                "    \"bondType\": \"FixedRate\",\n" +
                "    \"version\": 0, \n" +
                "    \"instrumentId\": \"240025.IB\",\n" +
                "    \"start\": 2024.12.25,\n" +
                "    \"maturity\": 2031.12.25,\n" +
                "    \"issuePrice\": 100.0,\n" +
                "    \"coupon\": 0.0149,\n" +
                "    \"frequency\": \"Annual\",\n" +
                "    \"dayCountConvention\": \"ActualActual\"\n" +
                "}\n" +
                "ins = parseInstrument(fixedRateBond)\n" +
                "ins;";
        BasicInstrument Instrument = (BasicInstrument)conn.run(script);
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("Instrument", (Entity) Instrument);
        String re = null;
        try{
            conn.upload(upObj);
        }catch(Exception ex){
            re =ex.getMessage();
        }
        assertEquals("Not support yet",re);
    }
}
