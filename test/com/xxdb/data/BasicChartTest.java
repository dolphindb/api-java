package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.io.Double2;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.LittleEndianDataInputStream;
import com.xxdb.io.Long2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicChartTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_BasicChart_chartType() throws Exception {
        BasicChart bc = new BasicChart();
        String re = null;
        try{
            bc.getChartType();
        }catch(Exception ex){
            re = ex.getMessage();
        }
        assertEquals("Invalid chart object. Chart type is not defined.",re);
    }
    @Test(expected = RuntimeException.class)
    public void test_BasicChart_chartType_1() throws Exception {
        BasicChart bc = new BasicChart(1);
        bc.getChartType();
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicChart_getdata()  {
        BasicChart bc = new BasicChart(5);
        bc.getData();
    }
    @Test
    public void test_BasicChart_get_NULL() throws Exception {
        BasicChart bc = new BasicChart();
        assertNotEquals(Chart.CHART_TYPE.CT_BAR,Chart.CHART_TYPE.CT_AREA);
        assertEquals("",bc.getTitle());
        assertEquals("",bc.getXAxisName());
        assertEquals("",bc.getYAxisName());
        assertNull(bc.getExtraParameters());
        assertNull(bc.getExtraParameter(CHART_PARAMETER_TYPE.multiYAxes));
        assertEquals(Entity.DATA_FORM.DF_CHART,bc.getDataForm());
    }
    @Test
    public void test_BasicChart_toJSONString() throws Exception {
        BasicChart bc = new BasicChart(1);
        String re = JSONObject.toJSONString(bc);
        System.out.println(re);
        assertEquals("{\"XAxisName\":\"\",\"YAxisName\":\"\",\"chart\":true,\"chunk\":false,\"dataCategory\":\"MIXED\",\"dataForm\":\"DF_CHART\",\"dataType\":\"DT_ANY\",\"dictionary\":false,\"keyDataType\":\"DT_STRING\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"\",\"table\":false,\"title\":\"\",\"vector\":false}", re);
    }

}
