package com.xxdb.compatibility_testing.release130.data;
import com.xxdb.DBConnection;
import com.xxdb.data.Entity;
import com.xxdb.data.Utils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.xxdb.data.BasicArrayVectorTest.bundle;
import static org.junit.Assert.*;

public class UtilsTest {
    public static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/compatibility_testing/release130/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_toMonth() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        try{
            Utils.toMonth(conn.run("x=5"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.",re.getMessage());
        }
        try{
            Utils.toMonth(conn.run("x=3 6 1 5 9;x;"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.",re.getMessage());
        }
        assertEquals("2022.07M",Utils.toMonth(conn.run("timestamp(2022.07.29);")).toString());
        assertEquals("2022.07M",Utils.toMonth(conn.run("date(2022.07.29 11:36:47);")).toString());
        assertEquals("2022.07M",Utils.toMonth(conn.run("datetime(2022.07.29);")).toString());
        assertEquals("2022.07M",Utils.toMonth(conn.run("nanotimestamp(2022.07.29 12:03:09);")).toString());
        assertEquals("[2016.03M,2019.06M,2020.02M]",Utils.toMonth(conn.run("date(2016.03M 2019.06M 2020.02M)")).getString());
        assertEquals("[2016.03M,2019.06M,2020.02M]",Utils.toMonth(conn.run("datetime(2016.03.17 2019.06.07 2020.02.25)")).getString());
    }

    @Test
    public void test_toDate() throws IOException{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        try{
            Utils.toDate(conn.run("x=5"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.",re.getMessage());
        }
        try{
            Utils.toDate(conn.run("x=3 6 1 5 9;x;"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.",re.getMessage());
        }
        String format = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
        assertEquals(format,Utils.toDate(conn.run("datetime(now());")).toString());
        assertEquals(format,Utils.toDate(conn.run("nanotimestamp(now());")).toString());
        assertEquals(format,Utils.toDate(conn.run("timestamp(now());")).toString());
        assertEquals("[1970.01.01,1971.02.02,1973.03.03]",Utils.toDate(conn.run("nanotimestamp(1970.01.01 1971.02.02 1973.03.03)")).getString());
        assertEquals("[1970.01.01,1971.02.02,1973.03.03]",Utils.toDate(conn.run("timestamp(1970.01.01 1971.02.02 1973.03.03)")).getString());
    }

    @Test
    public void test_toDateHour() throws IOException{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        try{
            Utils.toDateHour(conn.run("x=5"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.",re.getMessage());
        }
        try{
            Utils.toDateHour(conn.run("x=3 6 1 5 9;x;"));
        }catch(RuntimeException re){
            assertEquals("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.",re.getMessage());
        }
        String format = new SimpleDateFormat("yyyy.MM.dd.HH").format(new Date());
        assertEquals(format,Utils.toDateHour(conn.run("datetime(now());")).toString().replace('T','.'));
        assertEquals(format,Utils.toDateHour(conn.run("nanotimestamp(now());")).toString().replace('T','.'));
        assertEquals(format,Utils.toDateHour(conn.run("timestamp(now());")).toString().replace('T','.'));
        assertEquals("[1970.01.01.01,1980.02.02.02,1990.03.03.03]",Utils.toDateHour(conn.run("datetime(1970.01.01T01:01:01 1980.02.02T02:02:02 1990.03.03T03:03:03)")).getString().replace('T','.'));
        assertEquals("[1970.01.01.01,1980.02.02.02,1990.03.03.03]",Utils.toDateHour(conn.run("timestamp(1970.01.01T01:01:01 1980.02.02T02:02:02 1990.03.03T03:03:03)")).getString().replace('T','.'));
        assertEquals("[1970.01.01.01,1980.02.02.02,1990.03.03.03]",Utils.toDateHour(conn.run("nanotimestamp(1970.01.01T01:01:01 1980.02.02T02:02:02 1990.03.03T03:03:03)")).getString().replace('T','.'));
    }

    @Test
    public void test_castDateTime() throws IOException{
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        try {
            Utils.castDateTime(conn.run("t0=table(1 2 3 as a, `x`y`z as b, 10.8 7.6 3.5 as c);t0;"), Entity.DATA_TYPE.DT_DATE);
        }catch(RuntimeException re){
            assertEquals("The source data must be a temporal scalar/vector.",re.getMessage());
        }
        try{
            Utils.castDateTime(conn.run("timestamp(2016.03.17 2019.06.08 2020.02.10)"),Entity.DATA_TYPE.DT_TIMESTAMP);
        }catch(RuntimeException re){
            assertEquals("The target date/time type supports MONTH/DATE only for time being.",re.getMessage());
        }
        String format = new SimpleDateFormat("yyyy.MM.dd.HH").format(new Date());
        assertEquals(format,Utils.castDateTime(conn.run("datetime(now())"),Entity.DATA_TYPE.DT_DATEHOUR).toString().replace('T','.'));
        assertTrue(Utils.isLittleEndian());
    }
}
