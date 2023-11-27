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
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test
    public void test_countMonth(){
        //base month 0000.01
        assertEquals(24226, Utils.countMonths(YearMonth.of(2018,11)));
        assertEquals(24226, Utils.countMonths(2018,11));
        assertEquals(0, Utils.countMonths(0,1));
        assertEquals(0, Utils.countMonths(YearMonth.of(0,1)));
        assertEquals(23652,Utils.countMonths(365));
        assertEquals(23654,Utils.countMonths(424));
        assertEquals(24059,Utils.countMonths(12781));
        assertEquals(24060,Utils.countMonths(12813));
    }
    @Test
    public void test_parseMonth(){
        //base month 0000.01
        assertEquals(2018, Utils.parseMonth(24226).getYear());
        assertEquals(11, Utils.parseMonth(24226).getMonthValue());
        assertEquals(0, Utils.parseMonth(0).getYear());
        assertEquals(1, Utils.parseMonth(0).getMonthValue());
    }

    @Test
    public void test_countDays(){
        LocalDate start = LocalDate.of(1970,1,1);
        LocalDate now = LocalDate.now();
        long day = ChronoUnit.DAYS.between(start,now);
        assertEquals(day,Utils.countDays(new GregorianCalendar()));
        assertEquals(Integer.MIN_VALUE,Utils.countDays(2022,13,0));
        System.out.println(Utils.countDays(2012,3,30));
        assertEquals(day,Utils.countDays(now));
    }

    @Test
    public void test_countSeconds(){
        Calendar calendar = Calendar.getInstance();
        // 把时分秒毫秒都置为 0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取 calendar 的毫秒数
        long today00 = calendar.getTimeInMillis();
        // 今天的已经度过的分钟数
        long todaySeconds = (new Date().getTime() - today00) / 1000;
        assertEquals(todaySeconds,Utils.countSeconds(new GregorianCalendar()));
        assertEquals(todaySeconds,Utils.countSeconds(LocalTime.now()));
    }

    @Test
    public void test_divide(){
        assertEquals(2,Utils.divide(-6,-3));
        assertEquals(1L,Utils.divide(-7L,-3L));
        assertEquals(2L,Utils.divide(-6L,-3L));
    }

    @Test
    public void test_countMilliseconds() {
        Calendar calendar = Calendar.getInstance();
        // 把时分秒毫秒都置为 0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取 calendar 的毫秒数
        long today00 = calendar.getTimeInMillis();
        // 今天的已经度过的分钟数
        long todayMilliSeconds = (new Date().getTime() - today00);
        assertEquals(todayMilliSeconds, Utils.countMilliseconds(new GregorianCalendar()));
        calendar.set(1970, 1, 1, 1, 1, 1);
        assertEquals(2682061000L,Utils.countDateMilliseconds(calendar));
    }
    @Test
    public void test_countMinutes(){
        Calendar calendar = Calendar.getInstance();
        // 把时分秒毫秒都置为 0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // 获取 calendar 的毫秒数
        long today00 = calendar.getTimeInMillis();
        // 今天的已经度过的分钟数
        long todayMinute = (new Date().getTime() - today00)/1000/60;
        assertEquals(todayMinute,Utils.countMinutes(new GregorianCalendar()));
    }

    @Test
    public void test_murmur32(){
        byte[] data = new byte[]{'D','O','L','P','H','I','N','D','B'};
        System.out.println(Utils.murmur32(data,9,1));
        byte[] data2 = new byte[]{'h','a','n','g','z','h','o','u'};
        System.out.println(Utils.murmur32(data2,8,0));
        System.out.println(Utils.murmur32(new byte[]{'T','e','s','t','i','n','g'},7,3));
        System.out.println(Utils.murmur32(new byte[]{'Y','o','N','g','L','i'},6,2));
    }

    @Test
    public void test_parseTimeStamp(){

        assertEquals("1969-12-31T23:59:59",Utils.parseNanoTimestamp(-1000000000L).toString());
        assertEquals("1969-12-31T23:59:59",Utils.parseTimestamp(-1000L).toString());
        assertEquals("1972-03-01",Utils.parseDate(790).toString());
    }

    @Test
    public void test_getCategory(){
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,Utils.getCategory(Entity.DATA_TYPE.DT_BOOL));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Utils.getCategory(Entity.DATA_TYPE.DT_DOUBLE));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Utils.getCategory(Entity.DATA_TYPE.DT_FLOAT));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Utils.getCategory(Entity.DATA_TYPE.DT_INT128));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Utils.getCategory(Entity.DATA_TYPE.DT_UUID));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Utils.getCategory(Entity.DATA_TYPE.DT_IPADDR));
        assertEquals(Entity.DATA_CATEGORY.MIXED,Utils.getCategory(Entity.DATA_TYPE.DT_ANY));
        assertEquals(Entity.DATA_CATEGORY.NOTHING,Utils.getCategory(Entity.DATA_TYPE.DT_VOID));
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,Utils.getCategory(Entity.DATA_TYPE.DT_BLOB));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,Utils.getCategory(Entity.DATA_TYPE.DT_INT));
    }

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

    @Test
    public void test_reallocByteBuffer(){
        ByteBuffer src = ByteBuffer.allocate(9);
        src.put((byte) 'd');
        src.put((byte) 'o');
        src.put((byte) 'l');
        src.put((byte) 'p');
        src.put((byte) 'h');
        src.put((byte) 'i');
        src.put((byte) 'n');
        src.put((byte) 'd');
        src.put((byte) 'b');
        assertEquals(Arrays.toString(src.array()),Arrays.toString(Utils.reAllocByteBuffer(src,9).array()));
    }

    @Test
    public void test_isVariableCandidate(){
        String word = "0dolphindb";
        assertFalse(Utils.isVariableCandidate(word));
        String word2 = "Dolphindb(";
        assertFalse(Utils.isVariableCandidate(word2));
    }

}
