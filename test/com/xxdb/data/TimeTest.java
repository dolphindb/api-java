package com.xxdb.data;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TimeTest{

    @Test
    public void testTime(){
        {
            LocalDateTime dt = LocalDateTime.of(2017,8,11,10,03,10,2030);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 2030;
            if (nanoTime != Utils.countNanoseconds(dt)) {
                throw new RuntimeException("expect" + nanoTime + ", got " + Utils.countNanoseconds(dt));
            }
        }

        {
            LocalTime t = LocalTime.of(10, 03, 10, 2030);
            long nanoTime = t.getHour() * Utils.NANOS_PER_HOUR + t.getMinute() * Utils.NANOS_PER_MINUTE + t.getSecond() * Utils.NANOS_PER_SECOND + 2030;
            if (nanoTime != Utils.countNanoseconds(t)) {
                throw new RuntimeException("expect" + nanoTime + ", got " + Utils.countNanoseconds(t));
            }
        }

        {
            LocalDateTime dt = LocalDateTime.of(2000,12,31,12,12,12, 8500);
            BasicNanoTimestamp t = new BasicNanoTimestamp(dt);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 8500;
            if (nanoTime != t.getLong()) {
                throw new RuntimeException("expect" + nanoTime + ", got " + t.getLong());
            }
        }

        {
            LocalDateTime dt = Utils.parseNanoTimestamp(5);
            if (dt.getYear() != 1999)
                throw new RuntimeException("expect 1999, got " + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 31)
                throw new RuntimeException("expect 31, got " + dt.getDayOfMonth());
            if (dt.getHour() != 0)
                throw new RuntimeException("expect 0, got " + dt.getHour());
            if (dt.getMinute() != 0)
                throw new RuntimeException("expect 0, got " + dt.getMinute());
            if (dt.getSecond() != 0)
                throw new RuntimeException("expect 0, got " + dt.getSecond());
            if (dt.getNano() != 5)
                throw new RuntimeException("expect 5, got " + dt.getNano());
        }

        {
            LocalDateTime dt = Utils.parseNanoTimestamp(-1);
            if (dt.getYear() != 1999)
                throw new RuntimeException("expect 1999, got " + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 30)
                throw new RuntimeException("expect 30, got " + dt.getDayOfMonth());
            if (dt.getHour() != 23)
                throw new RuntimeException("expect 23, got " + dt.getHour());
            if (dt.getMinute() != 59)
                throw new RuntimeException("expect 59, got " + dt.getMinute());
            if (dt.getSecond() != 59)
                throw new RuntimeException("expect 59, got " + dt.getSecond());
            if (dt.getNano() != Utils.NANOS_PER_SECOND - 1)
                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 1) + ", got " + dt.getNano());
        }

        {
            LocalDateTime dt = Utils.parseTimestamp(-1);
            if (dt.getYear() != 1999)
                throw new RuntimeException("expect 1999, got " + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 30)
                throw new RuntimeException("expect 30, got " + dt.getDayOfMonth());
            if (dt.getHour() != 23)
                throw new RuntimeException("expect 23, got " + dt.getHour());
            if (dt.getMinute() != 59)
                throw new RuntimeException("expect 59, got " + dt.getMinute());
            if (dt.getSecond() != 59)
                throw new RuntimeException("expect 59, got " + dt.getSecond());
            if (dt.getNano() != Utils.NANOS_PER_SECOND - 1000000)
                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 1000000) + ", got " + dt.getNano());
        }
        {
            LocalTime dt = Utils.parseNanoTime(Utils.NANOS_PER_DAY - 5);
            if (dt.getHour() != 23)
                throw new RuntimeException("expect 0, got " + dt.getHour());
            if (dt.getMinute() != 59)
                throw new RuntimeException("expect 0, got " + dt.getMinute());
            if (dt.getSecond() != 59)
                throw new RuntimeException("expect 0, got " + dt.getSecond());
            if (dt.getNano() != Utils.NANOS_PER_SECOND - 5)
                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 5) + ", got " + dt.getNano());
            System.out.println(dt);
        }
        {
            LocalTime dt = Utils.parseNanoTime(5);
            if (dt.getHour() != 0)
                throw new RuntimeException("expect 0, got " + dt.getHour());
            if (dt.getMinute() != 0)
                throw new RuntimeException("expect 0, got " + dt.getMinute());
            if (dt.getSecond() != 0)
                throw new RuntimeException("expect 0, got " + dt.getSecond());
            if (dt.getNano() != 5)
                throw new RuntimeException("expect 5, got " + dt.getNano());
        }

        {
            LocalDateTime dt = LocalDateTime.of(1999,12,30,23,59,59,999000000);
            long mills = Utils.countMilliseconds(dt);
            if (mills != -1)
                throw new RuntimeException("expect -1, got " + mills);
        }

        {
            LocalDateTime dt = LocalDateTime.of(1999,12,30,23,59,59,999999995);
            long nanos = Utils.countNanoseconds(dt);
            if (nanos != -5)
                throw new RuntimeException("expect -1, got " + nanos);
        }

        {
            LocalDateTime dt = LocalDateTime.of(1999,12,31,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills != 0)
                throw new RuntimeException("expect 0, got " + mills);
        }


        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long nanos = Utils.countNanoseconds(dt);
            if (nanos != -Utils.NANOS_PER_DAY * 5)
                throw new RuntimeException("expect " + -Utils.NANOS_PER_DAY * 5 + ", got " + nanos);
            LocalDateTime dt2 = Utils.parseNanoTimestamp(nanos);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }

        {
            LocalDateTime dt = LocalDateTime.of(2000,1,1,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills != Utils.MILLS_PER_DAY)
                throw new RuntimeException("expect " + Utils.MILLS_PER_DAY + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills != -Utils.MILLS_PER_DAY * 5)
                throw new RuntimeException("expect " + -Utils.MILLS_PER_DAY * 5 + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
    }
    @Test
    public void testMonthCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMonthVector v = new BasicMonthVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicMonthVector v1 = new BasicMonthVector(list1);
        BasicMonthVector v2 = (BasicMonthVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicMonthVector v3 = new BasicMonthVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testDateCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateVector v = new BasicDateVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateVector v1 = new BasicDateVector(list1);
        BasicDateVector v2 = (BasicDateVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateVector v3 = new BasicDateVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testDateHourCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateHourVector v = new BasicDateHourVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateHourVector v1 = new BasicDateHourVector(list1);
        BasicDateHourVector v2 = (BasicDateHourVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateHourVector v3 = new BasicDateHourVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testDateTimeCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicDateTimeVector v = new BasicDateTimeVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicDateTimeVector v1 = new BasicDateTimeVector(list1);
        BasicDateTimeVector v2 = (BasicDateTimeVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicDateTimeVector v3 = new BasicDateTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testMinuteCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicMinuteVector v = new BasicMinuteVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicMinuteVector v1 = new BasicMinuteVector(list1);
        BasicMinuteVector v2 = (BasicMinuteVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicMinuteVector v3 = new BasicMinuteVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testNanoTimeCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicNanoTimeVector v = new BasicNanoTimeVector(list);
        List<Long> list1 = Arrays.asList(1l,2l,3l);
        BasicNanoTimeVector v1 = new BasicNanoTimeVector(list1);
        BasicNanoTimeVector v2 = (BasicNanoTimeVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,1l,2l,3l);
        BasicNanoTimeVector v3 = new BasicNanoTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testNanoTimeStampCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicNanoTimestampVector v = new BasicNanoTimestampVector(list);
        List<Long> list1 = Arrays.asList(1l,2l,3l);
        BasicNanoTimestampVector v1 = new BasicNanoTimestampVector(list1);
        BasicNanoTimestampVector v2 = (BasicNanoTimestampVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,1l,2l,3l);
        BasicNanoTimestampVector v3 = new BasicNanoTimestampVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testSecondCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicSecondVector v = new BasicSecondVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicSecondVector v1 = new BasicSecondVector(list1);
        BasicSecondVector v2 = (BasicSecondVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicSecondVector v3 = new BasicSecondVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testTimeCombine(){
        List<Integer> list = Arrays.asList(1,2,3);
        BasicTimeVector v = new BasicTimeVector(list);
        List<Integer> list1 = Arrays.asList(3,2,1);
        BasicTimeVector v1 = new BasicTimeVector(list1);
        BasicTimeVector v2 = (BasicTimeVector) v.combine(v1);
        List<Integer> list2 = Arrays.asList(1,2,3,3,2,1);
        BasicTimeVector v3 = new BasicTimeVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }
    @Test
    public void testTimeStampCombine(){
        List<Long> list = Arrays.asList(1l,2l,3l);
        BasicTimestampVector v = new BasicTimestampVector(list);
        List<Long> list1 = Arrays.asList(3l,2l,1l);
        BasicTimestampVector v1 = new BasicTimestampVector(list1);
        BasicTimestampVector v2 = (BasicTimestampVector) v.combine(v1);
        List<Long> list2 = Arrays.asList(1l,2l,3l,3l,2l,1l);
        BasicTimestampVector v3 = new BasicTimestampVector(list2);
        for (int i = 0;i<list2.size();i++){
            assertEquals(v3.get(i).getString() ,v2.get(i).getString());
        }
    }

    @Test
    public void test_Special_time_date(){
        LocalDate dt = LocalDate.of(2022,1,31);
        BasicDate date = new BasicDate(dt);
        assertEquals("2022.01.31",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2022,2,28);
        date = new BasicDate(dt);
        assertEquals("2022.02.28",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,2,29);
        date = new BasicDate(dt);
        assertEquals("2008.02.29",date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,3,31);
        date = new BasicDate(dt);
        String[] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,1,1);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,12,31);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2008,3,1);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

        dt = LocalDate.of(2000,2,29);
        date = new BasicDate(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDate());

  }

    @Test
    public void test_Special_time_datetime(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2);
        BasicDateTime date = new BasicDateTime(dt);
        String[] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2022,2,28,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,2,29,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,3,31,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,1,1,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,12,31,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2008,3,1,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

        dt = LocalDateTime.of(2000,2,29,2,2,2);
        date = new BasicDateTime(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        assertEquals(dt,date.getDateTime());

    }

    @Test
    public void test_Special_time_datehour (){
        Calendar calendar = Calendar.getInstance();
        calendar.set(2022,0,31,2,2,2);
        BasicDateHour date = new BasicDateHour(calendar);
        assertEquals("2022.01.31T02",date.getString());
        LocalDateTime dt = LocalDateTime.of(2022,2,28,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.02.28T02",date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2008.02.29T02",date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.03.31T02",date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.01.01T02",date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.12.31T02",date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2022.03.01T02",date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2);
        date = new BasicDateHour(dt);
        assertEquals("2000.02.29T02",date.getString());
    }


    @Test
    public void test_Special_time_timestamp(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2,4000000);
        BasicTimestamp date = new BasicTimestamp(dt);
        String [] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,2,28,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2,4000000);
        date = new BasicTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
    }

    @Test
    public void test_Special_time_nanotimestamp(){
        LocalDateTime dt = LocalDateTime.of(2022,1,31,2,2,2,32154365);
        BasicNanoTimestamp date = new BasicNanoTimestamp(dt);
        String [] lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,2,28,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2008,2,29,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,31,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,1,1,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,12,31,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2022,3,1,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
        dt = LocalDateTime.of(2000,2,29,2,2,2,32154365);
        date = new BasicNanoTimestamp(dt);
        lt=dt.toString().split("\\-");
        assertEquals(lt[0]+"."+lt[1]+"."+lt[2],date.getString());
    }
}

