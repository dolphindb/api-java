package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;

import static org.junit.Assert.*;

public class TimeTest{

    @Test
    public void testTime(){
        {
            LocalDateTime dt = LocalDateTime.of(2017,8,11,10,03,10,2030);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND +2030;
            if (nanoTime != Utils.countNanoseconds(dt)) {
                throw new RuntimeException("expect " + nanoTime + ", got " + Utils.countNanoseconds(dt));
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
            if (dt.getYear() != 1970)
                throw new RuntimeException("expect 1970, got " + dt.getYear());
            if (dt.getMonthValue() != 1)
                throw new RuntimeException("expect 1, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 1)
                throw new RuntimeException("expect 1, got " + dt.getDayOfMonth());
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
            if (dt.getYear() != 1969)
                throw new RuntimeException("expect 1999, got " + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 31)
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
            if (dt.getYear() != 1969)
                throw new RuntimeException("expect 1999, got " + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
            if (dt.getDayOfMonth() != 31)
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
            LocalDateTime dt = LocalDateTime.of(1969,12,31,23,59,59,999000000);
            long mills = Utils.countMilliseconds(dt);
            if (mills != -1)
                throw new RuntimeException("expect -1, got " + mills);
        }

        {
            LocalDateTime dt = LocalDateTime.of(1970,01,01,0,0,0,000000005);
            long nanos = Utils.countNanoseconds(dt);
            if (nanos != 5)
                throw new RuntimeException("expect 5, got " + nanos);
        }

        {
            LocalDateTime dt = LocalDateTime.of(1970,1,1,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills != 0)
                throw new RuntimeException("expect 0, got " + mills);
        }


        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long nanos = Utils.countNanoseconds(dt);
            if (nanos != 0)
                throw new RuntimeException("expect " + -Utils.NANOS_PER_DAY * 5 + ", got " + nanos);
            LocalDateTime dt2 = Utils.parseNanoTimestamp(nanos);
            if (dt.equals(dt2) == true) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }

        {
            LocalDateTime dt = LocalDateTime.of(2000,1,1,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills == Utils.MILLS_PER_DAY)
                throw new RuntimeException("expect " + Utils.MILLS_PER_DAY + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long mills = Utils.countMilliseconds(dt);
            if (mills == -Utils.MILLS_PER_DAY * 5)
                throw new RuntimeException("expect " + -Utils.MILLS_PER_DAY * 5 + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
    }

//    @Test
//    public void testTime(){
//        {
//            LocalDateTime dt = LocalDateTime.of(2017,8,11,10,03,10,2030);
//            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
//            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 2030;
//            if (nanoTime != Utils.countNanoseconds(dt)) {
//                throw new RuntimeException("expect" + nanoTime + ", got " + Utils.countNanoseconds(dt));
//            }
//        }
//
//        {
//            LocalTime t = LocalTime.of(10, 03, 10, 2030);
//            long nanoTime = t.getHour() * Utils.NANOS_PER_HOUR + t.getMinute() * Utils.NANOS_PER_MINUTE + t.getSecond() * Utils.NANOS_PER_SECOND + 2030;
//            if (nanoTime != Utils.countNanoseconds(t)) {
//                throw new RuntimeException("expect" + nanoTime + ", got " + Utils.countNanoseconds(t));
//            }
//        }
//
//        {
//            LocalDateTime dt = LocalDateTime.of(2000,12,31,12,12,12, 8500);
//            BasicNanoTimestamp t = new BasicNanoTimestamp(dt);
//            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
//            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 8500;
//            if (nanoTime != t.getLong()) {
//                throw new RuntimeException("expect" + nanoTime + ", got " + t.getLong());
//            }
//        }
//
//        {
//            LocalDateTime dt = Utils.parseNanoTimestamp(5);
//            if (dt.getYear() != 1999)
//                throw new RuntimeException("expect 1999, got " + dt.getYear());
//            if (dt.getMonthValue() != 12)
//                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
//            if (dt.getDayOfMonth() != 31)
//                throw new RuntimeException("expect 31, got " + dt.getDayOfMonth());
//            if (dt.getHour() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getHour());
//            if (dt.getMinute() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getMinute());
//            if (dt.getSecond() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getSecond());
//            if (dt.getNano() != 5)
//                throw new RuntimeException("expect 5, got " + dt.getNano());
//        }
//
//        {
//            LocalDateTime dt = Utils.parseNanoTimestamp(-1);
//            if (dt.getYear() != 1999)
//                throw new RuntimeException("expect 1999, got " + dt.getYear());
//            if (dt.getMonthValue() != 12)
//                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
//            if (dt.getDayOfMonth() != 30)
//                throw new RuntimeException("expect 30, got " + dt.getDayOfMonth());
//            if (dt.getHour() != 23)
//                throw new RuntimeException("expect 23, got " + dt.getHour());
//            if (dt.getMinute() != 59)
//                throw new RuntimeException("expect 59, got " + dt.getMinute());
//            if (dt.getSecond() != 59)
//                throw new RuntimeException("expect 59, got " + dt.getSecond());
//            if (dt.getNano() != Utils.NANOS_PER_SECOND - 1)
//                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 1) + ", got " + dt.getNano());
//        }
//
//        {
//            LocalDateTime dt = Utils.parseTimestamp(-1);
//            if (dt.getYear() != 1999)
//                throw new RuntimeException("expect 1999, got " + dt.getYear());
//            if (dt.getMonthValue() != 12)
//                throw new RuntimeException("expect 12, got " + dt.getMonthValue());
//            if (dt.getDayOfMonth() != 30)
//                throw new RuntimeException("expect 30, got " + dt.getDayOfMonth());
//            if (dt.getHour() != 23)
//                throw new RuntimeException("expect 23, got " + dt.getHour());
//            if (dt.getMinute() != 59)
//                throw new RuntimeException("expect 59, got " + dt.getMinute());
//            if (dt.getSecond() != 59)
//                throw new RuntimeException("expect 59, got " + dt.getSecond());
//            if (dt.getNano() != Utils.NANOS_PER_SECOND - 1000000)
//                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 1000000) + ", got " + dt.getNano());
//        }
//        {
//            LocalTime dt = Utils.parseNanoTime(Utils.NANOS_PER_DAY - 5);
//            if (dt.getHour() != 23)
//                throw new RuntimeException("expect 0, got " + dt.getHour());
//            if (dt.getMinute() != 59)
//                throw new RuntimeException("expect 0, got " + dt.getMinute());
//            if (dt.getSecond() != 59)
//                throw new RuntimeException("expect 0, got " + dt.getSecond());
//            if (dt.getNano() != Utils.NANOS_PER_SECOND - 5)
//                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 5) + ", got " + dt.getNano());
//            System.out.println(dt);
//        }
//        {
//            LocalTime dt = Utils.parseNanoTime(5);
//            if (dt.getHour() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getHour());
//            if (dt.getMinute() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getMinute());
//            if (dt.getSecond() != 0)
//                throw new RuntimeException("expect 0, got " + dt.getSecond());
//            if (dt.getNano() != 5)
//                throw new RuntimeException("expect 5, got " + dt.getNano());
//        }
//
//        {
//            LocalDateTime dt = LocalDateTime.of(1999,12,30,23,59,59,999000000);
//            long mills = Utils.countMilliseconds(dt);
//            if (mills != -1)
//                throw new RuntimeException("expect -1, got " + mills);
//        }
//
//        {
//            LocalDateTime dt = LocalDateTime.of(1999,12,30,23,59,59,999999995);
//            long nanos = Utils.countNanoseconds(dt);
//            if (nanos != -5)
//                throw new RuntimeException("expect -1, got " + nanos);
//        }
//
//        {
//            LocalDateTime dt = LocalDateTime.of(1999,12,31,0,0,0,0);
//            long mills = Utils.countMilliseconds(dt);
//            if (mills != 0)
//                throw new RuntimeException("expect 0, got " + mills);
//        }
//
//
//        {
//            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
//            long nanos = Utils.countNanoseconds(dt);
//            if (nanos != -Utils.NANOS_PER_DAY * 5)
//                throw new RuntimeException("expect " + -Utils.NANOS_PER_DAY * 5 + ", got " + nanos);
//            LocalDateTime dt2 = Utils.parseNanoTimestamp(nanos);
//            if (dt.equals(dt2) == false) {
//                throw new RuntimeException(dt + " != " + dt2);
//            }
//        }
//
//        {
//            LocalDateTime dt = LocalDateTime.of(2000,1,1,0,0,0,0);
//            long mills = Utils.countMilliseconds(dt);
//            if (mills != Utils.MILLS_PER_DAY)
//                throw new RuntimeException("expect " + Utils.MILLS_PER_DAY + ", got " + mills);
//            LocalDateTime dt2 = Utils.parseTimestamp(mills);
//            if (dt.equals(dt2) == false) {
//                throw new RuntimeException(dt + " != " + dt2);
//            }
//        }
//        {
//            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
//            long mills = Utils.countMilliseconds(dt);
//            if (mills != -Utils.MILLS_PER_DAY * 5)
//                throw new RuntimeException("expect " + -Utils.MILLS_PER_DAY * 5 + ", got " + mills);
//            LocalDateTime dt2 = Utils.parseTimestamp(mills);
//            if (dt.equals(dt2) == false) {
//                throw new RuntimeException(dt + " != " + dt2);
//            }
//        }
//    }
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

    @Test
    public void test_BasicTime_constructor(){
        assertTrue(new BasicTime(new GregorianCalendar()).getString().contains(new SimpleDateFormat("HH:mm:ss").format(new Date())));
        assertFalse(new BasicTime(new GregorianCalendar()).equals(null));
    }

    @Test
    public void test_BasicTimeMatrix() throws Exception {
        BasicTimeMatrix btm = new BasicTimeMatrix(2,2);
        btm.setTime(0,0,LocalTime.of(1,7,44));
        btm.setTime(0,1,LocalTime.of(3,17));
        btm.setTime(1,0,LocalTime.of(11,36,52));
        btm.setTime(1,1,LocalTime.now());
        assertEquals("11:36:52",btm.getTime(1,0).toString());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btm.getDataCategory());
        assertEquals(BasicTime.class,btm.getElementClass());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{61000,63000});
        listofArrays.add(new int[]{65000,67000});
        BasicTimeMatrix btm2 = new BasicTimeMatrix(2,2,listofArrays);
        assertEquals("00:01:07",btm2.getTime(1,1).toString());
    }

    @Test
    public void test_BasicTimeVector(){
        int[] array = new int[]{53000,75000,115000,145000,Integer.MIN_VALUE};
        BasicTimeVector btv = new BasicTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("00:02:25",btv.getTime(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[00:00:53.000,00:01:55.000,00:01:15.000]",btv.getSubVector(indices).getString());
        btv.setTime(2,LocalTime.of(1,25,42));
        assertEquals("01:25:42",btv.getTime(2).toString());
        assertEquals(BasicTime.class,btv.getElementClass());
        BasicTimeVector btv2 = new BasicTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        assertNull(btv.getTime(4));
        BasicTimeVector btv3 = new BasicTimeVector(1);
        assertEquals("00:00",btv3.getTime(0).toString());
    }

    @Test
    public void test_BasicDateMatrix() throws Exception {
        BasicDateMatrix bdhm = new BasicDateMatrix(2,2);
        bdhm.setDate(0,0,LocalDate.of(2022,7,29));
        bdhm.setDate(0,1,LocalDate.of(1970,1,1));
        bdhm.setDate(1,0,LocalDate.of(1993,6,23));
        bdhm.setDate(1,1,LocalDate.MIN);
        assertEquals("1993-06-23",bdhm.getDate(1,0).toString());
        assertEquals(BasicDate.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861,7963});
        listofArrays.add(new int[]{4565,2467});
        BasicDateMatrix bdhm2 = new BasicDateMatrix(2,2,listofArrays);
        assertEquals("1982-07-02",bdhm2.getDate(0,1).toString());
    }

    @Test
    public void test_BasicDateVector() throws Exception{
        int[] array = new int[]{2861,7963,4565,2467,Integer.MIN_VALUE};
        BasicDateVector btv = new BasicDateVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("1976-10-03",btv.getDate(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[1977.11.01,1982.07.02,1991.10.21]",btv.getSubVector(indices).getString());
        btv.setDate(2,LocalDate.of(1984,7,14));
        assertEquals("1984-07-14",btv.getDate(2).toString());
        assertNull(btv.getDate(4));
        assertEquals(BasicDate.class,btv.getElementClass());
        BasicDateVector bdhv = new BasicDateVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("1970-01-01",bdhv.getDate(0).toString());
        List<Integer> list = Arrays.asList(2861,7963,4565,2467,Integer.MIN_VALUE);
        BasicDateVector bdhv2 = new BasicDateVector(list);
        assertEquals("1991-10-21",bdhv2.getDate(1).toString());
        ByteBuffer bb = ByteBuffer.allocate(10000);
        bdhv2.writeVectorToBuffer(bb);
    }

    @Test
    public void test_BasicTime(){
        assertFalse(new BasicDate(new GregorianCalendar()).equals(null));
        assertFalse(new BasicDateHour(2).equals(null));
        assertFalse(new BasicDateTime(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMinute(LocalTime.now()).equals(null));
        assertFalse(new BasicMinute(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMonth(2022, Month.JULY).equals(null));
        assertFalse(new BasicMonth(new GregorianCalendar()).equals(null));
        assertFalse(new BasicMonth(YearMonth.of(2022,7)).equals(null));
        assertFalse(new BasicNanoTime(LocalTime.now()).equals(null));
        assertFalse(new BasicNanoTimestamp(3000L).equals(null));
        assertFalse(new BasicSecond(LocalTime.now()).equals(null));
        assertFalse(new BasicSecond(new GregorianCalendar()).equals(null));
        assertFalse(new BasicTimestamp(new GregorianCalendar()).equals(null));
    }
    @Test
    public void test_BasicDateHourMatrix() throws Exception {
        BasicDateHourMatrix bdhm = new BasicDateHourMatrix(2,2);
        bdhm.setDateHour(0,0,LocalDateTime.of(2022,7,29,11,07));
        bdhm.setDateHour(0,1,LocalDateTime.of(1970,1,1,11,11));
        bdhm.setDateHour(1,0,LocalDateTime.of(1993,6,23,15,36));
        bdhm.setDateHour(1,1,LocalDateTime.MIN);
        assertEquals("1993-06-23T15:00",bdhm.getDateHour(1,0).toString());
        assertEquals(BasicDateHour.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{2861000,7963000});
        listofArrays.add(new int[]{4565000,2467000});
        BasicDateHourMatrix bdhm2 = new BasicDateHourMatrix(2,2,listofArrays);
        assertEquals("2490-10-09T08:00",bdhm2.getDateHour(0,1).toString());
    }

    @Test
    public void test_BasicDateHourVector() throws Exception{
        int[] array = new int[]{2861000,7963000,4565000,2467000,Integer.MIN_VALUE};
        BasicDateHourVector btv = new BasicDateHourVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("2251-06-08T16:00",btv.getDateHour(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[2296.05.19T08,2490.10.09T08,2878.05.31T16]",btv.getSubVector(indices).getString());
        btv.setDateHour(2,LocalDateTime.of(1984,7,14,11,25));
        assertEquals("1984-07-14T11:00",btv.getDateHour(2).toString());
        assertNull(btv.getDateHour(4));
        assertEquals(BasicDateHour.class,btv.getElementClass());
        BasicDateHourVector bdhv = new BasicDateHourVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("1970-01-01T00:00",bdhv.getDateHour(0).toString());
        List<Integer> list = Arrays.asList(2861000,7963000,4565000,2467000,Integer.MIN_VALUE);
        BasicDateHourVector bdhv2 = new BasicDateHourVector(list);
        assertEquals("2878-05-31T16:00",bdhv2.getDateHour(1).toString());
        ByteBuffer bb = ByteBuffer.allocate(10000);
        bdhv2.writeVectorToBuffer(bb);
    }

    @Test
    public void test_BasicMinuteMatrix() throws Exception{
        BasicMinuteMatrix bdhm = new BasicMinuteMatrix(2,2);
        bdhm.setMinute(0,0,LocalTime.of(1,9,4));
        bdhm.setMinute(0,1,LocalTime.of(1,11,11));
        bdhm.setMinute(1,0,LocalTime.of(23,15,36));
        bdhm.setMinute(1,1,LocalTime.MIN);
        assertEquals("23:15",bdhm.getMinute(1,0).toString());
        assertEquals(BasicMinute.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{125,300});
        listofArrays.add(new int[]{456,200});
        BasicMinuteMatrix bdhm2 = new BasicMinuteMatrix(2,2,listofArrays);
        assertEquals("05:00",bdhm2.getMinute(1,0).toString());
    }

    @Test
    public void test_BasicMinuteVector(){
        int[] array = new int[]{286,796,456,246,Integer.MIN_VALUE};
        BasicMinuteVector btv = new BasicMinuteVector(array,true);
        assertNull(btv.getMinute(4));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("04:06",btv.getMinute(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[04:46m,07:36m,13:16m]",btv.getSubVector(indices).getString());
        btv.setMinute(2,LocalTime.of(14,11,25));
        assertEquals("14:11",btv.getMinute(2).toString());
        assertEquals(BasicMinute.class,btv.getElementClass());
        BasicMinuteVector bdhv = new BasicMinuteVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("00:00",bdhv.getMinute(0).toString());
        List<Integer> list = Arrays.asList(286,796,456,246,Integer.MIN_VALUE);
        BasicMinuteVector bdhv2 = new BasicMinuteVector(list);
        assertEquals("13:16",bdhv2.getMinute(1).toString());
        BasicMinuteVector bmv = new BasicMinuteVector(5);
        bmv = bdhv2;
        assertEquals("07:36",bmv.getMinute(2).toString());
    }

    @Test
    public void test_BasicMonthMatrix() throws Exception{
        BasicMonthMatrix bdhm = new BasicMonthMatrix(2,2);
        bdhm.setMonth(0,0,YearMonth.of(1978,12));
        bdhm.setMonth(0,1,YearMonth.of(1997,6));
        bdhm.setMonth(1,0,YearMonth.of(1999,12));
        bdhm.setMonth(1,1,YearMonth.of(2008,8));
        assertEquals("1999-12",bdhm.getMonth(1,0).toString());
        assertEquals(YearMonth.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<int[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new int[]{23641,23995});
        listofArrays.add(new int[]{24104,1201});
        BasicMonthMatrix bdhm2 = new BasicMonthMatrix(2,2,listofArrays);
        assertEquals("1999-08",bdhm2.getMonth(1,0).toString());
    }

    @Test
    public void test_BasicMonthVector(){
        int[] array = new int[]{23641,23995,24104,1201};
        BasicMonthVector btv = new BasicMonthVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("0100-02",btv.getMonth(3).toString());
        int[] indices = new int[]{0,2,1};
        assertEquals("[1970.02M,2008.09M,1999.08M]",btv.getSubVector(indices).getString());
        btv.setMonth(2,YearMonth.of(2012,12));
        assertEquals("2012-12",btv.getMonth(2).toString());
        assertEquals(YearMonth.class,btv.getElementClass());
        BasicMonthVector bdhv = new BasicMonthVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("0000-01",bdhv.getMonth(0).toString());
        List<Integer> list = Arrays.asList(23641,23995,24104,1201);
        BasicMonthVector bdhv2 = new BasicMonthVector(list);
        assertEquals("1999-08",bdhv2.getMonth(1).toString());
        BasicMonthVector bmv = new BasicMonthVector(4);
        bmv = bdhv2;
        assertEquals("2008-09",bmv.getMonth(2).toString());
    }

    @Test
    public void test_BasicNanoTimeMatrix() throws Exception{
        BasicNanoTimeMatrix bdhm = new BasicNanoTimeMatrix(2,2);
        bdhm.setNanoTime(0,0,LocalTime.of(19,12,25));
        bdhm.setNanoTime(0,1,LocalTime.of(20,13,35));
        bdhm.setNanoTime(1,0,LocalTime.of(21,14,45));
        bdhm.setNanoTime(1,1,LocalTime.of(22,15,55));
        assertEquals("21:14:45",bdhm.getNanoTime(1,0).toString());
        assertEquals(BasicNanoTime.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicNanoTimeMatrix bdhm2 = new BasicNanoTimeMatrix(2,2,listofArrays);
        assertEquals("06:39:55.876902",bdhm2.getNanoTime(1,0).toString());
    }

    @Test
    public void test_BasicNanoTimeVector(){
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimeVector btv = new BasicNanoTimeVector(array,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,btv.getDataCategory());
        assertEquals("03:20:13.435579",btv.getNanoTime(3).toString());
        assertNull(btv.getNanoTime(4));
        int[] indices = new int[]{0,2,1};
        assertEquals("[06:34:01.343568000,06:41:44.786790000,06:39:55.876902000]",btv.getSubVector(indices).getString());
        btv.setNanoTime(2,LocalTime.of(22,12,17));
        assertEquals("22:12:17",btv.getNanoTime(2).toString());
        assertEquals(BasicNanoTime.class,btv.getElementClass());
        BasicNanoTimeVector bdhv = new BasicNanoTimeVector(Entity.DATA_FORM.DF_VECTOR,1);
        assertEquals("00:00",bdhv.getNanoTime(0).toString());
        BasicNanoTimeVector bmv = new BasicNanoTimeVector(4);
        bmv = btv;
        assertEquals("22:12:17",bmv.getNanoTime(2).toString());
    }
    @Test
    public void test_BasicNanoTimeStampMatrix() throws Exception{
        BasicNanoTimestampMatrix bdhm = new BasicNanoTimestampMatrix(2,2);
        bdhm.setNanoTimestamp(0,0,LocalDateTime.of(1970,11,22,19,12,25));
        bdhm.setNanoTimestamp(0,1,LocalDateTime.of(1978,12,13,20,13,35));
        bdhm.setNanoTimestamp(1,0,LocalDateTime.of(1984,5,18,21,14,45));
        bdhm.setNanoTimestamp(1,1,LocalDateTime.of(1987,12,12,22,15,55));
        assertEquals("1984-05-18T21:14:45",bdhm.getNanoTimestamp(1,0).toString());
        assertEquals(BasicNanoTimestamp.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicNanoTimestampMatrix bdhm2 = new BasicNanoTimestampMatrix(2,2,listofArrays);
        assertEquals("1970-01-01T06:39:55.876902",bdhm2.getNanoTimestamp(1,0).toString());
    }

    @Test
    public void test_BasicNanoTimeStampVector(){
        BasicNanoTimestampVector bnts = new BasicNanoTimestampVector(Entity.DATA_FORM.DF_VECTOR,5);
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicNanoTimestampVector btv = new BasicNanoTimestampVector(array,true);
        assertNull(btv.getNanoTimestamp(4));
        bnts = btv;
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bnts.getDataCategory());
        assertEquals("[1970.01.01T03:20:13.435579000,1970.01.01T06:34:01.343568000," +
                "1970.01.01T06:39:55.876902000,1970.01.01T06:41:44.786790000]",btv.getSubVector(new int[]{3,0,1,2}).getString());
        assertEquals(BasicNanoTimestamp.class,bnts.getElementClass());
        btv.setNanoTimestamp(4,LocalDateTime.MIN);
        assertEquals("1982-02-08T12:37:20",btv.getNanoTimestamp(4).toString());
        assertEquals("1970-01-01T06:41:44.786790",bnts.getNanoTimestamp(2).toString());
    }

    @Test
    public void test_BasicSecondMatrix() throws Exception {
        BasicSecondMatrix bsm = new BasicSecondMatrix(2,2);
        bsm.setSecond(0,0,LocalTime.of(16,19,53));
        bsm.setSecond(0,1,LocalTime.of(11,11,11));
        bsm.setSecond(1,0,LocalTime.of(7,55,18));
        bsm.setSecond(1,1,LocalTime.of(23,57,55));
        assertEquals("11:11:11",bsm.getSecond(0,1).toString());
        assertEquals("23:57:55",bsm.get(1,1).getString());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bsm.getDataCategory());
        assertEquals(BasicSecond.class,bsm.getElementClass());
        List<int[]> listofArrays = new ArrayList<>();
        listofArrays.add(new int[]{28800,43215});
        listofArrays.add(new int[]{21600,54800});
        BasicSecondMatrix bsm2 = new BasicSecondMatrix(2,2,listofArrays);
        assertEquals("12:00:15",bsm2.getSecond(1,0).toString());
    }

    @Test
    public void test_BasicSecondVector(){
        BasicSecondVector bsv = new BasicSecondVector(4);
        bsv.setSecond(0,LocalTime.of(16,19,53));
        bsv.setSecond(1,LocalTime.of(11,11,11));
        bsv.setSecond(2,LocalTime.of(7,55,18));
        bsv.setSecond(3,LocalTime.of(23,57,55));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bsv.getDataCategory());
        assertEquals(BasicSecond.class,bsv.getElementClass());
        BasicSecondVector bsv2 = new BasicSecondVector(Entity.DATA_FORM.DF_VECTOR,4);
        bsv2 = bsv;
        assertEquals("23:57:55",bsv2.getSecond(3).toString());
        int[] array = new int[]{28800,43215,21630,54845,Integer.MIN_VALUE};
        BasicSecondVector bsv3 = new BasicSecondVector(array,true);
        assertEquals("[08:00:00,06:00:30,12:00:15]",bsv3.getSubVector(new int[]{0,2,1}).getString());
        assertNull(bsv3.getSecond(4));
    }

    @Test
    public void test_BasicTimeStampMatrix() throws Exception {
        BasicTimestampMatrix bdhm = new BasicTimestampMatrix(2,2);
        bdhm.setTimestamp(0,0,LocalDateTime.of(1970,11,22,19,12,25));
        bdhm.setTimestamp(0,1,LocalDateTime.of(1978,12,13,20,13,35));
        bdhm.setTimestamp(1,0,LocalDateTime.of(1984,5,18,21,14,45));
        bdhm.setTimestamp(1,1,LocalDateTime.of(1987,12,12,22,15,55));
        assertEquals("1984-05-18T21:14:45",bdhm.getTimestamp(1,0).toString());
        assertEquals(BasicTimestamp.class,bdhm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdhm.getDataCategory());
        List<long[]> listofArrays = new ArrayList<>(2);
        listofArrays.add(new long[]{23641343568000L,23995876902000L});
        listofArrays.add(new long[]{24104786790000L,12013435579000L});
        BasicTimestampMatrix bdhm2 = new BasicTimestampMatrix(2,2,listofArrays);
        assertEquals("2730-05-27T01:21:42",bdhm2.getTimestamp(1,0).toString());
    }

    @Test
    public void test_BasicTimeStampVector(){
        BasicTimestampVector btsv = new BasicTimestampVector(Entity.DATA_FORM.DF_VECTOR,5);
        long[] array = new long[]{23641343568000L,23995876902000L,24104786790000L,12013435579000L,Long.MIN_VALUE};
        BasicTimestampVector btv = new BasicTimestampVector(array,true);
        btsv = btv;
        assertEquals(BasicTimestamp.class,btsv.getElementClass());
        assertNull(btsv.getTimestamp(4));
        assertEquals("2733-11-07T14:06:30",btsv.getTimestamp(2).toString());
    }

    @Test
    public void test_BasicDateTimeMatrix(){
        BasicDateTimeMatrix bdtm = new BasicDateTimeMatrix(2,2);
        bdtm.setDateTime(0,0,LocalDateTime.of(2022,8,10,15,55));
        bdtm.setDateTime(0,1,LocalDateTime.of(1978,12,13,17,58));
        bdtm.setDateTime(1,0,LocalDateTime.MIN);
        bdtm.setDateTime(1,1,LocalDateTime.MAX);
        assertEquals("1966-02-13T07:02:23",bdtm.getDateTime(1,1).toString());
        assertEquals("1982.02.08T12:37:20",bdtm.get(1,0).getString());
        assertEquals(BasicDateTime.class,bdtm.getElementClass());
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdtm.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_DATETIME,bdtm.getDataType());
    }

    @Test
    public void test_BasicDateTimeMatrix_list() throws Exception {
        List<int[]> list = new ArrayList<>();
        int[] a = new int[]{237651730,257689940};
        int[] b = new int[]{323537820,454523230};
        list.add(a);
        list.add(b);
        BasicDateTimeMatrix bdtm = new BasicDateTimeMatrix(2,2,list);
        ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
        String HOST = bundle.getString("HOST");
        int PORT = Integer.parseInt(bundle.getString("PORT"));
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeMatrix",bdtm);
        conn.upload(map);
        BasicDateTimeMatrix bdtm2 = (BasicDateTimeMatrix) conn.run("dateTimeMatrix");
        assertEquals("1984-05-27T16:27:10",bdtm2.getDateTime(1,1).toString());
        conn.close();
    }

    @Test
    public void test_BasicDateTimeVector() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        bdtv.setDateTime(0,LocalDateTime.MIN);
        bdtv.setDateTime(1,LocalDateTime.MAX);
        bdtv.setDateTime(2,LocalDateTime.now());
        assertEquals("1982-02-08T12:37:20",bdtv.getDateTime(0).toString());
        bdtv.setNull(2);
        assertNull(bdtv.getDateTime(2));
        ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
        String HOST = bundle.getString("HOST");
        int PORT = Integer.parseInt(bundle.getString("PORT"));
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Map<String,Entity> map = new HashMap<>();
        map.put("dateTimeVector",bdtv);
        conn.upload(map);
        BasicDateTimeVector bdtv2 = (BasicDateTimeVector) conn.run("dateTimeVector");
        assertEquals(BasicDateTime.class,bdtv2.getElementClass());
        conn.close();
    }

    @Test
    public void test_BasicDateTimeVector_wvtb() throws IOException {
        BasicDateTimeVector bdtv = new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR,3);
        bdtv.setDateTime(0,LocalDateTime.MIN);
        bdtv.setDateTime(1,LocalDateTime.MAX);
        bdtv.setDateTime(2,LocalDateTime.now());
        ByteBuffer bb = bdtv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals(0,bb.get());
    }

    @Test
    public void test_BasicDateTimeVector_other(){
        int[] arr = new int[]{32245761,43556722,53367869};
        BasicDateTimeVector bdtv = new BasicDateTimeVector(arr,true);
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,bdtv.getDataCategory());
        assertEquals("[1971.09.10T16:24:29,1971.01.09T05:09:21,1971.05.20T03:05:22]",bdtv.getSubVector(new int[]{2,0,1}).getString());
    }
}


