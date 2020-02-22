package com.xxdb.data;

import org.junit.Test;
import java.time.YearMonth;

import static org.junit.Assert.assertEquals;

public class UtilsTest {
    @Test
    public void test_countMonth(){
        //base month 0000.01
        assertEquals(24226, Utils.countMonths(YearMonth.of(2018,11)));
        assertEquals(24226, Utils.countMonths(2018,11));
        assertEquals(0, Utils.countMonths(0,1));
        assertEquals(0, Utils.countMonths(YearMonth.of(0,1)));
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
    public void countMonths() {
    }

    @Test
    public void countMonths1() {
    }

    @Test
    public void parseMonth() {
    }

    @Test
    public void countDays() {
    }

    @Test
    public void countDays1() {
    }

    @Test
    public void parseDate() {
    }

    @Test
    public void countSeconds() {
    }

    @Test
    public void countSeconds1() {
    }

    @Test
    public void divide() {
    }

    @Test
    public void parseDateTime() {
    }

    @Test
    public void countHours() {
    }

    @Test
    public void countHours1() {
    }

    @Test
    public void parseDateHour() {
    }

    @Test
    public void countMilliseconds() {
    }

    @Test
    public void countMilliseconds1() {
    }

    @Test
    public void countNanoseconds() {
    }

    @Test
    public void parseTimestamp() {
    }

    @Test
    public void parseNanoTimestamp() {
    }

    @Test
    public void countMilliseconds2() {
    }

    @Test
    public void countMilliseconds3() {
    }

    @Test
    public void countNanoseconds1() {
    }

    @Test
    public void parseTime() {
    }

    @Test
    public void parseNanoTime() {
    }

    @Test
    public void countSeconds2() {
    }

    @Test
    public void countSeconds3() {
    }

    @Test
    public void parseSecond() {
    }

    @Test
    public void countMinutes() {
    }

    @Test
    public void countMinutes1() {
    }

    @Test
    public void parseMinute() {
    }

    @Test
    public void murmur32() {
    }
}
