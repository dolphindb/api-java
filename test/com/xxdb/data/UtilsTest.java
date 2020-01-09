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

    public void test_parseMonth(){
        //base month 0000.01
        assertEquals(2018, Utils.parseMonth(24226).getYear());
        assertEquals(11, Utils.parseMonth(24226).getMonthValue());
        assertEquals(0, Utils.parseMonth(0).getYear());
        assertEquals(1, Utils.parseMonth(0).getMonthValue());
    }
}
