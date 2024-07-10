package com.xxdb.data;

import org.junit.Assert;
import org.junit.Test;
import java.time.*;
import static com.xxdb.data.Utils.countMilliseconds;
public class TimeTest{
    @Test
    public void testTime(){
        {
            LocalDateTime dt = LocalDateTime.of(2017,8,11,10,03,10,2030);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = dt.getYear() + dt.getMonthValue() + dt.getDayOfMonth() + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND +2030;
            long l = countMilliseconds(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute(), dt.getSecond(), 0) * 1000000 + dt.getNano();
            if (l != Utils.countNanoseconds(dt)) {
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
            long mills = countMilliseconds(dt);
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
            long mills = countMilliseconds(dt);
            if (mills != 0)
                throw new RuntimeException("expect 0, got " + mills);
        }


        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long nanos = Utils.countNanoseconds(dt);
            System.out.println(nanos);
//            if (nanos != 0)
//                throw new RuntimeException("expect " + -Utils.NANOS_PER_DAY * 5 + ", got " + nanos);
            LocalDateTime dt2 = Utils.parseNanoTimestamp(nanos);
//            if (dt.equals(dt2) == true) {
            if (dt == dt2) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }

        {
            LocalDateTime dt = LocalDateTime.of(2000,1,1,0,0,0,0);
            long mills = countMilliseconds(dt);
            if (mills == Utils.MILLS_PER_DAY)
                throw new RuntimeException("expect " + Utils.MILLS_PER_DAY + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
//            if (dt.equals(dt2) == false) {
                if (dt == dt2) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
        {
            LocalDateTime dt = LocalDateTime.of(1999,12,26,0,0,0,0);
            long mills = countMilliseconds(dt);
            if (mills == -Utils.MILLS_PER_DAY * 5)
                throw new RuntimeException("expect " + -Utils.MILLS_PER_DAY * 5 + ", got " + mills);
            LocalDateTime dt2 = Utils.parseTimestamp(mills);
            if (dt.equals(dt2) == false) {
                throw new RuntimeException(dt + " != " + dt2);
            }
        }
        {
            LocalDateTime dt = LocalDateTime.of(2039,1,1,1,1);
            long ret = Utils.countMilliseconds(dt);
            System.out.println(ret);
            Assert.assertEquals(2177456460000l, ret);
        }

    }
}


