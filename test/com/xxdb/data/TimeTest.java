package com.xxdb.data;

import java.time.LocalDateTime;
import java.time.LocalTime;

public class TimeTest{
    public static void main(String args[]) {
        {
            LocalDateTime dt = LocalDateTime.of(2017,8,11,10,03,10,2030);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 2030;
            if (nanoTime != Utils.countNanoseconds(dt)) {
                throw new RuntimeException("expect" + nanoTime + ", got" + Utils.countNanoseconds(dt));
            }
        }

        {
            LocalTime t = LocalTime.of(10, 03, 10, 2030);
            long nanoTime = t.getHour() * Utils.NANOS_PER_HOUR + t.getMinute() * Utils.NANOS_PER_MINUTE + t.getSecond() * Utils.NANOS_PER_SECOND + 2030;
            if (nanoTime != Utils.countNanoseconds(t)) {
                throw new RuntimeException("expect" + nanoTime + ", got" + Utils.countNanoseconds(t));
            }
        }

        {
            LocalDateTime dt = LocalDateTime.of(2000,12,31,12,12,12, 8500);
            BasicNanoTimestamp t = new BasicNanoTimestamp(dt);
            int days = Utils.countDays(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
            long nanoTime = days * Utils.NANOS_PER_DAY + dt.getHour() * Utils.NANOS_PER_HOUR + dt.getMinute() * Utils.NANOS_PER_MINUTE + dt.getSecond() * Utils.NANOS_PER_SECOND + 8500;
            if (nanoTime != t.getLong()) {
                throw new RuntimeException("expect" + nanoTime + ", got" + t.getLong());
            }
        }

        {
            LocalDateTime dt = Utils.parseNanoTimestamp(5);
            if (dt.getYear() != 1999)
                throw new RuntimeException("expect 1999, got" + dt.getYear());
            if (dt.getMonthValue() != 12)
                throw new RuntimeException("expect 12, got" + dt.getMonthValue());
            if (dt.getDayOfMonth() != 31)
                throw new RuntimeException("expect 31, got" + dt.getDayOfMonth());
            if (dt.getHour() != 0)
                throw new RuntimeException("expect 0, got" + dt.getHour());
            if (dt.getMinute() != 0)
                throw new RuntimeException("expect 0, got" + dt.getMinute());
            if (dt.getSecond() != 0)
                throw new RuntimeException("expect 0, got" + dt.getSecond());
            if (dt.getNano() != 5)
                throw new RuntimeException("expect 5, got" + dt.getNano());
        }

        {
            LocalTime dt = Utils.parseNanoTime(5);
            if (dt.getHour() != 0)
                throw new RuntimeException("expect 0, got" + dt.getHour());
            if (dt.getMinute() != 0)
                throw new RuntimeException("expect 0, got" + dt.getMinute());
            if (dt.getSecond() != 0)
                throw new RuntimeException("expect 0, got" + dt.getSecond());
            if (dt.getNano() != 5)
                throw new RuntimeException("expect 5, got" + dt.getNano());
        }

        {
            LocalTime dt = Utils.parseNanoTime(Utils.NANOS_PER_DAY - 5);
            if (dt.getHour() != 23)
                throw new RuntimeException("expect 0, got" + dt.getHour());
            if (dt.getMinute() != 59)
                throw new RuntimeException("expect 0, got" + dt.getMinute());
            if (dt.getSecond() != 59)
                throw new RuntimeException("expect 0, got" + dt.getSecond());
            if (dt.getNano() != Utils.NANOS_PER_SECOND - 5)
                throw new RuntimeException("expect " + (Utils.NANOS_PER_SECOND - 5) + ", got" + dt.getNano());
            System.out.println(dt);
        }
    }
}
