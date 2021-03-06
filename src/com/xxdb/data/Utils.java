package com.xxdb.data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;

public class Utils {
	public static final int DISPLAY_ROWS = 20;
	public static final int DISPLAY_COLS = 100;
	public static final int DISPLAY_WIDTH = 100;
	
	private static final int[] cumMonthDays={0,31,59,90,120,151,181,212,243,273,304,334,365};
	private static final int[] cumLeapMonthDays={0,31,60,91,121,152,182,213,244,274,305,335,366};
	private static final int[] monthDays={31,28,31,30,31,30,31,31,30,31,30,31};
	private static final int[] leapMonthDays={31,29,31,30,31,30,31,31,30,31,30,31};
	
	public static int countMonths(YearMonth date){
		return date.getYear() * 12 + date.getMonthValue() -1;
	}
	
	public static int countMonths(int year, int month){
		return year * 12 + month -1;
	}
	
	public static YearMonth parseMonth(int value){
		return YearMonth.of(value/12, value % 12 +1);
	}
	
	public static int countDays(LocalDate date){
		return countDays(date.getYear(), date.getMonthValue(),date.getDayOfMonth());
	}
	
	public static int countDays(int year, int month, int day){
		if(month<1 || month>12 || day<0)
			return Integer.MIN_VALUE;
	    int divide400Years = year / 400;
	    int offset400Years = year % 400;
	    int days = divide400Years * 146097 + offset400Years * 365 - 719529;
	    if(offset400Years > 0) days += (offset400Years - 1) / 4 + 1 - (offset400Years - 1) / 100;
	    if((year%4==0 && year%100!=0) || year%400==0){
			days+=cumLeapMonthDays[month-1];
			return day <= leapMonthDays[month - 1] ? days + day : Integer.MIN_VALUE;
		}
		else{
			days+=cumMonthDays[month-1];
			return day <= monthDays[month - 1] ? days + day : Integer.MIN_VALUE;
		}
	}

	public static LocalDate parseDate(int days){
		int year, month, day;
		days += 719529;
	    int circleIn400Years = days / 146097;
	    int offsetIn400Years = days % 146097;
	    int resultYear = circleIn400Years * 400;
	    int similarYears = offsetIn400Years / 365;
	    int tmpDays = similarYears * 365;
	    if(similarYears > 0) tmpDays += (similarYears - 1) / 4 + 1 - (similarYears - 1) / 100;
	    if(tmpDays >= offsetIn400Years) --similarYears;
	    year = similarYears + resultYear;
	    days -= circleIn400Years * 146097 + tmpDays;
	    boolean leap = ( (year%4==0 && year%100!=0) || year%400==0 );
	    if(days <= 0) {
	        days += leap ? 366 : 365;
	    }
	    if(leap){
			month=days/32+1;
			if(days>cumLeapMonthDays[month])
				month++;
			day=days-cumLeapMonthDays[month-1];
		}
		else{
			month=days/32+1;
			if(days>cumMonthDays[month])
				month++;
			day=days-cumMonthDays[month-1];
		}
		
		return LocalDate.of(year,month,day);
	}
	
	public static int countSeconds(LocalDateTime dt){
		return countSeconds(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute(), dt.getSecond());
	}
	
	public static int countSeconds(int year, int month, int day, int hour, int minute, int second){
		int days = countDays(year, month, day);
		return days * 86400 + (hour *60 + minute) * 60 + second;
	}
	
	public static int divide(int x, int y){
		int tmp=x / y;
		if(x>=0)
			return tmp;
		else if(x%y<0)
			return tmp-1;
		else
			return tmp;
	}
	
	public static LocalDateTime parseDateTime(int seconds){
		LocalDate date = Utils.parseDate(divide(seconds, 86400));
		seconds = seconds % 86400;
		if(seconds < 0)
			seconds += 86400;
		int hour = seconds/3600;
		seconds = seconds % 3600;
		int minute = seconds / 60;
		int second = seconds % 60;
		return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), hour, minute, second);
	}
	
	public static int countHours(LocalDateTime dt) {
		return countHours(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour());
	}
	
	public static int countHours(int year, int month, int day, int hour) {
		int days = countDays(year, month, day);
		return days * 24 + hour;
	}
	
	public static LocalDateTime parseDateHour(int hours){
		LocalDate date = Utils.parseDate(divide(hours, 24));
		hours = hours % 24;
		if (hours < 0)
			hours += 24;
		return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), hours, 0);
	}
	
	public static long countMilliseconds(LocalDateTime dt){
		long seconds = countSeconds(dt);
		return seconds * 1000 + dt.getNano() / 1000000;
	}
	
	public static long countMilliseconds(int year, int month, int day, int hour, int minute, int second, int millisecond){
		return countSeconds(year, month, day, hour, minute, second) * 1000L + millisecond;
	}
	public static long countNanoseconds(LocalDateTime dt) {
		long seconds = countSeconds(dt);
		return seconds * 1000000000l + dt.getNano();
	}

	/*
	 * 1 <==> 1970.01.01 00:00:00.001
	 * 0 <==> 1970.01.01 00:00:00.000
	 * -1 <==> 1969.12.31 23:59:59.999
	 * ...
	 */
	public static LocalDateTime parseTimestamp(long milliseconds){
		int days= (int)Math.floor(((double)milliseconds / 86400000.0));
		LocalDate date = Utils.parseDate(days);
		
		milliseconds = milliseconds % 86400000L;
		if(milliseconds < 0)
			milliseconds += 86400000;
		int millisecond = (int)(milliseconds % 1000);
		int seconds = (int)(milliseconds / 1000);
		int hour = seconds/3600;
		seconds = seconds % 3600;
		int minute = seconds / 60;
		int second = seconds % 60;
		return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), hour, minute, second, millisecond * 1000000);
	}

	public static final int HOURS_PER_DAY = 24;
	public static final int MINUTES_PER_HOUR = 60;
	public static final int SECONDS_PER_MINUTE = 60;
	public static final long NANOS_PER_SECOND = 1000_000_000L;
	public static final long NANOS_PER_MINUTE = NANOS_PER_SECOND * SECONDS_PER_MINUTE;
	public static final long NANOS_PER_HOUR = NANOS_PER_MINUTE * MINUTES_PER_HOUR;
	public static final long NANOS_PER_DAY = NANOS_PER_HOUR * HOURS_PER_DAY;
	public static final long MILLS_PER_DAY = NANOS_PER_DAY / 1000000;
	/*
	 * 1 <==> 1970.01.01 00:00:00.000000001
	 * 0 <==> 1970.01.01 00:00:00.000000000
	 * -1 <==> 1969.12.31 23:59:59.999999999
	 * ...
	 */
	public static LocalDateTime parseNanoTimestamp(long nanoseconds){
		int days=  (int)Math.floor(((double)nanoseconds / NANOS_PER_DAY));
		LocalDate date = Utils.parseDate(days);
		nanoseconds = nanoseconds % NANOS_PER_DAY;
		if (nanoseconds < 0)
			nanoseconds += NANOS_PER_DAY;
		LocalTime time = Utils.parseNanoTime(nanoseconds % NANOS_PER_DAY);
		return LocalDateTime.of(date, time);
	}
	public static int countMilliseconds(LocalTime time){
		return countMilliseconds(time.getHour(), time.getMinute(), time.getSecond(), time.getNano() / 1000000);
	}
	
	public static int countMilliseconds(int hour, int minute, int second, int millisecond){
		return ((hour * 60 + minute) * 60 + second) * 1000+millisecond;
	}

	public static long countNanoseconds(LocalTime time) {
		return (long)countMilliseconds(time.getHour(), time.getMinute(), time.getSecond(), 0) * 1000000 + time.getNano();
	}

	public static LocalTime parseTime(int milliseconds){
		return LocalTime.of(milliseconds/3600000, milliseconds/60000 % 60, milliseconds/1000 %60, milliseconds % 1000 *1000000);
	}

	public static LocalTime parseNanoTime(long nanoOfDay){
		return LocalTime.ofNanoOfDay(nanoOfDay);
	}

	public static int countSeconds(LocalTime time){
		return countSeconds(time.getHour(), time.getMinute(), time.getSecond());
	}
	
	public static int countSeconds(int hour, int minute, int second){
		return (hour * 60 + minute) * 60 + second;
	}
	
	public static LocalTime parseSecond(int seconds){
		return LocalTime.of(seconds / 3600, seconds % 3600 / 60, seconds % 60);
	}
	
	public static int countMinutes(LocalTime time){
		return countMinutes(time.getHour(), time.getMinute());
	}
	
	public static int countMinutes(int hour, int minute){
		return hour * 60 + minute;
	}
	
	public static LocalTime parseMinute(int minutes){
		return LocalTime.of(minutes / 60, minutes % 60);
	}
	
	public static int murmur32(final byte[] data, final int len, final int seed) {

	    int h = len;
	    int length4 = len / 4;

	    // do the bulk of the input
	    for (int i = 0; i < length4; i++) {
	        final int i4 = i * 4;
	        int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
	                + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
	        k *= 0x5bd1e995;
	        k ^= k >>> 24;
	        k *= 0x5bd1e995;
	        h *= 0x5bd1e995;
	        h ^= k;
	    }

	    // Handle the last few bytes of the input array
	    switch (len % 4) {
	        case 3:
	            h ^= (data[(len & ~3) + 2] & 0xff) << 16;
	        case 2:
	            h ^= (data[(len & ~3) + 1] & 0xff) << 8;
	        case 1:
	            h ^= (data[(len & ~3)] & 0xff);
	            h *= 0x5bd1e995;
	    }

	    h ^= h >>> 13;
	    h *= 0x5bd1e995;
	    h ^= h >>> 15;

	    return h;
	}
	
	
}
