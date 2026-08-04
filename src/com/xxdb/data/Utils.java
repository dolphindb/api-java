package com.xxdb.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.*;
import java.util.*;
import com.xxdb.io.Double2;
import com.xxdb.io.Long2;
import com.xxdb.data.Entity.DATA_CATEGORY;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.xxdb.data.Entity.DATA_TYPE.*;


public class Utils {

	public static final String JAVA_API_VERSION = "3.00.6.0";

	public static final int DISPLAY_ROWS = 20;
	public static final int DISPLAY_COLS = 100;
	public static final int DISPLAY_WIDTH = 100;
	
	private static final int[] cumMonthDays={0,31,59,90,120,151,181,212,243,273,304,334,365};
	private static final int[] cumLeapMonthDays={0,31,60,91,121,152,182,213,244,274,305,335,366};
	private static final int[] monthDays={31,28,31,30,31,30,31,31,30,31,30,31};
	private static final int[] leapMonthDays={31,29,31,30,31,30,31,31,30,31,30,31};

	public static int SCALE = -1;

	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static String getJavaApiVersion() {
		return JAVA_API_VERSION;
	}

	public static void setFormat(int scale){
		SCALE = scale;
	}
	
	public static int countMonths(YearMonth date){
		return date.getYear() * 12 + date.getMonthValue()-1;
	}
	
	public static int countMonths(int year, int month){
		return year * 12 + month -1;
	}
	
	public static int countMonths(int days){
		int year, month;
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
		}
		else{
			month=days/32+1;
			if(days>cumMonthDays[month])
				month++;
		}
		
		return year * 12 + month -1;
	}


	public static YearMonth parseMonth(int value){
		return YearMonth.of(value/12, value % 12 + 1);
	}
	
	public static int countDays(LocalDate date){
		return countDays(date.getYear(), date.getMonthValue(),date.getDayOfMonth());
	}

	public static int countDays(Calendar calendar) {
		return countDays(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1, calendar.get(Calendar.DAY_OF_MONTH));
	}

	public static int countDays(int year, int month, int day){
		if(month<1 || month>12 || day<0){
			return Integer.MIN_VALUE;
		}
	    int divide400Years = year / 400;
	    int offset400Years = year % 400;
		int days;
		days = divide400Years * 146097 + offset400Years * 365 - 719529;
	    if(offset400Years > 0) days += (offset400Years - 1) / 4 + 1 - (offset400Years - 1) / 100;
	    if((year%4==0 && year%100!=0) || year%400==0){
			days+=cumLeapMonthDays[month-1];
			return day <= leapMonthDays[month - 1] ? days + day : Integer.MIN_VALUE;
		}
		else{
			days+=cumMonthDays[month-1];
			return day <= monthDays[month-1] ? days + day : Integer.MIN_VALUE;
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
		return countDTSeconds(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute(), dt.getSecond());
	}

	private static long countSecondsToLong(LocalDateTime dt){
		return countDTSecondsToLong(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute(), dt.getSecond());
	}

	public static int countSeconds(Calendar value){
		return countSeconds(value.get(Calendar.HOUR_OF_DAY), value.get(Calendar.MINUTE),value.get(Calendar.SECOND));
	}


	public static int countDTSeconds(Calendar value) {
		return countDTSeconds(value.get(Calendar.YEAR), value.get(Calendar.MONTH)+1, value.get(Calendar.DAY_OF_MONTH),
				value.get(Calendar.HOUR_OF_DAY), value.get(Calendar.MINUTE), value.get(Calendar.SECOND));
	}

	public static int countDTSeconds(int year, int month, int day, int hour, int minute, int second){
		int days = countDays(year, month, day);
		return days * 86400 + (hour *60 + minute) * 60 + second;
	}

	private static long countDTSecondsToLong(int year, int month, int day, int hour, int minute, int second){
		long days = countDays(year, month, day);
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
	
	public static long divide(long x, long y){
		long tmp=x / y;
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

	public static int countHours(Calendar calendar) {
		return countHours(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY));
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
		long seconds = countSecondsToLong(dt);
		return seconds * 1000 + dt.getNano() / 1000000;
	}
	
	public static long countMilliseconds(int year, int month, int day, int hour, int minute, int second, int millisecond){
		return countDTSeconds(year, month, day, hour, minute, second) * 1000L + millisecond;
	}
	public static long countDTNanoseconds(LocalDateTime dt) {
		long seconds = countSecondsToLong(dt);
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
		int days=  (int)(nanoseconds / NANOS_PER_DAY);
		if (nanoseconds < 0 && nanoseconds%NANOS_PER_DAY != 0){
			days -= 1;
		}
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
	public static int countMilliseconds(Calendar value) {
		return countMilliseconds(value.get(Calendar.HOUR_OF_DAY),
				value.get(Calendar.MINUTE),
				value.get(Calendar.SECOND),
				value.get(Calendar.MILLISECOND));
	}

	public static long countDateMilliseconds(Calendar value) {
		return countMilliseconds(value.get(Calendar.YEAR),
				value.get(Calendar.MONTH)+1,
				value.get(Calendar.DAY_OF_MONTH),
				value.get(Calendar.HOUR_OF_DAY),
				value.get(Calendar.MINUTE),
				value.get(Calendar.SECOND),
				value.get(Calendar.MILLISECOND));
	}
	
	public static int countMilliseconds(int hour, int minute, int second, int millisecond){
		return ((hour * 60 + minute) * 60 + second) * 1000+millisecond;
	}

	public static long countNanoseconds(LocalTime time) {
		return (long)countMilliseconds(time.getHour(), time.getMinute(), time.getSecond(), 0) * 1000000 + time.getNano();
	}

	public static long countNanoseconds(LocalDateTime time) {
		return (long)countMilliseconds(time.getYear(), time.getMonthValue(), time.getDayOfMonth(), time.getHour(), time.getMinute(), time.getSecond(), 0) * 1000000 + time.getNano();
	}

	static int[] dateVectorValues(LocalDate[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countDays(values[i]);
		}
		return data;
	}

	static int[] dateVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countDays(values[i]);
		}
		return data;
	}

	static int[] monthVectorValues(YearMonth[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countMonths(values[i]);
		}
		return data;
	}

	static int[] monthVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : values[i].get(Calendar.YEAR) * 12 + values[i].get(Calendar.MONTH);
		}
		return data;
	}

	static int[] dateHourVectorValues(LocalDateTime[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countHours(values[i]);
		}
		return data;
	}

	static int[] dateHourVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countHours(values[i]);
		}
		return data;
	}

	static int[] dateTimeVectorValues(LocalDateTime[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countSeconds(values[i]);
		}
		return data;
	}

	static int[] dateTimeVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countDTSeconds(values[i]);
		}
		return data;
	}

	static long[] timestampVectorValues(LocalDateTime[] values) {
		Objects.requireNonNull(values, "values");
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Long.MIN_VALUE : countMilliseconds(values[i]);
		}
		return data;
	}

	static long[] timestampVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Long.MIN_VALUE : countDateMilliseconds(values[i]);
		}
		return data;
	}

	static long[] nanoTimestampVectorValues(LocalDateTime[] values) {
		Objects.requireNonNull(values, "values");
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Long.MIN_VALUE : countDTNanoseconds(values[i]);
		}
		return data;
	}

	static int[] timeVectorValues(LocalTime[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countMilliseconds(values[i]);
		}
		return data;
	}

	static int[] timeVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countMilliseconds(values[i]);
		}
		return data;
	}

	static int[] secondVectorValues(LocalTime[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countSeconds(values[i]);
		}
		return data;
	}

	static int[] secondVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countSeconds(values[i]);
		}
		return data;
	}

	static int[] minuteVectorValues(LocalTime[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countMinutes(values[i]);
		}
		return data;
	}

	static int[] minuteVectorValues(Calendar[] values) {
		Objects.requireNonNull(values, "values");
		int[] data = new int[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Integer.MIN_VALUE : countMinutes(values[i]);
		}
		return data;
	}

	static long[] nanoTimeVectorValues(LocalTime[] values) {
		Objects.requireNonNull(values, "values");
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Long.MIN_VALUE : countNanoseconds(values[i]);
		}
		return data;
	}

	static long[] nanoTimeVectorValues(LocalDateTime[] values) {
		Objects.requireNonNull(values, "values");
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			data[i] = values[i] == null ? Long.MIN_VALUE : countNanoseconds(values[i].toLocalTime());
		}
		return data;
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

	public static int countMinutes(Calendar value){
		return countMinutes(value.get(Calendar.HOUR_OF_DAY),value.get(Calendar.MINUTE));
	}

	public static int countMinutes(int hour, int minute){
		return hour * 60 + minute;
	}

	public static LocalTime parseMinute(int minutes) {
		int hours = minutes / 60;
		int remainingMinutes = minutes % 60;

		if (minutes < 0) {
			hours = (hours - 1 + 24) % 24;
			remainingMinutes = 60 - Math.abs(remainingMinutes);
		}

		return LocalTime.of(hours, remainingMinutes);
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
	
	public static DATA_CATEGORY getCategory(DATA_TYPE type){
		if(type== DATA_TYPE.DT_TIME || type==DATA_TYPE.DT_SECOND || type==DATA_TYPE.DT_MINUTE || type==DATA_TYPE.DT_DATE || type==DATA_TYPE.DT_DATEHOUR 
				|| type==DATA_TYPE.DT_DATEMINUTE || type==DATA_TYPE.DT_DATETIME || type==DATA_TYPE.DT_MONTH || type==DATA_TYPE.DT_NANOTIME 
				|| type==DATA_TYPE.DT_NANOTIMESTAMP || type==DATA_TYPE.DT_TIMESTAMP)
			return DATA_CATEGORY.TEMPORAL;
		else if(type==DATA_TYPE.DT_INT || type==DATA_TYPE.DT_LONG || type==DATA_TYPE.DT_SHORT || type==DATA_TYPE.DT_BYTE)
			return DATA_CATEGORY.INTEGRAL;
		else if(type==DATA_TYPE.DT_BOOL)
			return DATA_CATEGORY.LOGICAL;
		else if(type==DATA_TYPE.DT_DOUBLE || type==DATA_TYPE.DT_FLOAT)
			return DATA_CATEGORY.FLOATING;
		else if(type==DATA_TYPE.DT_STRING || type==DATA_TYPE.DT_SYMBOL || type == DATA_TYPE.DT_BLOB)
			return DATA_CATEGORY.LITERAL;
		else if(type==DATA_TYPE.DT_INT128 || type==DATA_TYPE.DT_UUID || type==DATA_TYPE.DT_IPADDR)
			return DATA_CATEGORY.BINARY;
		else if(type==DATA_TYPE.DT_ANY || type == DATA_TYPE.DT_IOTANY)
			return DATA_CATEGORY.MIXED;
		else if(type==DATA_TYPE.DT_VOID)
			return DATA_CATEGORY.NOTHING;
		else if(type == DATA_TYPE.DT_DECIMAL32 || type == DATA_TYPE.DT_DECIMAL64 || type == DATA_TYPE.DT_DECIMAL128)
			return DATA_CATEGORY.DENARY;
		else
			return DATA_CATEGORY.SYSTEM;
	}
	
	public static Entity toMonth(Entity source){
		long scaleFactor = 1;
		int days;
		
		if(source.isScalar()){
			switch(source.getDataType()){
			case DT_NANOTIMESTAMP:
				scaleFactor = 86400000000000l;
				days = (int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor);
				return new BasicMonth(countMonths(days));
			case DT_TIMESTAMP:
				scaleFactor = 86400000;
				days = (int)divide(((BasicTimestamp)source).getLong(), scaleFactor);
				return new BasicMonth(countMonths(days));
			case DT_DATETIME:
				scaleFactor = 86400;
				days = divide(((BasicDateTime)source).getInt(), (int)scaleFactor);
				return new BasicMonth(countMonths(days));
			case DT_DATE:
				return new BasicMonth(countMonths(((BasicDate)source).getInt()));
			default:
				throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.");
			}
		}
		else{
			int rows = source.rows();
			int[] values = new int[rows];
			
			switch(source.getDataType()){
			case DT_NANOTIMESTAMP:
				scaleFactor = 86400000000000l;
				BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = countMonths((int)divide(ntsVec.getLong(i), scaleFactor));
				}
				return new BasicMonthVector(values);
			case DT_TIMESTAMP:
				scaleFactor = 86400000;
				BasicTimestampVector tsVec = (BasicTimestampVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = countMonths((int)divide(tsVec.getLong(i), scaleFactor));
				}
				return new BasicMonthVector(values);
			case DT_DATETIME:
				scaleFactor = 86400;
				BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = countMonths(divide(dtVec.getInt(i), (int)scaleFactor));
				}
				return new BasicMonthVector(values);
			case DT_DATE:
				BasicDateVector dVec = (BasicDateVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = countMonths(dVec.getInt(i));
				}
				return new BasicMonthVector(values);
			default:
				throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.");
			}
		}
	}
	
	public static Entity toDate(Entity source){
		if(source.isScalar()){
			long scaleFactor = 1;
			switch(source.getDataType()){
			case DT_NANOTIMESTAMP:
				scaleFactor = 86400000000000l;
				return new BasicDate((int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor));
			case DT_TIMESTAMP:
				scaleFactor = 86400000;
				return new BasicDate((int)divide(((BasicTimestamp)source).getLong(), scaleFactor));
			case DT_DATETIME:
				scaleFactor = 86400;
				return new BasicDate(divide(((BasicDateTime)source).getInt(), (int)scaleFactor));
			default:
				throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
			}
		}
		else{
			long scaleFactor = 1;
			int rows = source.rows();
			int[] values = new int[rows];
			
			switch(source.getDataType()){
			case DT_NANOTIMESTAMP:
				scaleFactor = 86400000000000l;
				BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = (int)divide(ntsVec.getLong(i), scaleFactor);
				}
				return new BasicDateVector(values);
			case DT_TIMESTAMP:
				scaleFactor = 86400000;
				BasicTimestampVector tsVec = (BasicTimestampVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = (int)divide(tsVec.getLong(i), scaleFactor);
				}
				return new BasicDateVector(values);
			case DT_DATETIME:
				scaleFactor = 86400;
				BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
				for(int i=0; i<rows; ++i){
					values[i] = divide(dtVec.getInt(i), (int)scaleFactor);
				}
				return new BasicDateVector(values);
			default:
				throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
			}
		}
	}

	public static Entity toDateHour(Entity source){
		if (source.isScalar()){
			long scaleFactor = 1;
			switch (source.getDataType()){
				case DT_DATETIME:
					scaleFactor = 3600;
					return new BasicDateHour(divide(((BasicDateTime)source).getInt(), (int) scaleFactor));
				case DT_TIMESTAMP:
					scaleFactor = 3600000;
					return new BasicDateHour((int)divide(((BasicTimestamp)source).getLong(), scaleFactor));
				case DT_NANOTIMESTAMP:
					scaleFactor = 3600000000000l;
					return new BasicDateHour((int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor));
				default:
					throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
			}
		}else {
			long scaleFactor = 1;
			int rows = source.rows();
			int[] values = new int[rows];
			switch (source.getDataType()){
				case DT_DATETIME:
					scaleFactor = 3600;
					BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
					for (int i = 0; i < rows; i++){
						values[i] = divide(dtVec.getInt(i), (int)scaleFactor);
					}
					return new BasicDateHourVector(values);
				case DT_TIMESTAMP:
					scaleFactor = 3600000;
					BasicTimestampVector tsVec = (BasicTimestampVector) source;
					for (int i = 0; i < rows; i++){
						values[i] = (int) divide(tsVec.getLong(i), scaleFactor);
					}
					return new BasicDateHourVector(values);
				case DT_NANOTIMESTAMP:
					scaleFactor = 3600000000000l;
					BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector) source;
					for (int i = 0; i < rows; i++){
						values[i] = (int)divide(ntsVec.getLong(i), scaleFactor);
					}
					return new BasicDateHourVector(values);
				default:
					throw new RuntimeException("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
			}
		}
	}
	
	public static Entity castDateTime(Entity source, DATA_TYPE newDateTimeType){
		if(source.getDataForm() != DATA_FORM.DF_VECTOR && source.getDataForm() != DATA_FORM.DF_SCALAR)
			throw new RuntimeException("The source data must be a temporal scalar/vector.");
		switch(newDateTimeType){
		case DT_MONTH :
				return toMonth(source);
		case DT_DATE :
			return toDate(source);
		case DT_DATEHOUR:
			return toDateHour(source);
		default:
			throw new RuntimeException("The target date/time type supports MONTH/DATE only for time being.");
		}
	}

	public static ByteBuffer reAllocByteBuffer(ByteBuffer src, int size){
		ByteBuffer ret = ByteBuffer.allocate(size).order(src.order());
		ret.put(src.array(), 0, src.position());
		return ret;
	}

	public static boolean isLittleEndian() {
		return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
	}

	public static boolean isVariableCandidate(String word){
		char cur = word.charAt(0);
		if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z'))
			return false;
		for(int i=1;i<word.length();i++){
			cur=word.charAt(i);
			if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z') && (cur<'0' || cur>'9') && cur!='_')
				return false;
		}
		return true;
	}

	public static class Timer{
		long start = 0;
		long end = 0;
		Map<String, ArrayList<Double>> runtime = new HashMap<>();

		public void reset(){
			start = 0;
			end = 0;
			runtime = new HashMap<>();
		}

		public void printAll(){
			for (Map.Entry<String, ArrayList<Double>> entry: runtime.entrySet()){
				Double sum = 0.0;
				Double avg = 0.0;
				String prefix = entry.getKey();
				ArrayList<Double> times = entry.getValue();
				Double min = times.get(0);
				Double max = times.get(0);
				for (int i = 0; i < times.size(); i++){
					Double time = times.get(i);
					sum += time;
					if (min >= time){
						min = time;
					}
					if (max <= time){
						max = time;
					}
				}
				avg = sum / times.size();
				log.info(prefix + "avg = " + avg + " min = " + min + " max = " + max);
			}
		}

		public void recordTime(String prefix, Long start, Long end){
			this.start = start;
			this.end = end;
			ArrayList<Double> time = null;
			if (runtime.containsKey(prefix)){
				time = runtime.get(prefix);
			}else {
				time = new ArrayList<>();
			}
			time.add((end-start)/1000000.0);
			runtime.put(prefix, time);
		}
	}

	public static String getDataTypeString(DATA_TYPE dt)
	{
		switch (dt)
		{
			case DT_BOOL:
				return "bool";
			case DT_BYTE:
				return "byte";
			case DT_SHORT:
				return "short";
			case DT_INT:
				return "int";
			case DT_LONG:
				return "long";
			case DT_FLOAT:
				return "float";
			case DT_DOUBLE:
				return "double";
			case DT_NANOTIME:
				return "nanotime";
			case DT_NANOTIMESTAMP:
				return "nanotimestamp";
			case DT_TIMESTAMP:
				return "timestamp";
			case DT_DATE:
				return "date";
			case DT_MONTH:
				return "month";
			case DT_TIME:
				return "time";
			case DT_SECOND:
				return "second";
			case DT_MINUTE:
				return "minute";
			case DT_DATETIME:
				return "datetime";
			case DT_INT128:
				return "int128";
			case DT_IPADDR:
				return "ipaddr";
			case DT_UUID:
				return "uuid";
			case DT_STRING:
				return "string";
			case DT_SYMBOL:
				return "symbol";
			case DT_DECIMAL32:
				return "decimal32";
			case DT_DECIMAL64:
				return "decimal64";
			default:
				return dt.toString();
		}
	}

	public static boolean isEmpty(CharSequence cs) {
		return cs == null || cs.length() == 0;
	}

	public static boolean isNotEmpty(CharSequence cs) {
		return cs != null && cs.length() > 0;
	}

	private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");
	private static final BigDecimal DECIMAL128_MAX_VALUE = new BigDecimal("170141183460469231731687303715884105728");

	public static void checkDecimal128Range(BigDecimal value, int scale) {
		if (Objects.isNull(value))
			throw new RuntimeException("Decimal value cannot be null.");

		if (value.scaleByPowerOfTen(scale).compareTo(DECIMAL128_MIN_VALUE) <0 || value.scaleByPowerOfTen(scale).compareTo(DECIMAL128_MAX_VALUE) > 0) {
			if (scale == 0)
				throw new RuntimeException("Decimal128 overflow " + value.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP).toBigInteger());
			else
				throw new RuntimeException("Decimal128 overflow " + new BigDecimal(value.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP).toBigInteger()).scaleByPowerOfTen(-scale));
		}
	}

	public static Vector createVector(DATA_TYPE type, int size, int capacity) {
		return createVector(type, size, capacity, -1);
	}

	public static Vector createVector(DATA_TYPE type, int size, int capacity, int scale) {
		if (type.getValue() >=65 || type == DT_FUNCTIONDEF || type == DT_HANDLE || type == DT_CODE || type == DT_DATASOURCE
				|| type == DT_RESOURCE || type == DT_COMPRESS || type == DT_DICTIONARY || type == DT_OBJECT || type == DT_INSTRUMENT || type == DT_MKTDATA) {
			throw new IllegalArgumentException("Cannot create vector for type '" + type + "'.");
		}

		return BasicEntityFactory.instance().createVector(type, size, capacity, scale);
	}

	static int[] createDefaultExtraParams(final int size) {
		int[] extraParams = new int[size];
		Arrays.fill(extraParams, -1);
		return extraParams;
	}

	static void validateColumnNames(final List<String> colNames) {
		if (Objects.isNull(colNames))
			throw new RuntimeException("The param 'colNames' in table cannot be null.");

		HashSet<String> colNameSet = new HashSet<String>();
		for (String name : colNames) {
			if (Utils.isEmpty(name))
				throw new RuntimeException("The column name in table cannot be null or empty.");
			if (!colNameSet.add(name))
				throw new RuntimeException("The table contains duplicated column name '" + name + "'.");
		}
	}

	static Vector inferAndConvertJavaColumn(final String colName, final List<?> values) {
		DATA_TYPE colType = inferJavaColumnType(colName, values);
		Vector temporalVector = createAutoInferenceTemporalVector(colType, values);
		if (temporalVector != null) {
			return temporalVector;
		}
		return convertJavaColumn(colName, colType, -1, values);
	}

	static Vector inferAndConvertJavaColumn(final String colName, final Object[] values) {
		DATA_TYPE colType = inferJavaColumnType(colName, values);
		Vector temporalVector = createAutoInferenceTemporalVector(colType, values);
		if (temporalVector != null) {
			return temporalVector;
		}
		return convertJavaColumn(colName, colType, -1, normalizeJavaArrayColumnValues(values));
	}

	static List<Vector> inferAndConvertJavaColumns(final List<String> colNames, final Collection<?> cols) {
		if (Objects.isNull(colNames))
			throw new RuntimeException("The param 'colNames' in table cannot be null.");
		if (Objects.isNull(cols))
			throw new RuntimeException("The param 'cols' in table cannot be null.");
		if (!(cols instanceof List<?>)) {
			throw new IllegalArgumentException("The auto-inference BasicTable constructor only supports List input for Java columns.");
		}
		if (colNames.size() != cols.size()) {
			throw new Error("The length of column name and column data is unequal.");
		}

		List<Vector> vectors = new ArrayList<Vector>(cols.size());
		int index = 0;
		for (Object col : cols) {
			vectors.add(inferAndConvertJavaConstructorColumn(colNames.get(index), col));
			index++;
		}
		return vectors;
	}

	static List<Vector> inferAndConvertJavaColumns(final List<String> colNames, final Object[] cols) {
		if (Objects.isNull(colNames))
			throw new RuntimeException("The param 'colNames' in table cannot be null.");
		if (Objects.isNull(cols))
			throw new RuntimeException("The param 'cols' in table cannot be null.");
		return inferAndConvertJavaColumns(colNames, Arrays.asList(cols));
	}

	static List<Vector> convertColumns(final List<String> colNames, final List<?> cols, final DATA_TYPE[] colTypes, final int[] colExtraParams) {
		if (Objects.isNull(colNames))
			throw new RuntimeException("The param 'colNames' in table cannot be null.");
		if (Objects.isNull(cols))
			throw new RuntimeException("The param 'cols' in table cannot be null.");
		if (colNames.size() != cols.size()) {
			throw new Error("The length of column name and column data is unequal.");
		}

		boolean hasVectorColumn = false;
		boolean hasJavaColumn = false;
		for (int i = 0; i < cols.size(); ++i) {
			Object col = cols.get(i);
			if (col == null) {
				if (colTypes == null) {
					throw new IllegalArgumentException("Column [" + colNames.get(i) + "] is null.");
				}
				hasJavaColumn = true;
				if (hasVectorColumn) {
					throw new IllegalArgumentException("The new BasicTable constructors only support all-Java columns or all-Vector columns.");
				}
				continue;
			}
			if (col instanceof Vector) {
				hasVectorColumn = true;
			} else if (col instanceof Entity) {
				throw new IllegalArgumentException("Column [" + colNames.get(i) + "] is a DolphinDB Entity but not a Vector. The new BasicTable constructors only support all-Java columns or all-Vector columns.");
			} else {
				hasJavaColumn = true;
			}
			if (hasVectorColumn && hasJavaColumn) {
				throw new IllegalArgumentException("The new BasicTable constructors only support all-Java columns or all-Vector columns.");
			}
		}

		if (hasVectorColumn) {
			return validateVectorColumns(colNames, cols, colTypes, colExtraParams);
		}
		return convertJavaColumns(colNames, cols, colTypes, colExtraParams);
	}

	private static Vector inferAndConvertJavaConstructorColumn(final String colName, final Object col) {
		if (col == null) {
			throw new IllegalArgumentException("Column [" + colName + "] is null.");
		}
		if (col instanceof Vector) {
			throw new IllegalArgumentException("Column [" + colName + "] is a DolphinDB Vector. Please use BasicTable(List<String>, List<Vector>) or the typed constructor.");
		}
		if (col instanceof List<?>) {
			DATA_TYPE colType = inferJavaConstructorColumnType(colName, (List<?>) col);
			Vector temporalVector = createAutoInferenceTemporalVector(colType, (List<?>) col);
			if (temporalVector != null) {
				return temporalVector;
			}
			return convertJavaColumn(colName, colType, -1, col);
		}
		if (col instanceof Object[]) {
			DATA_TYPE colType = inferJavaConstructorColumnType(colName, (Object[]) col);
			Vector temporalVector = createAutoInferenceTemporalVector(colType, (Object[]) col);
			if (temporalVector != null) {
				return temporalVector;
			}
			return convertJavaColumn(colName, colType, -1, normalizeJavaArrayColumnValues((Object[]) col));
		}
		DATA_TYPE primitiveArrayType = inferJavaConstructorTypeFromPrimitiveArray(col);
		if (primitiveArrayType != null) {
			return convertJavaColumn(colName, primitiveArrayType, -1, col);
		}
		if (isPrimitiveArrayColumn(col)) {
			throw unsupportedJavaConstructorInference(colName, col.getClass());
		}
		if (col instanceof Entity) {
			throw new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for DolphinDB Entity values. Please use the typed constructor.");
		}
		throw new IllegalArgumentException("Column [" + colName + "] only supports Java List or array values for automatic type inference. Please use the typed constructor.");
	}

	private static List<Vector> validateVectorColumns(final List<String> colNames, final List<?> cols, final DATA_TYPE[] colTypes, final int[] colExtraParams) {
		if (colTypes != null && colNames.size() != colTypes.length) {
			throw new Error("The lengths of column names, column data, and column types must be equal.");
		}
		if (colExtraParams != null && colNames.size() != colExtraParams.length) {
			throw new Error("The lengths of column names, column data, and column extra params must be equal.");
		}

		List<Vector> vectors = new ArrayList<Vector>(cols.size());
		for (int i = 0; i < cols.size(); ++i) {
			DATA_TYPE colType = colTypes == null ? null : colTypes[i];
			if (colType != null) {
				validateBasicTableConstructorColumnType(colType);
			}
			int extraParam = colExtraParams == null ? -1 : colExtraParams[i];
			vectors.add(validateVectorType(colNames.get(i), colType, extraParam, (Vector) cols.get(i)));
		}
		return vectors;
	}

	private static List<Vector> convertJavaColumns(final List<String> colNames, final List<?> cols, final DATA_TYPE[] colTypes, final int[] colExtraParams) {
		if (colTypes == null) {
			throw new IllegalArgumentException("Column types must be specified when using Java-native columns.");
		}
		if (colNames.size() != colTypes.length) {
			throw new Error("The lengths of column names, column data, and column types must be equal.");
		}
		int[] resolvedExtraParams = colExtraParams;
		if (resolvedExtraParams == null) {
			resolvedExtraParams = createDefaultExtraParams(colTypes.length);
		} else if (colNames.size() != resolvedExtraParams.length) {
			throw new Error("The lengths of column names, column data, column types, and column extra params must be equal.");
		}

		List<Vector> vectors = new ArrayList<Vector>(cols.size());
		for (int i = 0; i < cols.size(); ++i) {
			DATA_TYPE colType = colTypes[i];
			if (colType == null) {
				throw new IllegalArgumentException("Column [" + colNames.get(i) + "] type must be specified when using Java-native columns.");
			}
			validateBasicTableConstructorColumnType(colType);
			vectors.add(convertJavaColumn(colNames.get(i), colType, resolvedExtraParams[i], cols.get(i)));
		}
		return vectors;
	}

	private static DATA_TYPE inferJavaConstructorColumnType(final String colName, final List<?> values) {
		Object sample = findFirstNonNullValue(values);
		if (sample == null) {
			throw new IllegalArgumentException("Column [" + colName + "] type cannot be inferred from an empty or all-null List. Please use the typed constructor.");
		}

		DATA_TYPE inferredType = inferJavaConstructorTypeFromValue(colName, sample);
		validateInferredJavaConstructorValues(colName, inferredType, values);
		return inferredType;
	}

	private static DATA_TYPE inferJavaConstructorColumnType(final String colName, final Object[] values) {
		Class<?> componentType = values.getClass().getComponentType();
		DATA_TYPE inferredType = null;
		if (componentType != null && componentType != Object.class) {
			inferredType = inferJavaColumnTypeFromClass(componentType);
			if (inferredType == null) {
				Object sample = getSampleValue(values);
				if (sample == null) {
					throw unsupportedJavaConstructorInference(colName, componentType);
				}
			}
		}

		if (inferredType == null) {
			Object sample = getSampleValue(values);
			if (sample == null) {
				throw new IllegalArgumentException("Column [" + colName + "] type cannot be inferred from an empty or all-null Object[] column. Please use the typed constructor.");
			}
			inferredType = inferJavaConstructorTypeFromValue(colName, sample);
		}

		validateInferredJavaConstructorValues(colName, inferredType, Arrays.asList(values));
		return inferredType;
	}

	private static DATA_TYPE inferJavaColumnType(final String colName, final List<?> values) {
		Object sample = findFirstNonNullValue(values);
		if (sample == null) {
			throw new IllegalArgumentException("Column [" + colName + "] type cannot be inferred from an empty or all-null List. Please use addColumn(String, Vector).");
		}

		DATA_TYPE inferredType = inferJavaColumnTypeFromValue(colName, sample);
		validateInferredJavaColumnValues(colName, inferredType, values);
		return inferredType;
	}

	private static DATA_TYPE inferJavaColumnType(final String colName, final Object[] values) {
		Class<?> componentType = values.getClass().getComponentType();
		DATA_TYPE inferredType = null;
		if (componentType != null && componentType != Object.class) {
			inferredType = inferJavaColumnTypeFromClass(componentType);
			if (inferredType == null) {
				Object sample = getSampleValue(values);
				if (sample == null) {
					throw unsupportedJavaInference(colName, componentType);
				}
			}
		}

		if (inferredType == null) {
			Object sample = getSampleValue(values);
			if (sample == null) {
				throw new IllegalArgumentException("Column [" + colName + "] type cannot be inferred from an empty or all-null Object[] column. Please use addColumn(String, Vector).");
			}
			inferredType = inferJavaColumnTypeFromValue(colName, sample);
		}

		validateInferredJavaColumnValues(colName, inferredType, Arrays.asList(values));
		return inferredType;
	}

	private static DATA_TYPE inferJavaConstructorTypeFromValue(final String colName, final Object value) {
		DATA_TYPE inferredType = inferJavaColumnTypeFromClass(value.getClass());
		if (inferredType != null) {
			return inferredType;
		}
		if (value instanceof Vector) {
			throw new IllegalArgumentException("Column [" + colName + "] is a DolphinDB Vector. Please use BasicTable(List<String>, List<Vector>) or the typed constructor.");
		}
		if (value instanceof Entity) {
			throw new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for DolphinDB Entity values. Please use the typed constructor.");
		}
		throw unsupportedJavaConstructorInference(colName, value.getClass());
	}

	private static DATA_TYPE inferJavaColumnTypeFromValue(final String colName, final Object value) {
		DATA_TYPE inferredType = inferJavaColumnTypeFromClass(value.getClass());
		if (inferredType != null) {
			return inferredType;
		}
		if (value instanceof Vector) {
			throw new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for DolphinDB Vector values. Please use addColumn(String, Vector).");
		}
		if (value instanceof Entity) {
			throw new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for DolphinDB Entity values. Please use addColumn(String, Vector).");
		}
		throw unsupportedJavaInference(colName, value.getClass());
	}

	private static DATA_TYPE inferJavaColumnTypeFromClass(final Class<?> valueClass) {
		if (valueClass == Boolean.class) {
			return DATA_TYPE.DT_BOOL;
		}
		if (valueClass == Byte.class) {
			return DATA_TYPE.DT_BYTE;
		}
		if (valueClass == Short.class) {
			return DATA_TYPE.DT_SHORT;
		}
		if (valueClass == Integer.class) {
			return DATA_TYPE.DT_INT;
		}
		if (valueClass == Long.class) {
			return DATA_TYPE.DT_LONG;
		}
		if (valueClass == Float.class) {
			return DATA_TYPE.DT_FLOAT;
		}
		if (valueClass == Double.class) {
			return DATA_TYPE.DT_DOUBLE;
		}
		if (valueClass == String.class) {
			return DATA_TYPE.DT_STRING;
		}
		if (valueClass == byte[].class) {
			return DATA_TYPE.DT_BLOB;
		}
		if (YearMonth.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_MONTH;
		}
		if (LocalDate.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_DATE;
		}
		if (LocalTime.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_NANOTIME;
		}
		if (LocalDateTime.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_NANOTIMESTAMP;
		}
		if (Calendar.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_TIMESTAMP;
		}
		if (Date.class.isAssignableFrom(valueClass)) {
			return DATA_TYPE.DT_TIMESTAMP;
		}
		return null;
	}

	private static DATA_TYPE inferJavaConstructorTypeFromPrimitiveArray(final Object value) {
		if (value instanceof boolean[]) {
			return DATA_TYPE.DT_BOOL;
		}
		if (value instanceof byte[]) {
			return DATA_TYPE.DT_BYTE;
		}
		if (value instanceof short[]) {
			return DATA_TYPE.DT_SHORT;
		}
		if (value instanceof int[]) {
			return DATA_TYPE.DT_INT;
		}
		if (value instanceof long[]) {
			return DATA_TYPE.DT_LONG;
		}
		if (value instanceof float[]) {
			return DATA_TYPE.DT_FLOAT;
		}
		if (value instanceof double[]) {
			return DATA_TYPE.DT_DOUBLE;
		}
		return null;
	}

	private static void validateInferredJavaConstructorValues(final String colName, final DATA_TYPE inferredType, final Collection<?> values) {
		for (Object value : values) {
			if (value == null) {
				continue;
			}
			DATA_TYPE currentType = inferJavaConstructorTypeFromValue(colName, value);
			if (currentType != inferredType) {
				throw new IllegalArgumentException("Column [" + colName + "] contains values with inconsistent Java types for automatic inference.");
			}
		}
	}

	private static void validateInferredJavaColumnValues(final String colName, final DATA_TYPE inferredType, final Collection<?> values) {
		for (Object value : values) {
			if (value == null) {
				continue;
			}
			DATA_TYPE currentType = inferJavaColumnTypeFromValue(colName, value);
			if (currentType != inferredType) {
				throw new IllegalArgumentException("Column [" + colName + "] contains values with inconsistent Java types for automatic inference.");
			}
		}
	}

	private static Object normalizeJavaArrayColumnValues(final Object[] values) {
		Class<?> componentType = values.getClass().getComponentType();
		if (componentType == Boolean.class
				|| componentType == Byte.class
				|| componentType == Short.class
				|| componentType == Integer.class
				|| componentType == Long.class
				|| componentType == Float.class
				|| componentType == Double.class) {
			return Arrays.asList(values);
		}
		return values;
	}

	private static IllegalArgumentException unsupportedJavaInference(final String colName, final Class<?> valueClass) {
		return new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for Java type " + valueClass.getName() + ". Please use addColumn(String, Vector).");
	}

	private static IllegalArgumentException unsupportedJavaConstructorInference(final String colName, final Class<?> valueClass) {
		return new IllegalArgumentException("Column [" + colName + "] does not support automatic type inference for Java type " + valueClass.getName() + ". Please use the typed constructor.");
	}

	private static void validateBasicTableConstructorColumnType(final DATA_TYPE colType) {
		if (colType == DATA_TYPE.DT_FUNCTIONDEF || colType == DATA_TYPE.DT_HANDLE || colType == DATA_TYPE.DT_CODE
				|| colType == DATA_TYPE.DT_DATASOURCE || colType == DATA_TYPE.DT_RESOURCE || colType == DATA_TYPE.DT_COMPRESS
				|| colType == DATA_TYPE.DT_DICTIONARY || colType == DATA_TYPE.DT_DECIMAL || colType == DATA_TYPE.DT_OBJECT
				|| colType == DATA_TYPE.DT_ANY || colType == DATA_TYPE.DT_IOTANY || colType == DATA_TYPE.DT_INSTRUMENT
				|| colType == DATA_TYPE.DT_MKTDATA) {
			throw new RuntimeException("Column type " + colType + " is not supported for BasicTable constructor.");
		}
	}

	private static Vector createAutoInferenceTemporalVector(final DATA_TYPE colType, final List<?> values) {
		if (colType != DATA_TYPE.DT_TIMESTAMP) {
			return null;
		}
		Object sample = findFirstNonNullValue(values);
		if (sample instanceof Date) {
			return new BasicTimestampVector(timestampMillisFromDates(values), false);
		}
		if (sample instanceof Calendar) {
			return new BasicTimestampVector(timestampMillisFromCalendars(values), false);
		}
		return null;
	}

	private static Vector createAutoInferenceTemporalVector(final DATA_TYPE colType, final Object[] values) {
		if (colType != DATA_TYPE.DT_TIMESTAMP) {
			return null;
		}
		Class<?> componentType = values.getClass().getComponentType();
		if (componentType != null && Date.class.isAssignableFrom(componentType)) {
			return new BasicTimestampVector(timestampMillisFromDates(values), false);
		}
		if (componentType != null && Calendar.class.isAssignableFrom(componentType)) {
			return new BasicTimestampVector(timestampMillisFromCalendars(values), false);
		}

		Object sample = getSampleValue(values);
		if (sample instanceof Date) {
			return new BasicTimestampVector(timestampMillisFromDates(values), false);
		}
		if (sample instanceof Calendar) {
			return new BasicTimestampVector(timestampMillisFromCalendars(values), false);
		}
		return null;
	}

	private static long[] timestampMillisFromDates(final List<?> values) {
		long[] data = new long[values.size()];
		for (int i = 0; i < values.size(); ++i) {
			Object value = values.get(i);
			data[i] = value == null ? Long.MIN_VALUE : ((Date) value).getTime();
		}
		return data;
	}

	private static long[] timestampMillisFromDates(final Object[] values) {
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			Object value = values[i];
			data[i] = value == null ? Long.MIN_VALUE : ((Date) value).getTime();
		}
		return data;
	}

	private static long[] timestampMillisFromCalendars(final List<?> values) {
		long[] data = new long[values.size()];
		for (int i = 0; i < values.size(); ++i) {
			Object value = values.get(i);
			data[i] = value == null ? Long.MIN_VALUE : ((Calendar) value).getTimeInMillis();
		}
		return data;
	}

	private static long[] timestampMillisFromCalendars(final Object[] values) {
		long[] data = new long[values.length];
		for (int i = 0; i < values.length; ++i) {
			Object value = values[i];
			data[i] = value == null ? Long.MIN_VALUE : ((Calendar) value).getTimeInMillis();
		}
		return data;
	}

	private static Vector convertJavaColumn(final String colName, final DATA_TYPE colType, final int extraParam, final Object col) {
		Vector fastVector = tryCreateFastVector(colType, extraParam, col);
		if (fastVector != null) {
			return fastVector;
		}
		if (isArrayDataType(colType)) {
			if (col instanceof Collection<?>) {
				return convertCollectionColumn(colName, colType, extraParam, (Collection<?>) col);
			}
			if (col instanceof Object[]) {
				return convertCollectionColumn(colName, colType, extraParam, Arrays.asList((Object[]) col));
			}
			return convertScalarColumn(colName, colType, extraParam, col);
		}
		if (col instanceof Collection<?>) {
			return convertCollectionColumn(colName, colType, extraParam, (Collection<?>) col);
		}
		if (col instanceof Object[]) {
			return convertCollectionColumn(colName, colType, extraParam, Arrays.asList((Object[]) col));
		}
		if (isPrimitiveArrayColumn(col)) {
			return convertPrimitiveArrayColumn(colName, colType, extraParam, col);
		}
		return convertScalarColumn(colName, colType, extraParam, col);
	}

	private static Vector convertPrimitiveArrayColumn(final String colName, final DATA_TYPE colType, final int extraParam, final Object values) {
		if (colType == DATA_TYPE.DT_BLOB && values instanceof byte[]) {
			return convertScalarColumn(colName, colType, extraParam, values);
		}
		try {
			Entity vector = BasicEntityFactory.createScalar(toColumnArrayType(colType), values, extraParam);
			if (!(vector instanceof Vector)) {
				throw new IllegalArgumentException("Column [" + colName + "] could not be converted to a vector.");
			}
			return validateVectorType(colName, colType, extraParam, (Vector) vector);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to convert column [" + colName + "] to " + colType.getName() + ": " + ex.getMessage(), ex);
		}
	}

	private static Vector convertCollectionColumn(final String colName, final DATA_TYPE colType, final int extraParam, final Collection<?> values) {
		int resolvedExtraParam = resolveExtraParam(colType, extraParam, findFirstNonNullValue(values));
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(colType, values.size(), resolvedExtraParam);
		int rowIndex = 0;
		for (Object value : values) {
			setColumnValue(colName, vector, colType, resolvedExtraParam, rowIndex, value);
			rowIndex++;
		}
		return vector;
	}

	private static Vector convertScalarColumn(final String colName, final DATA_TYPE colType, final int extraParam, final Object value) {
		int resolvedExtraParam = resolveExtraParam(colType, extraParam, value);
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(colType, 1, resolvedExtraParam);
		setColumnValue(colName, vector, colType, resolvedExtraParam, 0, value);
		return vector;
	}

	private static void setColumnValue(final String colName, final Vector vector, final DATA_TYPE colType, final int extraParam, final int rowIndex, final Object value) {
		try {
			vector.set(rowIndex, toColumnEntity(colType, extraParam, value));
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to convert row " + rowIndex + " of column [" + colName + "] to " + colType.getName() + ": " + ex.getMessage(), ex);
		}
	}

	private static Entity toColumnEntity(final DATA_TYPE colType, final int extraParam, final Object value) throws Exception {
		if (value == null) {
			return null;
		}
		if (colType == DATA_TYPE.DT_MONTH && value instanceof YearMonth) {
			return new BasicMonth((YearMonth) value);
		}
		if (colType == DATA_TYPE.DT_BLOB && value instanceof byte[]) {
			return new BasicString((byte[]) value, true);
		}
		if (isArrayDataType(colType)) {
			if (value instanceof Entity) {
				Entity entity = (Entity) value;
				if (!entity.isVector()) {
					throw new IllegalArgumentException("Array-typed column values must be vectors.");
				}
				return entity;
			}
			return BasicEntityFactory.createScalar(colType, value, extraParam);
		}
		if (value instanceof Entity && !((Entity) value).isScalar()) {
			throw new IllegalArgumentException("Only scalar Entity values are supported in Java columns.");
		}
		return BasicEntityFactory.createScalar(colType, value, extraParam);
	}

	private static Vector validateVectorType(final String colName, final DATA_TYPE colType, final int extraParam, final Vector vector) {
		DATA_TYPE resolvedType = vector.getDataType();
		if (colType != null && resolvedType != colType) {
			throw new IllegalArgumentException("Column [" + colName + "] expected type " + colType.getName() + ", but got " + vector.getDataType().getName() + ".");
		}
		if (colType != null) {
			resolvedType = colType;
		}
		if (extraParam != -1 && isDecimalType(resolvedType)) {
			validateDecimalScale(resolvedType, extraParam);
		}
		if (extraParam >= 0 && isDecimalType(resolvedType) && vector instanceof AbstractVector) {
			int vectorExtraParam = ((AbstractVector) vector).getExtraParamForType();
			if (vectorExtraParam != extraParam) {
				throw new IllegalArgumentException("Column [" + colName + "] expected extra param " + extraParam + ", but got " + vectorExtraParam + ".");
			}
		}
		return vector;
	}

	private static Vector tryCreateFastVector(final DATA_TYPE colType, final int declaredExtraParam, final Object col) {
		int extraParam = resolveExtraParam(colType, declaredExtraParam, getSampleValue(col));
		if (col instanceof boolean[]) {
			if (colType == DATA_TYPE.DT_BOOL) {
				return new BasicBooleanVector((boolean[]) col);
			}
			return null;
		}
		if (col instanceof byte[]) {
			switch (colType) {
				case DT_BOOL:
					return new BasicBooleanVector((byte[]) col);
				case DT_BYTE:
					return new BasicByteVector((byte[]) col);
				default:
					return null;
			}
		}
		if (col instanceof short[]) {
			if (colType == DATA_TYPE.DT_SHORT) {
				return new BasicShortVector((short[]) col);
			}
			return null;
		}
		if (col instanceof int[]) {
			switch (colType) {
				case DT_INT:
					return new BasicIntVector((int[]) col);
				case DT_DATE:
					return new BasicDateVector((int[]) col);
				case DT_MONTH:
					return new BasicMonthVector((int[]) col);
				case DT_TIME:
					return new BasicTimeVector((int[]) col);
				case DT_MINUTE:
					return new BasicMinuteVector((int[]) col);
				case DT_SECOND:
					return new BasicSecondVector((int[]) col);
				case DT_DATETIME:
					return new BasicDateTimeVector((int[]) col);
				case DT_DATEHOUR:
					return new BasicDateHourVector((int[]) col);
				case DT_DECIMAL32:
					return new BasicDecimal32Vector((int[]) col, extraParam);
				default:
					return null;
			}
		}
		if (col instanceof long[]) {
			switch (colType) {
				case DT_LONG:
					return new BasicLongVector((long[]) col);
				case DT_NANOTIME:
					return new BasicNanoTimeVector((long[]) col);
				case DT_TIMESTAMP:
					return new BasicTimestampVector((long[]) col);
				case DT_NANOTIMESTAMP:
					return new BasicNanoTimestampVector((long[]) col);
				case DT_DECIMAL64:
					return new BasicDecimal64Vector((long[]) col, extraParam);
				default:
					return null;
			}
		}
		if (col instanceof float[]) {
			if (colType == DATA_TYPE.DT_FLOAT) {
				return new BasicFloatVector((float[]) col);
			}
			return null;
		}
		if (col instanceof double[]) {
			switch (colType) {
				case DT_DOUBLE:
					return new BasicDoubleVector((double[]) col);
				case DT_DECIMAL32:
					return new BasicDecimal32Vector((double[]) col, extraParam);
				case DT_DECIMAL64:
					return new BasicDecimal64Vector((double[]) col, extraParam);
				default:
					return null;
			}
		}
		if (col instanceof String[]) {
			switch (colType) {
				case DT_STRING:
					return new BasicStringVector((String[]) col);
				case DT_SYMBOL:
					return new BasicSymbolVector(Arrays.asList((String[]) col));
				case DT_DECIMAL32:
					return new BasicDecimal32Vector((String[]) col, extraParam);
				case DT_DECIMAL64:
					return new BasicDecimal64Vector((String[]) col, extraParam);
				case DT_DECIMAL128:
					return new BasicDecimal128Vector((String[]) col, extraParam);
				default:
					return null;
			}
		}
		if (col instanceof LocalDate[]) {
			if (colType == DATA_TYPE.DT_DATE) {
				return new BasicDateVector((LocalDate[]) col);
			}
			return null;
		}
		if (col instanceof YearMonth[]) {
			if (colType == DATA_TYPE.DT_MONTH) {
				return new BasicMonthVector((YearMonth[]) col);
			}
			return null;
		}
		if (col instanceof LocalTime[]) {
			switch (colType) {
				case DT_TIME:
					return new BasicTimeVector((LocalTime[]) col);
				case DT_SECOND:
					return new BasicSecondVector((LocalTime[]) col);
				case DT_MINUTE:
					return new BasicMinuteVector((LocalTime[]) col);
				case DT_NANOTIME:
					return new BasicNanoTimeVector((LocalTime[]) col);
				default:
					return null;
			}
		}
		if (col instanceof LocalDateTime[]) {
			switch (colType) {
				case DT_DATETIME:
					return new BasicDateTimeVector((LocalDateTime[]) col);
				case DT_DATEHOUR:
					return new BasicDateHourVector((LocalDateTime[]) col);
				case DT_TIMESTAMP:
					return new BasicTimestampVector((LocalDateTime[]) col);
				case DT_NANOTIME:
					return new BasicNanoTimeVector((LocalDateTime[]) col);
				case DT_NANOTIMESTAMP:
					return new BasicNanoTimestampVector((LocalDateTime[]) col);
				default:
					return null;
			}
		}
		if (col instanceof Calendar[]) {
			switch (colType) {
				case DT_DATE:
					return new BasicDateVector((Calendar[]) col);
				case DT_MONTH:
					return new BasicMonthVector((Calendar[]) col);
				case DT_TIME:
					return new BasicTimeVector((Calendar[]) col);
				case DT_SECOND:
					return new BasicSecondVector((Calendar[]) col);
				case DT_MINUTE:
					return new BasicMinuteVector((Calendar[]) col);
				case DT_DATETIME:
					return new BasicDateTimeVector((Calendar[]) col);
				case DT_DATEHOUR:
					return new BasicDateHourVector((Calendar[]) col);
				case DT_TIMESTAMP:
					return new BasicTimestampVector((Calendar[]) col);
				default:
					return null;
			}
		}
		if (col instanceof byte[][]) {
			if (colType == DATA_TYPE.DT_BLOB) {
				return new BasicStringVector((byte[][]) col);
			}
			return null;
		}
		if (col instanceof Long2[]) {
			switch (colType) {
				case DT_INT128:
					return new BasicInt128Vector((Long2[]) col);
				case DT_UUID:
					return new BasicUuidVector((Long2[]) col);
				case DT_IPADDR:
					return new BasicIPAddrVector((Long2[]) col);
				default:
					return null;
			}
		}
		if (col instanceof Double2[]) {
			switch (colType) {
				case DT_POINT:
					return new BasicPointVector((Double2[]) col);
				case DT_COMPLEX:
					return new BasicComplexVector((Double2[]) col);
				default:
					return null;
			}
		}
		if (col instanceof BigInteger[]) {
			if (colType == DATA_TYPE.DT_DECIMAL128) {
				return new BasicDecimal128Vector((BigInteger[]) col, extraParam);
			}
			return null;
		}
		if (col instanceof List<?>) {
			return tryCreateFastVectorFromList(colType, extraParam, (List<?>) col);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Vector tryCreateFastVectorFromList(final DATA_TYPE colType, final int extraParam, final List<?> values) {
		Object sample = findFirstNonNullValue(values);
		if (sample == null) {
			return null;
		}
		if (isArrayDataType(colType) && sample instanceof Vector) {
			DATA_TYPE inferredType = DATA_TYPE.valueOf(((Vector) sample).getDataType().getValue() + 64);
			if (inferredType != colType) {
				throw new IllegalArgumentException("Array column expected type " + colType.getName() + ", but got " + inferredType.getName() + ".");
			}
			if (extraParam >= 0 && isDecimalType(colType) && sample instanceof AbstractVector
					&& ((AbstractVector) sample).getExtraParamForType() != extraParam) {
				throw new IllegalArgumentException("Array column expected extra param " + extraParam + ", but got " + ((AbstractVector) sample).getExtraParamForType() + ".");
			}
			try {
				return new BasicArrayVector((List<Vector>) values);
			} catch (Exception ex) {
				throw new IllegalArgumentException("Failed to convert array column: " + ex.getMessage(), ex);
			}
		}
		switch (colType) {
			case DT_BOOL:
				if (sample instanceof Byte) {
					return new BasicBooleanVector((List<Byte>) values);
				}
				if (sample instanceof Boolean) {
					return new BasicBooleanVector(booleanListValues((List<Boolean>) values));
				}
				return null;
			case DT_BYTE:
				return sample instanceof Byte ? new BasicByteVector((List<Byte>) values) : null;
			case DT_SHORT:
				return sample instanceof Short ? new BasicShortVector((List<Short>) values) : null;
			case DT_INT:
				return sample instanceof Integer ? new BasicIntVector((List<Integer>) values) : null;
			case DT_LONG:
				return sample instanceof Long ? new BasicLongVector((List<Long>) values) : null;
			case DT_FLOAT:
				return sample instanceof Float ? new BasicFloatVector((List<Float>) values) : null;
			case DT_DOUBLE:
				return sample instanceof Double ? new BasicDoubleVector((List<Double>) values) : null;
			case DT_DATE:
				return sample instanceof Integer ? new BasicDateVector((List<Integer>) values) : null;
			case DT_MONTH:
				return sample instanceof Integer ? new BasicMonthVector((List<Integer>) values) : null;
			case DT_TIME:
				return sample instanceof Integer ? new BasicTimeVector((List<Integer>) values) : null;
			case DT_MINUTE:
				return sample instanceof Integer ? new BasicMinuteVector((List<Integer>) values) : null;
			case DT_SECOND:
				return sample instanceof Integer ? new BasicSecondVector((List<Integer>) values) : null;
			case DT_DATETIME:
				return sample instanceof Integer ? new BasicDateTimeVector((List<Integer>) values) : null;
			case DT_DATEHOUR:
				return sample instanceof Integer ? new BasicDateHourVector((List<Integer>) values) : null;
			case DT_NANOTIME:
				return sample instanceof Long ? new BasicNanoTimeVector((List<Long>) values) : null;
			case DT_TIMESTAMP:
				return sample instanceof Long ? new BasicTimestampVector((List<Long>) values) : null;
			case DT_NANOTIMESTAMP:
				return sample instanceof Long ? new BasicNanoTimestampVector((List<Long>) values) : null;
			case DT_STRING:
				return sample instanceof String ? new BasicStringVector((List<String>) values) : null;
			case DT_SYMBOL:
				return sample instanceof String ? new BasicSymbolVector((List<String>) values) : null;
			case DT_INT128:
				return sample instanceof Long2 ? new BasicInt128Vector((List<Long2>) values) : null;
			case DT_UUID:
				return sample instanceof Long2 ? new BasicUuidVector((List<Long2>) values) : null;
			case DT_IPADDR:
				return sample instanceof Long2 ? new BasicIPAddrVector((List<Long2>) values) : null;
			case DT_POINT:
				return sample instanceof Double2 ? new BasicPointVector((List<Double2>) values) : null;
			case DT_COMPLEX:
				return sample instanceof Double2 ? new BasicComplexVector((List<Double2>) values) : null;
			case DT_DECIMAL32:
				return sample instanceof String ? new BasicDecimal32Vector((List<String>) values, extraParam) : null;
			case DT_DECIMAL64:
				return sample instanceof String ? new BasicDecimal64Vector((List<String>) values, extraParam) : null;
			case DT_DECIMAL128:
				return sample instanceof String ? new BasicDecimal128Vector((List<String>) values, extraParam) : null;
			default:
				return null;
		}
	}

	private static int resolveExtraParam(final DATA_TYPE colType, final int declaredExtraParam, final Object sampleValue) {
		if (isDecimalType(colType) && declaredExtraParam != -1) {
			validateDecimalScale(colType, declaredExtraParam);
			return declaredExtraParam;
		}
		if (declaredExtraParam >= 0) {
			return declaredExtraParam;
		}
		if (sampleValue instanceof Scalar) {
			return ((Scalar) sampleValue).getScale();
		}
		if (isDecimalType(colType)) {
			throw new IllegalArgumentException("Column type " + colType.getName() + " requires extra parameters such as scale.");
		}
		return -1;
	}

	private static void validateDecimalScale(final DATA_TYPE colType, final int scale) {
		int upperBound = getDecimalScaleUpperBound(colType);
		if (scale < 0 || scale > upperBound) {
			throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0," + upperBound + "].");
		}
	}

	private static int getDecimalScaleUpperBound(final DATA_TYPE colType) {
		switch (colType) {
			case DT_DECIMAL32:
			case DT_DECIMAL32_ARRAY:
				return 9;
			case DT_DECIMAL64:
			case DT_DECIMAL64_ARRAY:
				return 18;
			case DT_DECIMAL128:
			case DT_DECIMAL128_ARRAY:
				return 38;
			default:
				throw new IllegalArgumentException("Column type " + colType.getName() + " is not a decimal type.");
		}
	}

	private static Object getSampleValue(final Object col) {
		if (col instanceof Collection<?>) {
			return findFirstNonNullValue((Collection<?>) col);
		}
		if (col instanceof Object[]) {
			for (Object value : (Object[]) col) {
				if (value != null) {
					return value;
				}
			}
			return null;
		}
		return col;
	}

	private static boolean isPrimitiveArrayColumn(final Object value) {
		return value instanceof boolean[]
				|| value instanceof byte[]
				|| value instanceof char[]
				|| value instanceof short[]
				|| value instanceof int[]
				|| value instanceof long[]
				|| value instanceof float[]
				|| value instanceof double[];
	}

	private static boolean isArrayDataType(final DATA_TYPE colType) {
		return colType.getValue() >= DATA_TYPE.DT_BOOL_ARRAY.getValue();
	}

	private static boolean isDecimalType(final DATA_TYPE colType) {
		return colType == DATA_TYPE.DT_DECIMAL32
				|| colType == DATA_TYPE.DT_DECIMAL64
				|| colType == DATA_TYPE.DT_DECIMAL128
				|| colType == DATA_TYPE.DT_DECIMAL32_ARRAY
				|| colType == DATA_TYPE.DT_DECIMAL64_ARRAY
				|| colType == DATA_TYPE.DT_DECIMAL128_ARRAY;
	}

	private static DATA_TYPE toColumnArrayType(final DATA_TYPE colType) {
		switch (colType) {
			case DT_BOOL:
				return DATA_TYPE.DT_BOOL_ARRAY;
			case DT_BYTE:
				return DATA_TYPE.DT_BYTE_ARRAY;
			case DT_SHORT:
				return DATA_TYPE.DT_SHORT_ARRAY;
			case DT_INT:
				return DATA_TYPE.DT_INT_ARRAY;
			case DT_LONG:
				return DATA_TYPE.DT_LONG_ARRAY;
			case DT_DATE:
				return DATA_TYPE.DT_DATE_ARRAY;
			case DT_MONTH:
				return DATA_TYPE.DT_MONTH_ARRAY;
			case DT_TIME:
				return DATA_TYPE.DT_TIME_ARRAY;
			case DT_MINUTE:
				return DATA_TYPE.DT_MINUTE_ARRAY;
			case DT_SECOND:
				return DATA_TYPE.DT_SECOND_ARRAY;
			case DT_DATETIME:
				return DATA_TYPE.DT_DATETIME_ARRAY;
			case DT_TIMESTAMP:
				return DATA_TYPE.DT_TIMESTAMP_ARRAY;
			case DT_NANOTIME:
				return DATA_TYPE.DT_NANOTIME_ARRAY;
			case DT_NANOTIMESTAMP:
				return DATA_TYPE.DT_NANOTIMESTAMP_ARRAY;
			case DT_FLOAT:
				return DATA_TYPE.DT_FLOAT_ARRAY;
			case DT_DOUBLE:
				return DATA_TYPE.DT_DOUBLE_ARRAY;
			case DT_SYMBOL:
				return DATA_TYPE.DT_SYMBOL_ARRAY;
			case DT_STRING:
				return DATA_TYPE.DT_STRING_ARRAY;
			case DT_UUID:
				return DATA_TYPE.DT_UUID_ARRAY;
			case DT_DATEHOUR:
				return DATA_TYPE.DT_DATEHOUR_ARRAY;
			case DT_DATEMINUTE:
				return DATA_TYPE.DT_DATEMINUTE_ARRAY;
			case DT_IPADDR:
				return DATA_TYPE.DT_IPADDR_ARRAY;
			case DT_INT128:
				return DATA_TYPE.DT_INT128_ARRAY;
			case DT_COMPLEX:
				return DATA_TYPE.DT_COMPLEX_ARRAY;
			case DT_POINT:
				return DATA_TYPE.DT_POINT_ARRAY;
			case DT_DECIMAL32:
				return DATA_TYPE.DT_DECIMAL32_ARRAY;
			case DT_DECIMAL64:
				return DATA_TYPE.DT_DECIMAL64_ARRAY;
			case DT_DECIMAL128:
				return DATA_TYPE.DT_DECIMAL128_ARRAY;
			default:
				throw new IllegalArgumentException("Column type " + colType.getName() + " does not support direct array conversion.");
		}
	}

	private static Object findFirstNonNullValue(final Collection<?> values) {
		for (Object value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	private static byte[] booleanListValues(final List<Boolean> values) {
		byte[] data = new byte[values.size()];
		for (int i = 0; i < values.size(); ++i) {
			Boolean value = values.get(i);
			data[i] = value == null ? Byte.MIN_VALUE : (value ? (byte) 1 : (byte) 0);
		}
		return data;
	}
}
