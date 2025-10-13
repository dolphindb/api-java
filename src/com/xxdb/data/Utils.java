package com.xxdb.data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.*;
import java.util.*;
import com.xxdb.data.Entity.DATA_CATEGORY;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.xxdb.data.Entity.DATA_TYPE.*;


public class Utils {

	public static final String JAVA_API_VERSION = "3.00.4.1";

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
}
