package com.xxdb.streaming.data;

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
	    //1999.12.31 return 0

		if(month<1 || month>12 || day<0)
			return 0;

	    int days=0;
	    days=(year-2000)/4*1461;
	    year=(year-2000)%4;
	    days+=365*year;
	    if(year==0){
	    	//leap year
	    	days += cumLeapMonthDays[month-1];
	    	days += Math.min(day, leapMonthDays[month-1]);
	    }
	    else{
	    	if(year>=0) days++;
	    	days += cumMonthDays[month-1];
	    	days += Math.min(day, monthDays[month-1]);
	    }

		return days;
	}

	public static LocalDate parseDate(int days){
		int year, month, day;
		boolean leap=false;

		year=2000+(days/1461)*4;
		days=days%1461;
		if(days<0){
			year-=4;
			days+=1461;
		}
		if(days>366){
			year+=1;
			days=days-366;
			year+=days/365;
			days=days%365;
		}
		else{
			leap=true;
		}
		if(days==0){
			year=year-1;
			month=12;
			day=31;
		}
		else{
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
	
	public static LocalDateTime parseDateTime(int seconds){
		int days= seconds / 86400;
		LocalDate date = Utils.parseDate(days);
		seconds = seconds % 86400;
		if(seconds < 0)
			seconds += 86400;
		int hour = seconds/3600;
		seconds = seconds % 3600;
		int minute = seconds / 60;
		int second = seconds % 60;
		return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), hour, minute, second);
	}
	
	public static long countMilliseconds(LocalDateTime dt){
		return countMilliseconds(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano()/1000000);
	}
	
	public static long countMilliseconds(int year, int month, int day, int hour, int minute, int second, int millisecond){
		return countSeconds(year, month, day, hour, minute, second) * 1000L + millisecond;
	}
	
	public static LocalDateTime parseTimestamp(long milliseconds){
		int days= (int)(milliseconds / 86400000L);
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
	
	public static int countMilliseconds(LocalTime time){
		return countMilliseconds(time.getHour(), time.getMinute(), time.getSecond(), time.getNano() / 1000000);
	}
	
	public static int countMilliseconds(int hour, int minute, int second, int millisecond){
		return ((hour * 60 + minute) * 60 + second) * 1000+millisecond;
	}
	
	public static LocalTime parseTime(int milliseconds){
		return LocalTime.of(milliseconds/3600000, milliseconds/60000 % 60, milliseconds/1000 %60, milliseconds % 1000 *1000000);
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
}
