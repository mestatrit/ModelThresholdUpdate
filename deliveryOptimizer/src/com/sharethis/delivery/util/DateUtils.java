package com.sharethis.delivery.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtils {
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat timeFormatWithZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	private static final String timeZone = "America/Los_Angeles";
	
	public static void initialize() {
		timeFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		timeFormatWithZone.setTimeZone(TimeZone.getTimeZone(timeZone));
		dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
	}
	
	public static long currentTime() {
		return new Date().getTime();
	}
	
	public static Date parseDate(String date) throws ParseException {
		return timeFormat.parse(date);
	}
	
	public static long parseTime(String date) throws ParseException {
		return parseDate(date).getTime();
	}
	
	public static long parseTime(ResultSet result, String field) throws SQLException {
		Date date = result.getTimestamp(field, Calendar.getInstance(TimeZone.getTimeZone(timeZone)));
		return date.getTime();
	}
	
	public static long parseDateTime(ResultSet result, String field) throws SQLException {
		Date date = result.getTimestamp(field);
		return date.getTime();
	}
	
	public static String toString(Date date) {
		return timeFormat.format(date);
	}

	public static String getDate(String format, int days) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, days);
		SimpleDateFormat dateFormat = new SimpleDateFormat(format, Locale.US); 
		dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		return dateFormat.format(calendar.getTime());
	}

	public static String getDayOfWeek(int days) {
		return getDate("EEE", days);
	}
	
	public static String getDatetime(int days) {
		return getDate("yyyy-MM-dd HH:mm:ss", days);
	}

	public static long getToday(int days) throws ParseException {
		String day = getDate("yyyy-MM-dd 00:00:00", days); 
		return parseTime(day);
	}
	
	public static String getDatetime(long time) {
		return timeFormat.format(time);
	}
	
	public static String getDatetimeWithZone(long time) {
		return timeFormatWithZone.format(time);
	}
	
	public static String getShortDate(long time) {
		return dateFormat.format(time);
	}
	
	public static String getDayOfWeek() {
		return getDayOfWeek(0);
	}
	
	public static String getDatetime() {
		return getDatetime(0);
	}
	
	public static int getIntDayOfWeek() {
		return getIntDayOfWeek(0L, 0);
	}
		
	public static int getIntDayOfWeek(long time, int n) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone), Locale.US);
		if (time > 0L) calendar.setTimeInMillis(time);
		return (calendar.get(Calendar.DAY_OF_WEEK) - 1 + n) % 7;
	}
	
	public static String getDatetimeUtc() {
		SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormatUtc.format(new Date());
	}
	
	public static boolean isWeekend(long time) {
		return isWeekend(time, 0);
	}
	
	public static boolean isWeekend(long time, int n) {
		int dayOfWeek = getIntDayOfWeek(time, n);
		return (dayOfWeek == 0 || dayOfWeek == 6);
	}
	

	public static int getMonth(long time) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone), Locale.US);
		calendar.setTimeInMillis(time);
		return calendar.get(Calendar.MONTH) + 1;
	}
	
	public static String getDatetimeUtc(int minutes) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, minutes);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US); 
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormat.format(calendar.getTime());
	}
}
