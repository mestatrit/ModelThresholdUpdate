package com.sharethis.delivery.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.sharethis.delivery.common.Constants;

public class DateUtils {
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat timeFormatWithZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
	private static SimpleDateFormat shortDateFormat = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final String timeZone = "America/Los_Angeles";
	
	public static void initialize() {
		timeFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		timeFormatWithZone.setTimeZone(TimeZone.getTimeZone(timeZone));
		shortDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
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
	
	public static String getShortDayOfWeek(int days) {
		return getDate("EE", days);
	}
	
	public static String getDatetime(int days) {
		return getDate("yyyy-MM-dd HH:mm:ss", days);
	}

	public static long getToday(int days) throws ParseException {
		String day = getDate("yyyy-MM-dd 00:00:00", days); 
		return parseTime(day);
	}
	
	public static String getToday() {
		return getDate("yyyy-MM-dd 00:00:00", 0); 
	}
	
	public static String getDatetime(long time) {
		return timeFormat.format(time);
	}
	
	public static String getDatetimeWithZone(long time) {
		return timeFormatWithZone.format(time);
	}
	
	public static String getShortDate(long time) {
		return shortDateFormat.format(time);
	}
	
	public static String getDate(long time) {
		return dateFormat.format(time);
	}
	
	public static String getDate(int days) {
		return getDate("yyyy-MM-dd", days); 
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
	
	
	public static long getTime(long time, int days) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone), Locale.US);
		if (time > 0L) calendar.setTimeInMillis(time);
		calendar.add(Calendar.DATE, days);
		return calendar.getTimeInMillis();
	}

	public static String getDatetimeUtc() {
		SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormatUtc.format(new Date());
	}
	
	public static String getDatetimeUtc(String date) throws ParseException {
		SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));	     
	    return dateFormatUtc.format(parseDate(date));
	}
	
	public static String getDatetimeUtc(long time) {
		SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormatUtc.format(time);
	}
	
	public static String parseDatetimeUtc(String dateStr, String timeStr) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		Date date = dateFormat.parse(dateStr + " " + timeStr);
		SimpleDateFormat dateFormatUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));	  
		return dateFormatUtc.format(date);
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
	
	public static double getHoursPerDay(long time) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone), Locale.US);
		calendar.setTimeInMillis(time);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		long start = calendar.getTimeInMillis();
		calendar.add(Calendar.DATE, 1);
		long end = calendar.getTimeInMillis();
		return (double) (end - start) / (double) Constants.MILLISECONDS_PER_HOUR;
	}
}
