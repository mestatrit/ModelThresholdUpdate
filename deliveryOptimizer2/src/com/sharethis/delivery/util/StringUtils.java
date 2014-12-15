package com.sharethis.delivery.util;

public class StringUtils {
	public static String concat(String s1, String s2) {
		if (isEmpty(s1)) {
			return s2;
		} else if (isEmpty(s2)) {
			return s1;
		} else {
			return s1 + "; " + s2;
		}
	}
	
	public static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}
	
	public static String removeSpecialChars(String str) {
		return str.replaceAll("'", "").replaceAll("%", "").replaceAll("\"", "");
	}
}
