package com.sharethis.delivery.util;

public class MathUtils {
	public static double round(double x, int n) {
		if (Double.isNaN(x)) {
			return x;
		}
		double ax = Math.abs(x);
		double e = n + (ax < 1 ? Math.floor(-Math.log10(Math.max(ax, 1.0e-8))) : 0);
		double d = Math.pow(10.0, e);
		x = Math.round(d * x) / d;
		return x;
	}
}
