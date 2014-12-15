package com.sharethis.delivery.optimization;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.record.PerformanceRecord;
import com.sharethis.delivery.util.DateUtils;

public class Statistics {
	private static final int weekdays = Constants.WEEKDAYS;
	private boolean validStatistics;
	private int days;
	private double[] impressions0;
	private double[] impressions1;
	private double[] conversions;
	private double[] dailyConversionFactor;
	private double[] conversionRatio;
	private double maxDailyConversions;

	public Statistics() {
		days = 0;
		impressions0 = new double[weekdays];
		impressions1 = new double[weekdays];
		conversions = new double[weekdays];
		dailyConversionFactor = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			dailyConversionFactor[i] = Constants.DAILY_CONVERSION_FACTOR[i];
			conversions[i] = impressions0[i] = impressions1[i] = 0.0;
		}
		maxDailyConversions = 1.0;
		validStatistics = false;
	}
	
	public boolean add(PerformanceRecord record) {
		days++;
		int day = DateUtils.getIntDayOfWeek(record.getTime(), 0);
		impressions0[day] += (double) record.getDeliveredImpressions(0);
		impressions1[day] += (double) record.getDeliveredImpressions(1);
		double totalDeliveredConversions = record.getTotalDeliveredConversions();
		conversions[day] += totalDeliveredConversions;
		maxDailyConversions = Math.max(totalDeliveredConversions, maxDailyConversions);
		conversionRatio = record.getEstimatedConversionRatios();
		return true;
	}

	public boolean updateDailyConversionFactor() {
		double imps0 = 0.0;
		double imps1 = 0.0;	
		double cvrs = 0.0;
		int zeroDays = 0;
		for (int i = 0; i < weekdays; i++) {
			imps0 += impressions0[i];
			imps1 += impressions1[i];
			cvrs += conversions[i];
			zeroDays += (conversions[i] == 0 ? 1 : 0);
		}
		double imps = imps0 + imps1;
		if (imps < 200000.0 || cvrs < 10 || days < weekdays || zeroDays > 1) 
			return false;
		
		validStatistics = true;
		double sum = 0.0;
		double priorConversions = cvrs / 20.0 + Math.max(maxDailyConversions, 5.0) * weekdays;
		double priorDailyConversions = priorConversions / (double) weekdays;
		for (int i = 0; i < weekdays; i++) {
			dailyConversionFactor[i] = (conversions[i] + priorDailyConversions) / (conversionRatio[0] * impressions0[i] + conversionRatio[1] * impressions1[i] + priorConversions);
			sum += dailyConversionFactor[i];
		}
		sum /= (double) weekdays;
		for (int i = 0; i < weekdays; i++) {
			dailyConversionFactor[i] /= sum;
		}
		
		return true;
	}
	
	public double[] getDailyConversionFactor() {
		return dailyConversionFactor;
	}
	
	private double getG2() {
		if (!validStatistics) {
			return 0.0;
		}
		double[] factor = new double[weekdays];
		double sum1 = 0.0;
		double sum2 = 0.0;
		for (int i = 0; i < weekdays; i++) {
			double convs = conversionRatio[0] * impressions0[i] + conversionRatio[1] * impressions1[i];
			factor[i] = conversions[i] / convs;
			sum1 += conversions[i];
			sum2 += convs;
		}
		sum2 /= sum1;
		double g2 = 0.0;
		for (int i = 0; i < weekdays; i++) {
			g2 += (conversions[i] > 0 ? 2.0 * conversions[i] * Math.log(sum2 * factor[i]) : 0.0);
		}
		return g2;
	}
	
	public String factorToString() {
		String str = String.format("%4.2f", dailyConversionFactor[0]);
		for (int i = 1; i < weekdays; i++) {
			str += String.format(" %4.2f", dailyConversionFactor[i]);
		}
		str += String.format(": %.1f", getG2());
		return str;
	}
}
