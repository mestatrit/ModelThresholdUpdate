package com.sharethis.delivery.optimization;

import com.sharethis.delivery.base.Metric;
import com.sharethis.delivery.base.State;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;

public class Statistics {
	private static final int weekdays = Constants.WEEKDAYS;
	private boolean validClickStatistics;
	private boolean validConversionStatistics;
	private int days;
	
	private int segmentCount;
	private double[][] impressions;
	private double[][] clicks;
	private double[] conversions;
	
	private double[] dailyClickFactor;
	private double[] dailyConversionFactor;
	private double[] clickRatio;
	private double[] conversionRatio;
	private double maxDailyClicks;
	private double maxDailyConversions;

	public Statistics(int segmentCount) {
		this.segmentCount = segmentCount;
		impressions = new double[segmentCount][weekdays];
		clicks = new double[segmentCount][weekdays];
		conversions = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			conversions[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				impressions[p][i] = clicks[p][i] = 0.0;
			}
		}
		days = 0;
		maxDailyClicks = 1.0;
		maxDailyConversions = 1.0;
		validConversionStatistics = false;
		validClickStatistics = false;

		
		dailyClickFactor = new double[weekdays];
		dailyConversionFactor = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			dailyClickFactor[i] = Constants.DAILY_CONVERSION_FACTOR[i];
			dailyConversionFactor[i] = Constants.DAILY_CONVERSION_FACTOR[i];
		}
	}
	
	public boolean add(State deliveredState, double[] clickRatio, double[] conversionRatio) {
		days++;
		int day = DateUtils.getIntDayOfWeek(deliveredState.getTime(), 0);
		double totalDeliveredClicks = 0.0;
		double totalDeliveredConversions = 0.0;
		for (int p = 0; p < segmentCount; p++) {
			Metric deliveredMetric = deliveredState.getDeliveredMetric(p);
			impressions[p][day] += deliveredMetric.getImpressions();
			double clks = deliveredMetric.getClicks();
			clicks[p][day] += clks;
			totalDeliveredClicks += clks;
			totalDeliveredConversions += deliveredMetric.getConversions();
		}
		maxDailyClicks = Math.max(totalDeliveredClicks, maxDailyClicks);
		if (deliveredState.getKpiStatus()) {
			conversions[day] += totalDeliveredConversions;
			maxDailyConversions = Math.max(totalDeliveredConversions, maxDailyConversions);
		}
		this.clickRatio = clickRatio;
		this.conversionRatio = conversionRatio;
		return true;
	}

	public boolean updateDailyClickFactor() {
		double imps = 0.0;
		double clks = 0.0;
		int zeroDays = 0;
		double[] weeklyImpressions = new double[weekdays];
		double[] weeklyClicks = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			weeklyImpressions[i] = weeklyClicks[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				weeklyImpressions[i] += impressions[p][i];
				weeklyClicks[i] += clicks[p][i];
			}
			imps += weeklyImpressions[i];
			clks += weeklyClicks[i];
			zeroDays += (weeklyClicks[i] == 0 ? 1 : 0);
		}
		if (imps < 200000.0 || clks < 10 || days < weekdays || zeroDays > 1) 
			return false;
		
		validClickStatistics = true;
		double sum = 0.0;
		double priorClicks = clks / 20.0 + Math.max(maxDailyClicks, 5.0) * weekdays;
		double priorDailyClicks = priorClicks / (double) weekdays;
		double[] aveClicks = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			aveClicks[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				aveClicks[i] += clickRatio[p] * impressions[p][i];
			}
			dailyClickFactor[i] = (weeklyClicks[i] + priorDailyClicks) / (aveClicks[i] + priorClicks);
			sum += dailyClickFactor[i];
		}
		sum /= (double) weekdays;
		for (int i = 0; i < weekdays; i++) {
			dailyClickFactor[i] /= sum;
		}
		
		return true;
	}

	public boolean updateDailyConversionFactor() {
		double imps = 0.0;
		double conv = 0.0;
		int zeroDays = 0;
		double[] weeklyImpressions = new double[weekdays];
		double[] weeklyConversions = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			weeklyImpressions[i] = weeklyConversions[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				weeklyImpressions[i] += impressions[p][i];
			}
			weeklyConversions[i] += conversions[i];
			imps += weeklyImpressions[i];
			conv += weeklyConversions[i];
			zeroDays += (weeklyConversions[i] == 0 ? 1 : 0);
		}
		if (imps < 200000.0 || conv < 10 || days < weekdays || zeroDays > 1) 
			return false;
		
		validConversionStatistics = true;
		double sum = 0.0;
		double priorConversions = conv / 20.0 + Math.max(maxDailyConversions, 5.0) * weekdays;
		double priorDailyConversions = priorConversions / (double) weekdays;
		double[] aveConversions = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			aveConversions[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				aveConversions[i] += conversionRatio[p] * impressions[p][i];
			}
			dailyConversionFactor[i] = (weeklyConversions[i] + priorDailyConversions) / (aveConversions[i] + priorConversions);
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
	
	public double[] getDailyClickFactor() {
		return dailyClickFactor;
	}
	
	private double getClickG2() {
		if (!validClickStatistics) {
			return 0.0;
		}
		double[] factor = new double[weekdays];
		double sum1 = 0.0;
		double sum2 = 0.0;
		double[] weeklyClicks = new double[weekdays];
		for (int i = 0; i < weekdays; i++) {
			double clks = 0.0;
			weeklyClicks[i] = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				clks += clickRatio[p] * impressions[p][i];
				weeklyClicks[i] += clicks[p][i];
			}
			factor[i] = weeklyClicks[i] / clks;
			sum1 += weeklyClicks[i];
			sum2 += clks;
		}
		sum2 /= sum1;
		double g2 = 0.0;
		for (int i = 0; i < weekdays; i++) {
			g2 += (weeklyClicks[i] > 0 ? 2.0 * weeklyClicks[i] * Math.log(sum2 * factor[i]) : 0.0);
		}
		return g2;
	}
	
	private double getConversionG2() {
		if (!validConversionStatistics) {
			return 0.0;
		}
		double[] factor = new double[weekdays];
		double sum1 = 0.0;
		double sum2 = 0.0;
		for (int i = 0; i < weekdays; i++) {
			double conv = 0.0;
			for (int p = 0; p < segmentCount; p++) {
				conv += conversionRatio[p] * impressions[p][i];
			}
			factor[i] = conversions[i] / conv;
			sum1 += conversions[i];
			sum2 += conv;
		}
		sum2 /= sum1;
		double g2 = 0.0;
		for (int i = 0; i < weekdays; i++) {
			g2 += (conversions[i] > 0 ? 2.0 * conversions[i] * Math.log(sum2 * factor[i]) : 0.0);
		}
		return g2;
	}
	
	public String clickFactorToString() {
		String str = String.format("%4.2f", dailyClickFactor[0]);
		for (int i = 1; i < weekdays; i++) {
			str += String.format(" %4.2f", dailyClickFactor[i]);
		}
		str += String.format(": %.1f", getClickG2());
		return str;
	}
	
	public String conversionFactorToString() {
		String str = String.format("%4.2f", dailyConversionFactor[0]);
		for (int i = 1; i < weekdays; i++) {
			str += String.format(" %4.2f", dailyConversionFactor[i]);
		}
		str += String.format(": %.1f", getConversionG2());
		return str;
	}
}
