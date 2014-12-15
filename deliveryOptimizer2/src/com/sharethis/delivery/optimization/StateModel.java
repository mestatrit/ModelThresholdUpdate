package com.sharethis.delivery.optimization;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.CampaignState;
import com.sharethis.delivery.base.CampaignStates;
import com.sharethis.delivery.base.CampaignType;
import com.sharethis.delivery.base.Comments;
import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.base.Metric;
import com.sharethis.delivery.base.State;
import com.sharethis.delivery.base.StrategyType;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.estimation.GaussianDistribution;
import com.sharethis.delivery.solvers.SolverException;
import com.sharethis.delivery.util.DateUtils;

public class StateModel {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private Parameters campaignParameters;
	private static Parameters globalParameters = Constants.globalParameters;
	private CampaignType campaignType;
	private StrategyType strategy;
	private CampaignStates pastCampaignStates;
	private CampaignState futureCampaignState;
	private Statistics weeklyTrend;
	
	private Map<Integer, List<Double>> imps;
	private Map<Integer, List<Double>> clks;
	private Map<Integer, List<Double>> conv;
	private Map<Integer, List<Double>> cost;
	
	private double[] ratio;
	private double[] clickRatio, conversionRatio;
	private double[] priorClickRatio, priorConversionRatio;
	private double[] priorWeight;
	private double[] averageEcpm;
	
	private double impressions, clicks, conversions;
	
	private Errors errors;
	private Comments comments;
	private int segmentCount;
	private int day;
	
	private double ratioTarget;
	
	private double[] campaignSpent;
	private int[] campaignImpressions;
	private double i1, i2;

	private double[] dailyClickFactor;
	private double[] dailyConversionFactor;
	
	public boolean isCurrentCampaign() {
		return pastCampaignStates.isCurrentCampaign(day);
	}

	public Errors getErrors() {
		return errors;
	}
	
	public boolean hasNext() {
		day++;
		return (day < pastCampaignStates.size());
	}
	
	public void reset() {
		day = -1;
	}
	
	public void resetToCurrentCampaign() {
		day = pastCampaignStates.getCurrentCampaignIndex() - 1;
	}
	
	public StateModel(Parameters campaignParameters, CampaignStates pastCampaignStates, CampaignState futureCampaignState) {
		this.campaignParameters = campaignParameters;
		this.pastCampaignStates = pastCampaignStates;
		this.futureCampaignState = futureCampaignState;
		
		strategy = campaignParameters.getStrategy();
		
		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		campaignType = CampaignType.valueOf(campaignParameters);
		weeklyTrend = new Statistics(segmentCount);
		
		comments = new Comments(segmentCount);
		errors = new Errors();
		
		imps = new HashMap<Integer, List<Double>>();
		clks = new HashMap<Integer, List<Double>>();
		conv = new HashMap<Integer, List<Double>>();
		cost = new HashMap<Integer, List<Double>>();
		for (int p = 0; p < segmentCount; p++) {
			imps.put(p, new ArrayList<Double>());
			clks.put(p, new ArrayList<Double>());
			cost.put(p, new ArrayList<Double>());
		}
		conv.put(0, new ArrayList<Double>());
		
		day = -1;
		ratio = new double[segmentCount];
		clickRatio = new double[segmentCount];
		conversionRatio = new double[segmentCount];
		priorClickRatio = new double[segmentCount];
		priorConversionRatio = new double[segmentCount];
		priorWeight = new double[segmentCount];
		double priorImpressions = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
		for (int p = 0; p < segmentCount; p++) {
			if (campaignType.isCpc()) {
				clickRatio[p] = priorClickRatio[p] 
						= campaignParameters.getDouble(String.format("%s%d", Constants.PRIOR_RATIO, p), 2.0e-5);
				conversionRatio[p] = priorConversionRatio[p] = (p == 0 ? 1.0e-5 : 0.0);
				priorWeight[p] = Math.max(priorImpressions, 1000.0);
			} else {
				clickRatio[p] = priorClickRatio[p] = (p == 0 ? 1.0e-5 : 0.0);
				conversionRatio[p] = priorConversionRatio[p] 
						= campaignParameters.getDouble(String.format("%s%d", Constants.PRIOR_RATIO, p), 2.0e-5);
				priorWeight[p] = Math.max(priorImpressions, 10000.0);
//				priorWeight[p] = campaignParameters.getDouble(String.format("%s%d", Constants.PRIOR_WEIGHT, p), 0.0);
			}
		}
	
		campaignSpent = new double[segmentCount];
		averageEcpm = new double[segmentCount];
		campaignImpressions = new int[segmentCount];
		for (int i = 0; i < segmentCount; i++) {
			campaignImpressions[i] = 0;
			campaignSpent[i] = 0.0;
		}
		
		dailyClickFactor = new double[Constants.WEEKDAYS];
		dailyConversionFactor = new double[Constants.WEEKDAYS];
		for (int i = 0; i < Constants.WEEKDAYS; i++) {
			dailyClickFactor[i] = Constants.DAILY_CLICK_FACTOR[i];
			dailyConversionFactor[i] = Constants.DAILY_CONVERSION_FACTOR[i];
		}
	}

	public String getDailyConversionFactor() {
		return weeklyTrend.conversionFactorToString();
	}
	
	public double getKpiScore() {
		Metric cumDeliveryMetric = pastCampaignStates.getCumDeliveryMetric();
		double kpi = campaignParameters.getDouble(Constants.KPI, 0.0);
		double ekpi = campaignType.getEkpi(cumDeliveryMetric);
		return (Double.isNaN(ekpi) ? 1000.0 : (ekpi - kpi)/kpi);
	}
	
	public int updateDoTables() throws SQLException {
		return pastCampaignStates.updateDoTables(day);
	}
	
	private void setEndOfCampaignDeliveryFactor(long currentTime) {
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		if (currentTime + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime) {
			strategy.setEndOfCampaignDeliveryFactor();
//			log.info("state factor: " + state.strategy.deliveryFactorToString());
		}
	}
	
	private void computeTargets() {
		long recordTime = futureCampaignState.getTime();
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		double hoursLeft = (double) (endTime - recordTime) / (double) Constants.MILLISECONDS_PER_HOUR;

		setEndOfCampaignDeliveryFactor(recordTime);
		
		double intervalsLeft = Math.max(1.0, hoursLeft / deliveryInterval);
		int n = (int) Math.round(intervalsLeft);
		double totalDelivery = 0.0;
		long time = recordTime;
		for (int i = 0; i < n; i++) {
			totalDelivery += strategy.getDeliveryFactor(time, 0);	
			double dayLength = DateUtils.getHoursPerDay(time);
			double numberOfIntervals = Math.round(dayLength / deliveryInterval);
			double normalizedDeliveryInterval = dayLength / numberOfIntervals;
			time += (long) (normalizedDeliveryInterval * Constants.MILLISECONDS_PER_HOUR);
		}
		double deliveryFraction = (totalDelivery > 0 ? 1.0 / totalDelivery : 0.0);
		
		double targetImpressions = campaignParameters.getDouble(Constants.TARGET_IMPRESSIONS, 0.0);
		double targetClicks = campaignParameters.getDouble(Constants.TARGET_CLICKS, 0.0);
		double targetConversions = campaignParameters.getDouble(Constants.TARGET_CONVERSIONS, 0.0);
		Metric totalCumulativeDeliveredMetric = pastCampaignStates.getTotalCumulativeDeliveryMetric(day);
		double totalCumulativeImpressions = totalCumulativeDeliveredMetric.getImpressions();
		double totalCumulativeClicks = totalCumulativeDeliveredMetric.getClicks();
		double totalCumulativeConversions = totalCumulativeDeliveredMetric.getConversions();

		impressions = deliveryFraction * (targetImpressions - totalCumulativeImpressions);
		clicks = deliveryFraction * (targetClicks - totalCumulativeClicks);
		conversions = deliveryFraction * (targetConversions - totalCumulativeConversions);
		double leaveOutImpressions = (double) campaignParameters.getInt(Constants.LEAVE_OUT_IMPRESSIONS, 0);
		if (impressions < leaveOutImpressions && leaveOutImpressions > 0) {
			impressions = 1000.0;
		} else {
			impressions -= leaveOutImpressions;
		}

		double uniformDeliveryFraction = (double) (recordTime - startTime) / (double) (endTime - startTime);
		double impressionExcess = totalCumulativeImpressions - uniformDeliveryFraction * targetImpressions;
		double clickExcess = totalCumulativeClicks - uniformDeliveryFraction * targetClicks;
		double conversionExcess = totalCumulativeConversions - uniformDeliveryFraction * targetConversions;
		
		if (!campaignType.isCpc() && conversionExcess < 0) {
			double reversionIntervals = (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000);
			if (reversionIntervals < intervalsLeft) {
				conversions = campaignParameters.getDouble(Constants.CONVERSIONS_PER_INTERVAL, 0.0) - conversionExcess/reversionIntervals;
			}
			double impressionIntervalsLeft = (double) globalParameters.getInt(Constants.IMPRESSION_INTERVALS_LEFT, 0);
			if (intervalsLeft < impressionIntervalsLeft) {
				impressions = Math.max(impressions, campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0));
			}
		}
		if (campaignType.isCpc() && clickExcess < 0) {
			double reversionIntervals = (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000);
			if (reversionIntervals < intervalsLeft) {
				clicks = campaignParameters.getDouble(Constants.CLICKS_PER_INTERVAL, 0.0) - clickExcess/reversionIntervals;
			}
			double impressionIntervalsLeft = (double) globalParameters.getInt(Constants.IMPRESSION_INTERVALS_LEFT, 0);
			if (intervalsLeft < impressionIntervalsLeft) {
				impressions = Math.max(impressions, campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0));
			}
		}
		if (impressionExcess < 0) {
			int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
			double endOfCampaignDeliveryIntervals = 24.0 * endOfCampaignDeliveryPeriodDays / deliveryInterval;
			if (intervalsLeft <= endOfCampaignDeliveryIntervals) {
				deliveryFraction = (totalDelivery > 0 ? 1.0 : 0.0);
			} else {
				deliveryFraction = (totalDelivery > 0 ? intervalsLeft / totalDelivery : 0.0);
			}
			
			double reversionIntervals = Math.min(intervalsLeft, (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000));
			impressions = deliveryFraction * campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0)
									- impressionExcess/reversionIntervals;
		}

		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		boolean maxEkpiExceeded = false;
		if (impressions > 0) {
			double maxEkpi = campaignParameters.getDouble(Constants.MAX_EKPI, Double.MAX_VALUE);
			double actionFloor = 0.001 * cpm * impressions / maxEkpi;
			if (campaignType.isCpc()) {
				if (clicks < actionFloor) {
					maxEkpiExceeded = (clicks > 0);
					clicks = actionFloor;
				}
			} else {
				if (conversions < actionFloor) {
					maxEkpiExceeded = (conversions > 0);
					conversions = actionFloor;
				}
			}
		}
		
		double conversionFactor = (DateUtils.isWeekend(recordTime) ? 1.0 : 1.0 + globalParameters.getDouble(Constants.KPI_GOAL_MARGIN, 0.0));
		conversions *= conversionFactor;
	
		impressions = Math.max(impressions, 0.0);
		clicks = Math.max(clicks, 0.0);
		conversions = Math.max(conversions, 0.0);
		
		if (strategy.isPausedDelivery()) {
			impressions = clicks = conversions = 0.0;
		} else {
			comments.addGoalComments(targetConversions < totalCumulativeConversions, targetImpressions < totalCumulativeImpressions);
			if (maxEkpiExceeded) {
				comments.add("Max eKPI exceeded");
			}
		}
	}
	
	private boolean validate() {
		int impressionLimit = Math.min(30 * (int) campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0),
				globalParameters.getInt(Constants.DAILY_IMPRESSION_LIMIT, Constants.MAX_IMPRESSION_TARGET));
		if (impressions > impressionLimit) {
			String cause = String.format("Daily impression limit exceeded (%,d, %,d)", (int) Math.round(impressions), impressionLimit);
			errors.add(cause);
			comments.set("Error: " + cause);
			impressions = 0;
			return false;
		}
		
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double days = (double) (endTime - startTime) / (double) (24 * Constants.MILLISECONDS_PER_HOUR);
		double dailyBudgetLimit = 30.0 * campaignParameters.getDouble(Constants.BUDGET, 0.0) / days;
		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		double customerSpend = 0.001 * cpm * impressions;
		double maxSpend = globalParameters.getDouble(Constants.DAILY_SPEND_LIMIT, Constants.MAX_DAILY_SPEND);
		maxSpend = Math.min(maxSpend, dailyBudgetLimit);
		
		if (customerSpend > maxSpend) {
			impressions = 0;
			String cause = String.format("Daily customer spend limit exceeded (%.1f, %.1f)", customerSpend, maxSpend);
			errors.add(cause);
			comments.set("Error: " + cause);
			return false;
		}
		return true;
	}
	
	public boolean optimize() {
		if (strategy.isPausedDelivery()) {
			ratioTarget = 0.0;
			i1 = 0.0;
			i2 = 0.0;
			errors.add("Paused");
			return true;
		}
		
		computeTargets();
		if (!validate()) {
			return false;
		}

		if (segmentCount == 1) {
			return true;
		}
		
		double kpi = (campaignType.isCpc() ? clicks : conversions);

		day++;
		
		if (kpi > 0) {
			if (impressions < 1) impressions = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
			ratioTarget = kpi / impressions;
		} else {
			ratioTarget = 0.0;
			i1 = 0.0;
			i2 = impressions;
			return true;
		}
		
		double kpiScore = getKpiScore();
		double effKpiFactor = (kpiScore > 0 ? kpiScore /(kpiScore + 1.0) : 0.0);
		double effRatioTarget = (1.0 + effKpiFactor) * ratioTarget;
		
		double[] costPerKpi = new double[segmentCount];
		if (averageEcpm[0] == 0 || averageEcpm[1] == 0) {
			averageEcpm[0] = averageEcpm[1] = 1.0;
		}
		costPerKpi[0] = 0.001 * averageEcpm[0] / Math.max(ratio[0], 1.0e-7);
		costPerKpi[1] = 0.001 * averageEcpm[1] / Math.max(ratio[1], 1.0e-8);
		
		if (ratio[0] >= effRatioTarget && ratio[1] >= effRatioTarget) {
			if (costPerKpi[0] < costPerKpi[1]) {
				i1 = impressions;
				i2 = 0.0;
				comments.add(0, "Exceeds conversion goal");
			} else {
				i1 = 0.0;
				i2 = impressions;
				comments.add(1, "Exceeds conversion goal");
			}
		} else if (ratio[0] <= effRatioTarget && ratio[1] <= effRatioTarget) {
			if (ratio[0] >= ratio[1]) {
				i1 = impressions;
				i2 = 0.0;
				comments.add(0, "Does not meet conversion goal");
			} else {
				i1 = 0.0;
				i2 = impressions;
				comments.add(1, "Does not meet conversion goal");
			}
		} else {
			if (ratio[0] > ratio[1]) {
				i1 = impressions * (effRatioTarget - ratio[1]) / (ratio[0] - ratio[1]);
				i2 = impressions * (ratio[0] - effRatioTarget) / (ratio[0] - ratio[1]);
			} else if (ratio[0] < ratio[1]) {
				i1 = impressions * (ratio[1] - effRatioTarget) / (ratio[1] - ratio[0]);
				i2 = impressions * (effRatioTarget - ratio[0]) / (ratio[1] - ratio[0]);
			} else {
				i1 = 0.5*impressions;
				i2 = 0.5*impressions;
			}
		}

		final double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		final double minMargin = campaignParameters.getDouble(Constants.MIN_MARGIN, 0.0);
		final double impressionsPerInterval = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
		final double spreadLimit = 0.5;
		
		double predictedImpressions = i1 + i2;
		allocateWithoutComments();
		double[] predictedEcpm = futureCampaignState.getPredictedEcpm();
		double predictedSpread = Math.abs(predictedEcpm[0] - predictedEcpm[1]);
		
		double totalSpent = 0.001 * predictedEcpm[0] * i1 + campaignSpent[0] + 0.001 * predictedEcpm[1] * i2 + campaignSpent[1];
		double totalImpressions = i1 + campaignImpressions[0] + i2 + campaignImpressions[1];
		double totalMargin = (totalImpressions < 1 ? 100.0 : 1.0 - totalSpent / (0.001 * cpm * totalImpressions));

		if (predictedSpread > spreadLimit && totalMargin < minMargin && predictedImpressions > 0.1 * impressionsPerInterval && totalImpressions > 3.0 * impressionsPerInterval) {
			double margin = (i1 * (cpm - predictedEcpm[0]) + i2 * (cpm - predictedEcpm[1])) / (cpm * (i1 + i2));
			if (margin < minMargin) {
				double minMarginRatio = ((1.0 - minMargin)*cpm - predictedEcpm[1]) / predictedSpread;
				minMarginRatio = Math.min(1.0, Math.max(0.0, minMarginRatio));
		
				double i1old = i1;
				double i2old = i2;
				i1 = minMarginRatio * predictedImpressions;
				i2 = (1.0 - minMarginRatio) * predictedImpressions;
				
				comments.add(0, String.format("; Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i1old), Math.round(i1)));
				comments.add(1, String.format("; Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i2old), Math.round(i2)));
			}
		}

		return true;
	}
	
	public void allocateWithComments() {
		allocate(true);
	}
	
	public void allocateWithoutComments() {
		allocate(false);
	}
	
	private void allocate(boolean addComments) {
		Metric[] impressionTarget = getImpressionTarget(0, addComments);
		Metric[] nextImpressionTarget = getImpressionTarget(1, false);
		futureCampaignState.setImpressionTargets(impressionTarget, nextImpressionTarget, comments);
	}

	public void estimate() {
		if (strategy.isPausedDelivery()) {
			return;
		}
		State campaignState = pastCampaignStates.get(day);
		if (campaignState.getTotalDeliveredMetric().getImpressions() < 1000) {
			return;
		}

		double totalConversions = 0.0;
		for (int p = 0; p < segmentCount; p++) {
			Metric deliveredMetric = campaignState.getDeliveredMetric(p);
			imps.get(p).add(deliveredMetric.getImpressions());
			clks.get(p).add(deliveredMetric.getClicks());
			cost.get(p).add(deliveredMetric.getCost());
			totalConversions += deliveredMetric.getConversions();
		}
		
		try {
//			double[] ap = StretchedExponentialLikelihood.maximizeLikelihood(hi1, hi2, hc, ao);
			clickRatio = GaussianDistribution.maximizeLikelihood(imps, clks, priorClickRatio, priorWeight);
			campaignState.updateClickRatios(clickRatio);
			for (int p = 0; p < segmentCount; p++) {
				double aveImps = GaussianDistribution.average(imps.get(p));
				averageEcpm[p] = 1000.0 * (aveImps < 1 ? 0.0 : GaussianDistribution.average(cost.get(p)) / aveImps);
			}
		} catch (SolverException e) {
			log.error(e.getMessage());
			clickRatio[segmentCount] = -1.0;
		}
		if (!campaignType.isCpc()) {
			conv.get(0).add(campaignState.getKpiStatus() ? totalConversions : null);
			try {
				conversionRatio = GaussianDistribution.maximizeLikelihood(imps, conv, priorConversionRatio, priorWeight);
				campaignState.updateConversionRatios(conversionRatio);
			} catch (SolverException e) {
				log.error(e.getMessage());
				conversionRatio[segmentCount] = -1.0;
			}
		}
		campaignState.setEstimatorStatus();
		ratio = (campaignType.isCpc() ? clickRatio : conversionRatio);
	}

	public void updateStatistics() throws ParseException {
		if (strategy.isWeekdayDelivery()) {
			return;
		}
		State deliveredState = pastCampaignStates.get(day);
		weeklyTrend.add(deliveredState, clickRatio, conversionRatio);
		if (weeklyTrend.updateDailyClickFactor()) {
			dailyClickFactor = weeklyTrend.getDailyClickFactor();
			if (campaignType.isCpc()) {
				strategy.setNonuniformDeliveryFactor(dailyClickFactor);
			}
		}
		if (weeklyTrend.updateDailyConversionFactor()) {
			dailyConversionFactor = weeklyTrend.getDailyConversionFactor();
			if (!campaignType.isCpc()) {
				strategy.setNonuniformDeliveryFactor(dailyConversionFactor);
			}
		}
	}

	private double getDeliveryFactor(long deliveryTime, int addDays) {
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		double endOfCampaignDeliveryMargin = campaignParameters.getDouble(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN, 0.1);
		double campaignDeliveryMargin = campaignParameters.getDouble(Constants.CAMPAIGN_DELIVERY_MARGIN, 0.1);
		double impressionsPerInterval = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
		if (deliveryTime + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime) {
			if (impressions < 10) {
				return strategy.getDeliveryFactor(deliveryTime, addDays);
			} else {
				return (1.0 + endOfCampaignDeliveryMargin * impressionsPerInterval / impressions) * strategy.getDeliveryFactor(deliveryTime, addDays);
			}
		} else {
			if (DateUtils.isWeekend(deliveryTime, addDays) && !strategy.isUniformDelivery()) {
				campaignDeliveryMargin = 0;
			}
			return (1.0 + campaignDeliveryMargin) * strategy.getDeliveryFactor(deliveryTime, addDays);
		}
	}
	
	public Metric[] getImpressionTarget(int addDays, boolean addComments) {
		long deliveryTime = futureCampaignState.getTime();//currentTime + (long) Math.round(addDays * deliveryInterval * Constants.MILLISECONDS_PER_HOUR) + 10L;
		long nextDeliveryTime = DateUtils.getTime(deliveryTime, addDays);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		boolean noDelivery = (strategy.isPausedDelivery() ||
							strategy.isWeekdayDelivery() && DateUtils.isWeekend(nextDeliveryTime) ||
							nextDeliveryTime >= endTime);
		if (noDelivery) {
			Metric[] target = new Metric[segmentCount];
			for (int p = 0; p < segmentCount; p++) {
				target[p] = new Metric();
			}
			return target;
		}
		
		double deliveryFactor = this.getDeliveryFactor(deliveryTime, addDays);
		
		if (segmentCount == 1) {
			double factor = (addDays == 0 ? 1.0 : globalParameters.getDouble(Constants.NEXT_DAY_IMPRESSION_FACTOR, 0.0));
			Metric[] target = toFinalTarget(new double[]{deliveryFactor * impressions}, factor, nextDeliveryTime);
			return target;
		}
		
		double[] impressionTarget = new double[segmentCount];
		double[] maxImpressions = futureCampaignState.getMaxImpressions();
		double segmentOneFactor = globalParameters.getDouble(Constants.SEGMENT_ONE_FACTOR, 1.0);
		double[] scaledMaxImpressions = new double[segmentCount];
		scaledMaxImpressions[0] = segmentOneFactor * maxImpressions[0];
		scaledMaxImpressions[1] = maxImpressions[1];

		if (strategy.isFixedRt()) {
			impressionTarget[0] = strategy.getRtImpressions();
			impressionTarget[1] = Math.max(deliveryFactor * impressions - impressionTarget[0], 0.0);
			if (addComments) {
				comments.add(0, "Delivery target set to user defined value");
			}
			if (impressionTarget[1] > strategy.getNrtImpressions()) {
				impressionTarget[1] = strategy.getNrtImpressions();
				if (addComments) {
					comments.add(1, "Delivery target set to user defined value");
				}
			}
		} else if (strategy.isMaxRt()) {
			impressionTarget[0] = Math.min(deliveryFactor * impressions, scaledMaxImpressions[0]);
			impressionTarget[1] = Math.max(deliveryFactor * impressions - impressionTarget[0], 0.0);
			if (addComments) {
				comments.add(0, "Delivery target maximized");
			}
			checkForLowerBounds(impressionTarget, maxImpressions);
//			adjustBySegmentFactors(impressionTarget, maxImpressions);
			if (impressionTarget[1] > strategy.getNrtImpressions()) {
				impressionTarget[1] = strategy.getNrtImpressions();
				if (addComments) {
					comments.add(1, "Delivery target set to user defined value");
				}
			}
		} else if (strategy.isRatio()) {
			impressionTarget[0] = Math.min(strategy.getRatio() * deliveryFactor * impressions, scaledMaxImpressions[0]);
			impressionTarget[1] = Math.max(deliveryFactor * impressions - impressionTarget[0], 0.0);
		} else {
			impressionTarget[0] = deliveryFactor * i1;
			impressionTarget[1] = deliveryFactor * i2;
			if (impressionTarget[0] > scaledMaxImpressions[0]) {
				if (addComments) {
					comments.add(0, String.format("Impression target (%,.0f) exceeds daily max (%,.0f) by factor %.1f",
							impressionTarget[0], maxImpressions[0], segmentOneFactor));
				}
				impressionTarget[1] = Math.min(impressionTarget[0] + impressionTarget[1] - scaledMaxImpressions[0], scaledMaxImpressions[1]);
				impressionTarget[0] = scaledMaxImpressions[0];
			} else if (impressionTarget[1] > scaledMaxImpressions[1]) {
				if (addComments) {
					comments.add(1, String.format("Impression target (%,.0f) exceeds daily max (%,.0f)",
							impressionTarget[1], maxImpressions[1]));
				}
				impressionTarget[0] = Math.min(impressionTarget[0] + impressionTarget[1] - scaledMaxImpressions[1], scaledMaxImpressions[0]);
				impressionTarget[1] = scaledMaxImpressions[1];
			}

			double deficit = Math.max(deliveryFactor * impressions - impressionTarget[0] - impressionTarget[1], 0);
			impressionTarget[1] = Math.max(impressionTarget[1] + deficit, 0.0);
			checkForLowerBounds(impressionTarget, maxImpressions);
		}
		double factor;
		if (addDays == 0) {
			factor = 1.0;
			checkForOverDelivery(impressionTarget, addComments);
		} else {
			factor = globalParameters.getDouble(Constants.NEXT_DAY_IMPRESSION_FACTOR, 0.0);
		}
		Metric[] target = toFinalTarget(impressionTarget, factor, nextDeliveryTime);
		return target;
	}
	
	private void checkForOverDelivery(double[] impressionTarget, boolean addComments) {
		if (segmentCount == 1) {
			return;
		}
		boolean disableOverDeliveryCheck = Constants.globalParameters.getBoolean(Constants.DISABLE_OVER_DELIVERY_CHECK, true);
		if (disableOverDeliveryCheck) {
			return;
		}
		try {
			if (futureCampaignState.wouldOverDeliver(impressionTarget)) {
				boolean overDelivery = false;
				double overDeliveryAmount = 0.0;
				String cause = String.format("Over delivery: Impression targets adjusted (were: %.0f, %.0f; ", impressionTarget[0], impressionTarget[1]);
				double[] delivery = futureCampaignState.getCurrentImpressionDelivery();
				if (impressionTarget[1] < delivery[1]) {
					double correction = delivery[1] - impressionTarget[1];
					if (impressionTarget[0] > delivery[0]) {
						overDeliveryAmount =  Math.max(correction - impressionTarget[0], 0.0);
//						impressionTarget[0] = Math.max(impressionTarget[0] - correction, 0.0);
						overDeliveryAmount += Math.max(delivery[0] - impressionTarget[0], 0.0);
					} else {
						overDelivery = true;
					}
				} else if (impressionTarget[0] < delivery[0]) {
					double correction = delivery[0] - impressionTarget[0];
					if (impressionTarget[1] > delivery[1]) {
						overDeliveryAmount =  Math.max(correction - impressionTarget[1], 0.0);
						impressionTarget[1] = Math.max(impressionTarget[1] - correction, 0.0);
						overDeliveryAmount += Math.max(delivery[1] - impressionTarget[1], 0.0);
					} else {
						overDelivery = true;
					}
				}

				if (overDelivery) {
					overDeliveryAmount = Math.max(delivery[0] - impressionTarget[0], 0.0) + Math.max(delivery[1] - impressionTarget[1], 0.0);
					cause = String.format("Over delivery: excess delivery: %.0f", overDeliveryAmount);
				} else {
					cause += String.format("now: %.0f, %.0f)", impressionTarget[0], impressionTarget[1]);
					if (overDeliveryAmount > 1000.0) {
						cause += String.format("; excess delivery: %.0f", overDeliveryAmount);
					}
				}
				if (addComments) {
					errors.add(cause);
					errors.log();
				}
			}
		} catch (SQLException e) {
			String cause = String.format("Impression targets not checked for over delivery: %s", e.getMessage());
			errors.add(cause);
			errors.log();
			return;
		}
	}
	
	private void checkForLowerBounds(double[] impressionTarget, double[] maxImpressions) {
		double minRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_RT_IMPRESSION_TARGET, 0), 0.5*(impressionTarget[0] + impressionTarget[1]));
		minRtImpressionTarget = Math.min(minRtImpressionTarget, maxImpressions[0]);
		double minNRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_NON_RT_IMPRESSION_TARGET, 0), 0.5*(impressionTarget[0] + impressionTarget[1]));
		minNRtImpressionTarget = Math.min(minNRtImpressionTarget, maxImpressions[1]);
		
		if (impressionTarget[0] < minRtImpressionTarget && impressionTarget[1] > 2.0 * minRtImpressionTarget) {
			impressionTarget[1] -= (minRtImpressionTarget - impressionTarget[0]);
			impressionTarget[0] = minRtImpressionTarget;
		} else if (impressionTarget[1] < minNRtImpressionTarget && impressionTarget[0] > 2.0 * minNRtImpressionTarget) {
			impressionTarget[0] -= (minNRtImpressionTarget - impressionTarget[1]);
			impressionTarget[1] = minNRtImpressionTarget;
		}
	}
	
	private void adjustBySegmentFactors(double[] impressionTarget, double[] maxImpressions) {
		double minRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_RT_IMPRESSION_TARGET, 0), 0.5*(impressionTarget[0] + impressionTarget[1]));
		minRtImpressionTarget = Math.min(minRtImpressionTarget, maxImpressions[0]);
		double minNRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_NON_RT_IMPRESSION_TARGET, 0), 0.5*(impressionTarget[0] + impressionTarget[1]));
		minNRtImpressionTarget = Math.min(minNRtImpressionTarget, maxImpressions[1]);
		
		double segmentOneFactor = globalParameters.getDouble(Constants.SEGMENT_ONE_FACTOR, 1.0);
		double segmentTwoFactor = Math.max(1.0 - (segmentOneFactor - 1.0) * impressionTarget[0] / (impressionTarget[1] + 1), 0.5);
		
		impressionTarget[0] = (int) Math.round(Math.max(segmentOneFactor * impressionTarget[0], minRtImpressionTarget));
		impressionTarget[1] = (int) Math.round(Math.max(segmentTwoFactor * impressionTarget[1], minNRtImpressionTarget));
	}
	
	private Metric[] toFinalTarget(double[] impressionTarget, double factor, long time) {
		int dayIndex = DateUtils.getIntDayOfWeek(time, 0);
		Metric[] target = new Metric[segmentCount];
		for (int p = 0; p < segmentCount; p++) {
			double impressions = factor * impressionTarget[p];
			double clicks = dailyClickFactor[dayIndex] * clickRatio[p] * impressions;
			double conversions = dailyConversionFactor[dayIndex] * conversionRatio[p] * impressions;
			double cost = 0.001 * averageEcpm[p] * impressions;
			target[p] = new Metric(impressions, clicks, conversions, cost);
		}
		return target;
	}

	public double getTotalConversionTarget() {
		Metric[] target = getImpressionTarget(0, false);
		double conv = 0.0;
		for (int p = 0; p < segmentCount; p++) {
			conv += ratio[p] * target[p].getImpressions();
		}
		return conv;
	}

	public double getKpiRatioTarget() {
		return ratioTarget;
	}
}
