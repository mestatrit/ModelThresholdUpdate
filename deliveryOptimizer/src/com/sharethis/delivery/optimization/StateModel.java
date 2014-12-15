package com.sharethis.delivery.optimization;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.estimation.GaussianDistribution;
import com.sharethis.delivery.estimation.StretchedExponentialDistribution;
import com.sharethis.delivery.estimation.PoissonDistribution;
import com.sharethis.delivery.record.DeliveryRecord;
import com.sharethis.delivery.record.PerformanceRecord;
import com.sharethis.delivery.solvers.SolverException;
import com.sharethis.delivery.util.DateUtils;

public class StateModel {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	
	private Segment[] segments;
	private Parameters campaignParameters;
	private Parameters globalParameters;
	private StrategyType strategy;
	private long currentTime;
	private String[] comments;
	private int segmentSize;
	private int modulate, day;
	
	private double totalImpressionTarget;
	private double conversionRatioTarget;
	
	private double[] campaignSpent;
	private int[] campaignImpressions;
	private double[] averageEcpm;
	
	private double[] conversionRatio;
	private double[] ao;
	private double a1p, a2p;
	private double i1, i2, i1next, i2next;
	private double m11, m12, m21, m22, singularity;

	private double[] dailyConversionFactor;
	
	private List<Double> hi1, hi2, hc, hs1, hs2;
	private List<Boolean> hm;
	
	public StateModel(Parameters globalParameters, Parameters campaignParameters, double priorWeight, Segment[] segments, StrategyType strategy) {
		segmentSize = campaignParameters.getInt(Constants.SEGMENTS, 0);
		modulate = campaignParameters.getInt(Constants.MODULATE, 0);
		this.globalParameters = globalParameters;
		this.campaignParameters = campaignParameters;
		this.segments = segments;
		this.strategy = strategy;
		
		day = 0;
		conversionRatio = new double[segmentSize];
		for (int i = 0; i < segmentSize; i++) {
			conversionRatio[i] = campaignParameters.getDouble(String.format("%s%d", Constants.CONVERSION_RATIO, i + 1), 2.0e-5);
		}

		a1p = conversionRatio[0];
		a2p = conversionRatio[1];
		ao = new double[] {a1p, a2p};
		
		m11 = m22 = priorWeight;
		m12 = m21 = 0;

		campaignSpent = new double[segmentSize];
		averageEcpm = new double[segmentSize];
		campaignImpressions = new int[segmentSize];
		for (int i = 0; i < segmentSize; i++) {
			campaignImpressions[i] = 0;
			campaignSpent[i] = 0.0;
		}
		
		dailyConversionFactor = new double[Constants.WEEKDAYS];
		for (int i = 0; i < Constants.WEEKDAYS; i++) {
			dailyConversionFactor[i] = Constants.DAILY_CONVERSION_FACTOR[i];
		}
		
		hi1 = new ArrayList<Double>();
		hi2 = new ArrayList<Double>();
		hc  = new ArrayList<Double>();
		hs1 = new ArrayList<Double>();
		hs2 = new ArrayList<Double>();
		hm  = new ArrayList<Boolean>();
	}
	
	public void setDailyConversionFactor(double[] dailyConversionFactor) {
		this.dailyConversionFactor = dailyConversionFactor;
	}

	public void optimize(double totalConversionTarget, double totalImpressionTarget, double impressionExcess, long currentTime) {
		totalConversionTarget = Math.max(totalConversionTarget, 0.0);
		totalImpressionTarget = Math.max(totalImpressionTarget, 0.0);		
		this.currentTime = currentTime;
		this.totalImpressionTarget = totalImpressionTarget;
		comments = new String[segmentSize];
		for (int i = 0; i < segmentSize; i++) {
			comments[i] = "";
		}

		day++;
		double ip1max = Math.min(totalImpressionTarget, (double) segments[0].getMaxImpressions());
		double ip2max = Math.min(totalImpressionTarget, (double) segments[1].getMaxImpressions());
		
		if (totalConversionTarget > 0) {
			if (totalImpressionTarget < 1) totalImpressionTarget = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
			conversionRatioTarget = totalConversionTarget / totalImpressionTarget;
		} else {
			conversionRatioTarget = 0.0;
			i1 = 0.0;
			i2 = ip2max;
			i1next = i1;
			i2next = i2;
			return;
		}
		
		double[] costPerConversion = new double[segmentSize];
		if (averageEcpm[0] == 0 || averageEcpm[1] == 0) {
			averageEcpm[0] = averageEcpm[1] = 1.0;
		}
		costPerConversion[0] = 0.001 * averageEcpm[0] / Math.max(a1p, 1.0e-7);
		costPerConversion[1] = 0.001 * averageEcpm[1] / Math.max(a2p, 1.0e-8);
		
		if (a1p >= conversionRatioTarget && a2p >= conversionRatioTarget) {
			if (costPerConversion[0] < costPerConversion[1]) {
				i1 = ip1max;
				i2 = Math.max(0.0, Math.min(totalImpressionTarget - ip1max, ip2max));
				comments[0] = "Exceeds conversion goal";
				if (i2 > 0) comments[1] = "Exceeds conversion goal";
			} else {
				i1 = 0.0;
				i2 = ip2max;
				comments[1] = "Exceeds conversion goal";
			}
		} else if (a1p <= conversionRatioTarget && a2p <= conversionRatioTarget) {
			if (a1p >= a2p) {
				i1 = ip1max;
				i2 = Math.max(0.0, Math.min(totalImpressionTarget - ip1max, ip2max));
				comments[0] = "Does not meet conversion goal";
				if (i2 > 0) comments[1] = "Does not meet conversion goal";
			} else {
				i1 = 0.0;
				i2 = ip2max;
				comments[1] = "Does not meet conversion goal";
			}
		} else {
			if (a1p > a2p) {
				i1 = totalImpressionTarget * (conversionRatioTarget - a2p) / (a1p - a2p);
				i2 = totalImpressionTarget * (a1p - conversionRatioTarget) / (a1p - a2p);
			} else if (a1p < a2p) {
				i1 = totalImpressionTarget * (a2p - conversionRatioTarget) / (a2p - a1p);
				i2 = totalImpressionTarget * (conversionRatioTarget - a1p) / (a2p - a1p);
			} else {
				i1 = 0;
				i2 = totalImpressionTarget;
			}
			if (i1 > ip1max) {
				comments[0] = String.format("Does not meet conversion goal: impression target (%,.0f) exceeds daily max (%,.0f)",
								i1, ip1max);
			}
			if (i2 > ip2max) {
				comments[1] = String
						.format("Does not meet conversion goal: impression target (%,.0f) exceeds daily max (%,.0f)",
								i2, ip2max);
			}
		}
		
		final double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		final double minMargin = campaignParameters.getDouble(Constants.MIN_MARGIN, 0.0);
		final double impressionsPerInterval = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
		final double spreadLimit = 0.5;
		
		double predictedImpressions = i1 + i2;
		double predictedEcpm1 = segments[0].getPredictedEcpm(i1);
		double predictedEcpm2 = segments[1].getPredictedEcpm(i2);
		double predictedSpread = Math.abs(predictedEcpm1 - predictedEcpm2);
		
		double totalSpent = 0.001 * predictedEcpm1 * i1 + campaignSpent[0] + 0.001 * predictedEcpm2 * i2 + campaignSpent[1];
		double totalImpressions = i1 + campaignImpressions[0] + i2 + campaignImpressions[1];
		double totalMargin = (totalImpressions < 1 ? 100.0 : 1.0 - totalSpent / (0.001 * cpm * totalImpressions));

		if (predictedSpread > spreadLimit && totalMargin < minMargin && predictedImpressions > 0.1 * impressionsPerInterval && totalImpressions > 3.0 * impressionsPerInterval) {
			double margin = (i1 * (cpm - predictedEcpm1) + i2 * (cpm - predictedEcpm2)) / (cpm * (i1 + i2));
			if (margin < minMargin) {
				double minMarginRatio = ((1.0 - minMargin)*cpm - predictedEcpm2) / predictedSpread;
				minMarginRatio = Math.min(1.0, Math.max(0.0, minMarginRatio));
		
				double i1old = i1;
				double i2old = i2;
				i1 = minMarginRatio * predictedImpressions;
				i2 = (1.0 - minMarginRatio) * predictedImpressions;
				
				if (comments[0].length() == 0) {
					comments[0] = String.format("Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i1old), Math.round(i1));
				} else {
					comments[0] += String.format("; Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i1old), Math.round(i1));
				}
				if (comments[1].length() == 0) {
					comments[1] = String.format("Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i2old), Math.round(i2));
				} else {
					comments[1] += String.format("; Impression alloctation violates margin limit (%.1f): %d -> %d", margin, Math.round(i2old), Math.round(i2));
				}
			}
		}
		
		i1next = i1;
		i2next = i2;
		
		if (modulate > 0 && day >= modulate) {
			int delta = day - modulate;
			if (delta % 2 == 0) {
				i1 = 0.0;
				i2 = ip2max;
				comments[0] = "Delivery target set to zero per modulation plan";
				comments[1] = "Delivery target set to max per modulation plan";
			} else {
				i1next = 0;
			}
		}
		
		int[] impressionTarget = getImpressionTarget();
		int[] nextImpressionTarget = getNextImpressionTarget();
		for (int i = 0; i < segments.length; i++) {
			segments[i].setImpressionTargets(impressionTarget[i], nextImpressionTarget[i]);
		}
	}

	public void updateState(PerformanceRecord record, boolean isCurrentCampaign) {
		if (isCurrentCampaign) {
			for (int i = 0; i < segmentSize; i++) {
				campaignSpent[i] += record.getSpent(i);
				campaignImpressions[i] += record.getDeliveredImpressions(i);
			}
		}

		if (record.isPaused()) {
			record.setEstimatedConversionRatios(a1p, a2p, 0.0);
			return;
		}
		
		double i1 = record.getDeliveredImpressions(0);
		double i2 = record.getDeliveredImpressions(1);
		double c = record.getTotalDeliveredConversions();
		double s1 = record.getSpent(0);
		double s2 = record.getSpent(1);
		
		if (i1 + i2 > 5000.0) {
			hi1.add(i1);
			hi2.add(i2);
			hc.add(c);
			hs1.add(s1);
			hs2.add(s2);
			hm.add(isCurrentCampaign);
		}
		
		try {
//			double[] ap = StretchedExponentialLikelihood.maximizeLikelihood(hi1, hi2, hc, ao);
			double[] ap = GaussianDistribution.maximizeLikelihood(hi1, hi2, hc, ao);
			record.setEstimatedConversionRatios(ap[0], ap[1], ap[2]);
			a1p = ap[0];
			a2p = ap[1];
			double ci1 = GaussianDistribution.average(hi1);
			double ci2 = GaussianDistribution.average(hi2);
			averageEcpm[0] = 1000.0 * (ci1 < 1 ? 0.0 : GaussianDistribution.average(hs1) / ci1);
			averageEcpm[1] = 1000.0 * (ci2 < 1 ? 0.0 : GaussianDistribution.average(hs2) / ci2);
		} catch (SolverException e) {
			log.error(e.getMessage());
			record.setEstimatedConversionRatios(a1p, a1p, -1.0);
		}
	}
	
	public void old_updateState(PerformanceRecord record) {
		for (int i = 0; i < segmentSize; i++) {
//			cumulativeSpent[i] += record.getSpent(i);
			campaignImpressions[i] += record.getDeliveredImpressions(i);
			averageEcpm[i] = (campaignImpressions[i] == 0 ? 0.0 : 1000.0 * campaignSpent[i] / campaignImpressions[i]);
		}

		double i1 = record.getDeliveredImpressions(0);
		double i2 = record.getDeliveredImpressions(1);

		if (record.isPaused()) {
			record.setEstimatedConversionRatios(a1p, a2p, 0.0);
			return;
		}
		double dt = record.getTotalDeliveredConversions();

		double t_correction = dt - (a1p * i1 + a2p * i2);
		m11 += i1 * i1;
		m12 += i1 * i2;
		m21 += i1 * i2;
		m22 += i2 * i2;

		double det = m11 * m22 - m12 * m21;
		double m11inv = m22 / det;
		double m22inv = m11 / det;
		double m12inv = -m12 / det;
		double m21inv = -m21 / det;
		singularity = (m11 + m22 + Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21))
				/ (m11 + m22 - Math.sqrt((m11 - m22) * (m11 - m22) + 4.0 * m12 * m21));

		double da1p = (m11inv * i1 + m12inv * i2) * t_correction;
		double da2p = (m21inv * i1 + m22inv * i2) * t_correction;

		a1p = Math.max(a1p + da1p, 0.0);
		a2p = Math.max(a2p + da2p, 0.0);
		record.setEstimatedConversionRatios(a1p, a2p, singularity);
	}
	
	private double getDeliveryFactor(long currentTime, int addDays) {
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		double endOfCampaignDeliveryMargin = campaignParameters.getDouble(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN, 0.1);
		double campaignDeliveryMargin = campaignParameters.getDouble(Constants.CAMPAIGN_DELIVERY_MARGIN, 0.1);
		double impressionsPerInterval = campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0);
		if (currentTime + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime) {
			if (totalImpressionTarget < 1) {
				return strategy.getDeliveryFactor(currentTime, addDays);
			} else {
				return (1.0 + endOfCampaignDeliveryMargin * impressionsPerInterval / totalImpressionTarget) * strategy.getDeliveryFactor(currentTime, addDays);
			}
		} else {
			if (DateUtils.isWeekend(currentTime, addDays) && !strategy.isUniformDelivery()) {
				campaignDeliveryMargin = 0;
			}
			return (1.0 + campaignDeliveryMargin) * strategy.getDeliveryFactor(currentTime, addDays);
		}
	}
	
	public int[] getImpressionTarget() {
		double segmentOneFactor = globalParameters.getDouble(Constants.SEGMENT_ONE_FACTOR, 1.0);
		double deliveryFactor = this.getDeliveryFactor(currentTime, 0);
		double ip1max = (double) segments[0].getMaxImpressions();
		double ip2max = (double) segments[1].getMaxImpressions();
		int[] impressionTarget = new int[segmentSize];
		if (strategy.isFixedDelivery()) {
			double i1cap = strategy.getRtImpressions();//Math.min(strategy.getRtImpressions(), ip1max);
			i1cap = Math.min(deliveryFactor * totalImpressionTarget, i1cap);
			double i2cap =strategy.getNrtImpressions();//Math.min(strategy.getNrtImpressions(), ip2max);
			i2cap = Math.min(deliveryFactor * totalImpressionTarget - i1cap, i2cap);
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
			comments[0] = "Delivery target set to user defined value or max capacity";
//			comments[1] = "Delivery target set to user defined value or max capacity";
		} else if (strategy.isMaxRt()) {
			double i1cap = Math.min(deliveryFactor * totalImpressionTarget, ip1max);
			double i2cap = Math.max(deliveryFactor * totalImpressionTarget - i1cap, 0.0);
			double minRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_RT_IMPRESSION_TARGET, 0), 0.5*(i1cap + i2cap));
			minRtImpressionTarget = Math.min(minRtImpressionTarget, ip1max);
			double minNRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_NON_RT_IMPRESSION_TARGET, 0), 0.5*(i1cap + i2cap));
			minNRtImpressionTarget = Math.min(minNRtImpressionTarget, ip2max);
			if (i1cap < minRtImpressionTarget && i2cap > 2.0 * minRtImpressionTarget) {
				i2cap -= (minRtImpressionTarget - i1cap);
				i1cap = minRtImpressionTarget;
			} else if (i2cap < minNRtImpressionTarget && i1cap > 2.0 * minNRtImpressionTarget) {
				i1cap -= (minNRtImpressionTarget - i2cap);
				i2cap = minNRtImpressionTarget;
			}
			double segmentTwoFactor = Math.max(1.0 - (segmentOneFactor - 1.0) * i1cap / (i2cap + 1), 0.5);
			impressionTarget[0] = (int) Math.round(Math.max(segmentOneFactor * i1cap, minRtImpressionTarget));
			impressionTarget[1] = (int) Math.round(Math.max(segmentTwoFactor * i2cap, minNRtImpressionTarget));
		} else if (strategy.isFixedRatio()) {
			double i1cap = Math.min(strategy.getRatio() * deliveryFactor * totalImpressionTarget, ip1max);
			double i2cap = Math.max(deliveryFactor * totalImpressionTarget - i1cap, 0.0);
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		} else {
			double i1cap = deliveryFactor * i1;
			double i2cap = deliveryFactor * i2;
			if (i1cap > ip1max) {
				i2cap = Math.min(i2cap + i1cap - ip1max, ip2max);
				i1cap = ip1max;
			} else if (i2cap > ip2max) {
				i1cap = Math.min(i1cap + i2cap - ip2max, ip1max);
				i2cap = ip2max;
			}
			double deficit = Math.max(deliveryFactor * totalImpressionTarget - i1cap - i2cap, 0);
			i2cap = Math.max(i2cap + deficit, 0.0);
			double minRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_RT_IMPRESSION_TARGET, 0), 0.5*(i1cap + i2cap));
			minRtImpressionTarget = Math.min(minRtImpressionTarget, ip1max);
			double minNRtImpressionTarget = Math.min((double) globalParameters.getInt(Constants.MIN_NON_RT_IMPRESSION_TARGET, 0), 0.5*(i1cap + i2cap));
			minNRtImpressionTarget = Math.min(minNRtImpressionTarget, ip2max);
			if (i1cap < minRtImpressionTarget && i2cap > 2.0 * minRtImpressionTarget) {
				i2cap -= (minRtImpressionTarget - i1cap);
				i1cap = minRtImpressionTarget;
			} else if (i2cap < minNRtImpressionTarget && i1cap > 2.0 * minNRtImpressionTarget) {
				i1cap -= (minNRtImpressionTarget - i2cap);
				i2cap = minNRtImpressionTarget;
			}
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		}
		return impressionTarget;
	}
	
	public int getTotalConversionTarget() {
		int day = DateUtils.getIntDayOfWeek(currentTime, 0);
		int[] impressionTarget = getImpressionTarget();
		return (int) Math.round(dailyConversionFactor[day] * (a1p * impressionTarget[0] + a2p * impressionTarget[1]));
	}
	
	public int getTotalConversionTarget(PerformanceRecord record) {
		int day = DateUtils.getIntDayOfWeek(currentTime, 0);
		return (int) Math.round(dailyConversionFactor[day] * (a1p * record.getDeliveredImpressions(0) + a2p * record.getDeliveredImpressions(1)));
	}
	
	public double getConversionRatioTarget() {
		return conversionRatioTarget;
	}
	
	public int[] getNextImpressionTarget() {
		int[] impressionTarget = new int[segmentSize];
		double segmentOneFactor = globalParameters.getDouble(Constants.SEGMENT_ONE_FACTOR, 1.0);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		long nextTime = currentTime + 24L * Constants.MILLISECONDS_PER_HOUR + 10L;
		if (nextTime > endTime) {
			impressionTarget[0] = impressionTarget[1] = 0;
			return impressionTarget;
		}
		double deliveryFactor = this.getDeliveryFactor(currentTime, 1);
		double ip1max = (double) segments[0].getMaxImpressions();
		double ip2max = (double) segments[1].getMaxImpressions();//(double) priceVolumeCurve.getMaxImpressions(1);
		if (strategy.isFixedDelivery()) {
			double i1cap = strategy.getRtImpressions();//Math.min(strategy.getRtImpressions(), ip1max);
			i1cap = Math.min(deliveryFactor * totalImpressionTarget, i1cap);
			double i2cap = strategy.getNrtImpressions();//Math.min(strategy.getNrtImpressions(), ip2max);
			i2cap = Math.min(deliveryFactor * totalImpressionTarget - i1cap, i2cap);
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		} else if (strategy.isMaxRt()) {
			double i1cap = Math.min(deliveryFactor * totalImpressionTarget, ip1max);
			double i2cap = Math.max(deliveryFactor * totalImpressionTarget - i1cap, 0.0);
			impressionTarget[0] = (int) Math.round(segmentOneFactor * i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		} else if (strategy.isFixedRatio()) {
			double i1cap = Math.min(strategy.getRatio() * deliveryFactor * totalImpressionTarget, ip1max);
			double i2cap = Math.max(deliveryFactor * totalImpressionTarget - i1cap, 0.0);
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		} else {
			double i1cap = deliveryFactor * i1next;
			double i2cap = deliveryFactor * i2next;
			if (i1cap > ip1max) {
				i2cap = Math.min(i2cap + i1cap - ip1max, ip2max);
				i1cap = ip1max;
			} else if (i2cap > ip2max) {
				i1cap = Math.min(i1cap + i2cap - ip2max, ip1max);
				i2cap = ip2max;
			}
			double deficit = Math.max(deliveryFactor * totalImpressionTarget - i1cap - i2cap, 0.0);
			i2cap = Math.max(i2cap + deficit, 0.0);
			impressionTarget[0] = (int) Math.round(i1cap);
			impressionTarget[1] = (int) Math.round(i2cap);
		}
		
		double factor = globalParameters.getDouble(Constants.NEXT_DAY_IMPRESSION_FACTOR, 0.0);
		impressionTarget[0] = (int) Math.round(factor * impressionTarget[0]);
		impressionTarget[1] = (int) Math.round(factor * impressionTarget[1]);
		
		return impressionTarget;
	}

	public DeliveryRecord getDeliveryRecord() {
		return new DeliveryRecord(currentTime, segments, comments);
	}
	
	public PerformanceRecord getPerformanceRecord() {
		return new PerformanceRecord(campaignParameters, this);
	}
	
	public long getTime() {
		return currentTime;
	}
	
	public double[] getMaxCpm() {
		double[] maxCpm = new double[segments.length];
		for (int i = 0; i < segments.length; i++) {
			maxCpm[i] = segments[i].getTotalMaxCpm();
		}
		return maxCpm;
	}
}
