package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.MathUtils;
import com.sharethis.delivery.util.Sort;
import com.sharethis.delivery.util.StringUtils;

public class Segment {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
//	private Parameters campaignParameters;
	private int segmentId;
	private Metric deliveryMetric;
	private Metric targetMetric;
	private Metric auditMetric;
	private List<AdGroup> adGroups;
	protected double clickRatio;
	protected double conversionRatio;
	private double clickAttribution;
	private double conversionAttribution;
	private long time;
	private Errors errors;
	
	public Segment(int segmentId, long time, double clickAttribution, double conversionAttribution) {
		this.segmentId = segmentId;
		this.time = time;
		this.clickAttribution = clickAttribution;
		this.conversionAttribution = conversionAttribution;
		this.adGroups = new ArrayList<AdGroup>();
		deliveryMetric = new Metric();
		auditMetric = new Metric();
		errors = new Errors();
	}
	
	public static Segment parseSegment(ResultSet result, long time, double clickAttribution, double conversionAttribution) throws SQLException {
		Segment segment = new Segment(result.getInt("segmentId"), time, clickAttribution, conversionAttribution);
		segment.parseDeliveryMetric(result);
		segment.parseAuditMetric(result);
		segment.parseTargetMetric(result);
		return segment;
	}

	public void parseDeliveryMetric(ResultSet result) throws SQLException {
		deliveryMetric = new Metric(result.getInt("deliveryImpressions"), clickAttribution * result.getInt("deliveryClicks"), 
				conversionAttribution * result.getDouble("deliveryConversions"), result.getDouble("deliveryCost"));
	}
	
	public void parseAuditMetric(ResultSet result) throws SQLException {
		auditMetric = new Metric(result.getInt("auditImpressions"), clickAttribution * result.getInt("auditClicks"), 
				conversionAttribution * result.getDouble("auditConversions"), result.getDouble("auditCost"));
	}
	
	public void parseTargetMetric(ResultSet result) throws SQLException {
		targetMetric = new Metric(result.getInt("targetImpressions"), clickAttribution * result.getInt("targetClicks"), 
				conversionAttribution * result.getDouble("targetConversions"), result.getDouble("targetCost"));
	}
	
	public void setClickRatio(double clickRatio) {
		this.clickRatio = clickRatio;
	}
	
	public void setConversionRatio(double conversionRatio) {
		this.conversionRatio = conversionRatio;
	}
	
	public void resetImpressionTargets() {
		for (AdGroup adGroup : adGroups) {
			adGroup.resetImpressionTarget();
		}
	}
	
	public Metric getAuditMetric() {
		return auditMetric;
	}
	
	public Metric getDeliveryMetric() {
		return deliveryMetric;
	}

	public Metric getTargetMetric() {
		return targetMetric;
	}
	
	public boolean add(AdGroup newAdGroup) {
		for (AdGroup adGroup : adGroups) {
			if (newAdGroup.equals(adGroup)) {
				errors.add(adGroup.getName() + " already added to the segment");
				return false;
			}
		}
		adGroups.add(newAdGroup);
		return true;
	}

	public boolean readAdGroups(ResultSet result) throws SQLException, ParseException {
		while (result.next()) {
			if (!readAdGroup(result)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean readAdGroup(ResultSet result) throws SQLException, ParseException {
		AdGroup newAdGroup = AdGroup.parseAdGroup(result);
		if (!this.add(newAdGroup)) {
			return false;
		}
		return true;
	}
	
	public boolean addDeliveryMetrics(Map<Long, Metric> deliveryMetrics) {
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			if (deliveryMetrics.containsKey(adGroupId)) {
				Metric metric = deliveryMetrics.get(adGroupId);
				long metricTime = metric.getTime();
				if (time == metricTime) {
					adGroup.setDeliveryMetric(metric);
					deliveryMetric.add(metric);
					log.info(String.format("Impressions: %,9.0f, Clicks: %2.0f, eCPM: %5.2f, Ad group: %s", metric.getImpressions(), metric.getClicks(), metric.getEcpm(), adGroup.getName()));

				} else {
					errors.add(String.format("AdWords time (%s) and segment time (%s) do not match", DateUtils.getDatetime(metricTime), DateUtils.getDatetime(time)));
					errors.log();
					return false;
				}
			} else {
				errors.add(String.format("AdGroupId = %d missing from AdWords stats set", adGroupId));
				errors.log();
				return false;
			}
		}
		return true;
	}
	
	public boolean readPriceVolumeCurves(long startTime, long endTime) throws SQLException, ParseException {
		boolean succeeded = true;
		String errorCause = null;
		String start = DateUtils.getDatetime(startTime);
		String end = DateUtils.getDatetime(endTime);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			String adGroupName = adGroup.getName();
			String query = "SELECT adGroupId, adGroupName, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES
							+ " WHERE adGroupId = " + adGroupId + " AND updateDate >= '" + start + "' AND updateDate < '" + end + "' ORDER BY updateDate DESC";
			ResultSet result = statement.executeQuery(query);
			PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(result, adGroup);
			if (!priceVolumeCurve.isValid() || priceVolumeCurve.getTime() < startTime || priceVolumeCurve.getTime() >= endTime) {
				log.error(String.format("Error in %s: adGroupId = %d: price-volume curve missing or has invalid updateDate", Constants.DO_PRICE_VOLUME_CURVES, adGroupId));
				query = "SELECT adGroupId, adGroupName, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES_BACKUP
						+ " WHERE adGroupId = " + adGroupId + " AND totalInventory >= " + Constants.PV_MIN_VALID_TOTAL_INVENTORY + " ORDER BY updateDate DESC LIMIT 1";
				result = statement.executeQuery(query);
				priceVolumeCurve = new PriceVolumeCurve(result, adGroup);
				if (!priceVolumeCurve.isValid()) {
					String newErrorCause = String.format("%s: %s (%d): no valid backup price-volume curve available", Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupName, adGroupId);
					log.error("Error in " + newErrorCause);
					errorCause = StringUtils.concat(errorCause, newErrorCause);
					priceVolumeCurve = null;
					succeeded = false;
				} else {
					log.info(String.format("Using date from %s for adGroupId = %d: %s", Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupId, DateUtils.getDatetime(priceVolumeCurve.getTime())));
				}
			}
			adGroup.setPriceVolumeCurve(priceVolumeCurve);
		}
		if (succeeded) {
			return true;
		} else {
			errors.add(errorCause);
			return false;
		}
	}
	
	public boolean setDefaultPriceVolumeCurves() throws SQLException, ParseException {
		for (AdGroup adGroup : adGroups) {
			if (adGroup.getPriceVolumeCurve() != null) continue;
			PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());
			adGroup.setPriceVolumeCurve(priceVolumeCurve);
		}
		return true;
	}
/*	
	public boolean readDeliveryImpressions(long startTime, long endTime) throws SQLException, ParseException {
		String start = DateUtils.getDatetime(startTime);
		String end = DateUtils.getDatetime(endTime);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			String query = "SELECT ecpmDelivery, impressionDelivery, clickDelivery, totalInventory, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES
							+ " WHERE adGroupId = " + adGroupId + " AND updateDate >= '" + start + "' AND updateDate < '" + end + "' ORDER BY updateDate DESC";
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				Metric deliveryMetric = Metric.parseDeliveryAdGroupMetric(result, clickAttribution);
				adGroup.setDeliveryMetric(deliveryMetric);
			} else {
				errors.add(String.format("%s: adGroupId = %d: price-volume curve missing or has invalid updateDate", Constants.DO_PRICE_VOLUME_CURVES, adGroupId));
				errors.log();
				return false;
			}
		}
		return true;
	}
*/
	public boolean readDeliveryImpressions(long startTime, long endTime) throws SQLException, ParseException {
		String start = DateUtils.getDatetimeUtc(startTime);
		String end = DateUtils.getDatetimeUtc(endTime);
		Statement statement = DbUtils.getStatement(Constants.URL_RTB_CAMPAIGN_STATISTICS);
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			String query = String.format("SELECT SUM(imprcnt) AS impressions, SUM(clkcnt) AS clicks, 0.001*SUM(cost) AS cost, SUM(nullCostCount) AS costCountCorrection FROM %s WHERE adgId = %d AND date >= '%s' AND date < '%s';",
					Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS, adGroupId, start, end);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				Metric adGroupMetric = Metric.parseDeliveryAdGroupMetric(result, clickAttribution);
				adGroup.setDeliveryMetric(adGroupMetric);
				deliveryMetric.add(adGroupMetric);
			} else {
				errors.add(String.format("%s: adGroupId = %d: missing data or has invalid updateDate", Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS, adGroupId));
				errors.log();
				return false;
			}
		}
		return true;
	}/*
	String start = DateUtils.parseDatetimeUtc(startDate, "00:00:00");
	String end = DateUtils.parseDatetimeUtc(endDate, "23:59:59");
	System.out.println("ShareThis Report");
	Map<Long, Metric> deliveredMetrics = new HashMap<Long, Metric>();
	Statement rtbStatement = DbUtils.getStatement(Constants.URL_RTB_CAMPAIGN_STATISTICS);
	for (Map.Entry<Long, String> entry : adGroupIds.entrySet()) {
		long adGroupId = entry.getKey();
		String adGroupName = entry.getValue();
		String query = String.format("SELECT SUM(imprcnt) AS impressions, SUM(clkcnt) AS clicks, 0.001*SUM(cost) AS cost FROM %s WHERE adgId = %d AND date >= '%s' AND date < '%s';",
				Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS, adGroupId, start, end);
		ResultSet result = rtbStatement.executeQuery(query);
		if (result.next()) {
			double impressions = (double) result.getInt("impressions");
			double clicks = (double) result.getInt("clicks");
			double cost = result.getDouble("cost");
			Metric metric = new Metric(impressions, clicks, 0.0, cost);
			metric.setTime(0L);
			deliveredMetrics.put(adGroupId, metric);
		} else {
			throw new SQLException(String.format("adGroup = %s (%d) returns no rows from %s", adGroupName, adGroupId,
					Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS));
		}
	}
	return deliveredMetrics;
	*/
	public boolean readCurrentDelivery() throws SQLException {
		boolean freshData = true;
		for (AdGroup adGroup : adGroups) {
			freshData &= adGroup.readCurrentDelivery(time);
		}
		return freshData;
	}
	
	public double getCurrentImpressionDelivery() throws SQLException {
		if(!readCurrentDelivery()) {
			errors.add("Segment " + segmentId + ": rtbStats data stale");
			errors.log();
		}
		double imps = 0.0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getDeliveryMetric().getImpressions();
		}
		return imps;
	}
/*	
	public double getProjectedImpressionOverDelivery() throws SQLException {
		if(!readCurrentDelivery()) {
			errors.add("Segment " + segmentId + ": rtbStats data stale");
			errors.log();
		}
		double imps = 0.0;
		for (AdGroup adGroup : adGroups) {
			imps += Math.max(0.0, adGroup.getDeliveryMetric().getImpressions() - adGroup.getTargetMetric().getImpressions());
		}
		return imps;
	}
*/	
	public boolean hasAdGroup(String adGroupName) {
		for (AdGroup adGroup : adGroups) {
			if (adGroup.equals(adGroupName)) {
				return true;
			}
		}
		return false;
	}
	
	public List<AdGroup> getAdGroups() {
		return adGroups;
	}
	
	public Map<Long, String> getAdGroupIds() {
		Map<Long, String> adGroupIds = new HashMap<Long, String>();
		for (AdGroup adGroup : adGroups) {
			adGroupIds.put(adGroup.getId(), adGroup.getName());
		}
		return adGroupIds;
	}
	
	public int size() {
		return adGroups.size();
	}

	public int getMaxImpressions() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getPriceVolumeCurve().getMaxImpressions();
		}
		return imps;
	}
	
	public int getDeliveryImpressions() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getDeliveryMetric().getImpressions();
		}
		return imps;
	}
	
	public int getDeliveryClicks() {
		int clks = 0;
		for (AdGroup adGroup : adGroups) {
			clks += adGroup.getDeliveryMetric().getClicks();
		}
		return clks;
	}
	
	public double getDeliveryEcpm() {
		double cost = 0.0;
		double imps = 0;
		for (AdGroup adGroup : adGroups) {
			cost += adGroup.getDeliveryMetric().getCost();
			imps += adGroup.getDeliveryMetric().getImpressions();
		}
		return (imps == 0 ? 0.0 : 1000.0 * cost / imps);
	}

	public double getTargetEcpm() {
		double cost = 0.0;
		double imps = 0;
		for (int i = 0; i < adGroups.size(); i++) {
			AdGroup adGroup = adGroups.get(i);
			cost += adGroup.getTargetMetric().getCost();
			imps += adGroup.getTargetMetric().getImpressions();
		}
		return (imps == 0 ? 0.0 : 1000.0 * cost / imps);
	}
	
	public double getTotalMaxCpm() {
		double maxCpm = 0.0;
		for (AdGroup adGroup : adGroups) {
			maxCpm = Math.max(maxCpm, adGroup.getMaxCpm());
		}
		return maxCpm;
	}
	
	public int getTotalImpressionTarget() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getTargetMetric().getImpressions();
		}
		return imps;
	}
	
	public int getTotalNextImpressionTarget() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getNextTargetMetric().getImpressions();
		}
		return imps;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public long getTime() {
		return time;
	}
	
	public long getId() {
		return segmentId;
	}

	public Errors getErrors() {
		return errors;
	}
	
	public boolean hasErrors() {
		return (!errors.isEmpty());
	}
	
	public void setImpressionTargets(Metric impressionTarget, Metric nextImpressionTarget, String comment) {
		targetMetric = impressionTarget;
		double[] targets = this.getImpressionTargets(impressionTarget.getImpressions());
		double[] nextTargets = this.getImpressionTargets(nextImpressionTarget.getImpressions());
		for (int i = 0; i < adGroups.size(); i++) {
			Metric metric = new Metric(targets[i]);
			Metric nextMetric = new Metric(nextTargets[i]);
			adGroups.get(i).setTargetMetric(metric);
			adGroups.get(i).setNextTargetMetric(nextMetric);
			adGroups.get(i).setComment(comment);
		}
		double cost = 0.0;
		for (AdGroup adGroup : adGroups) {
			cost += adGroup.getTargetCost();
		}
		targetMetric.setCost(cost);
	}
	
	private double[] getImpressionTargets(double totalImpressions) {
		int n = adGroups.size();
		double[] targets = new double[n];
		
		if (totalImpressions == 0) {
			return targets;
		}
		
		if (n == 1) {
			targets[0] = totalImpressions;
			return targets;
		}
		
		int[] maxImp = new int[n];
		double imps = 0.0;
		for (int i = 0; i < n; i++) {
			maxImp[i] = adGroups.get(i).getPriceVolumeCurve().getMaxImpressions();
			imps += (double) maxImp[i];
		}
		
		if (totalImpressions >= imps) {
			for (int i = 0; i < n; i++) {
				targets[i] = Math.round(totalImpressions * (double) maxImp[i] / imps);
			}
		} else {
			int[] index = new int[n];
			int cumPriority = 0;
			for (int i = 0; i < n; i++) {
				int priority = adGroups.get(i).getPriority();
				cumPriority += priority;
				index[priority] = i;
			}
			if (cumPriority > 0) {
				for (int j = 0; j < n; j++) {
					int i = index[j];
					if (maxImp[i] >= totalImpressions) {
						targets[i] = totalImpressions;
						break;
					} else {
						targets[i] = maxImp[i];
						totalImpressions -= maxImp[i];
					}
				}
			} else {
				Map<Integer, Integer> capacity = new HashMap<Integer, Integer>();
				for (int i = 0; i < n; i++) {
					capacity.put(i, maxImp[i]);
				}
				List<Integer> order = Sort.byValue(capacity, "asc");
				int m = 0;
				int i = 0;
				while (m < n) {
					double impsPerAdGroup = totalImpressions / (double) (n - m);
					i = order.get(m);
					targets[i] = Math.min(impsPerAdGroup, maxImp[i]);
					totalImpressions -= targets[i];
					m++;
				}
				if (totalImpressions > 0) {
					targets[i] += totalImpressions;
				}
			}
		}
		return targets;
	}
	
	public int writeTargetSegment(long campaignRecordId) throws SQLException {
		double targetImpressions = targetMetric.getImpressions();
		double targetClicks = MathUtils.round(targetMetric.getClicks() / clickAttribution, 1);
		double targetConversions = MathUtils.round(targetMetric.getConversions() / conversionAttribution, 1);
		double targetCost = targetMetric.getCost();
		double maxCpm = getTotalMaxCpm();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("INSERT INTO %s (campaignRecordId,segmentId,targetImpressions,targetClicks,targetConversions,targetCost,deliveryClicks,deliveryConversions,maxCpm) VALUES (%d,%d,%.0f,%f,%f,%f,%f,%f,%f);", 
				Constants.DO_SEGMENT_PERFORMANCES, campaignRecordId, segmentId, targetImpressions, targetClicks, targetConversions, targetCost, targetClicks, targetConversions, maxCpm);
		int writtenRecords = statement.executeUpdate(query);
		return writtenRecords;
	}
	
	public int updateSegmentTable(long campaignRecordId) throws SQLException {
		double deliveryImpressions = deliveryMetric.getImpressions();
		double deliveryClicks = deliveryMetric.getClicks() / clickAttribution;
		double deliveryConversions = deliveryMetric.getConversions() / conversionAttribution;
		double deliveryCost = deliveryMetric.getCost();
		double targetClicks = MathUtils.round(clickRatio * deliveryImpressions / clickAttribution, 1);
		double targetConversions = MathUtils.round(conversionRatio * deliveryImpressions / conversionAttribution, 1);
		double auditImpressions = auditMetric.getImpressions();
		double auditClicks = auditMetric.getClicks() / clickAttribution;
		double auditConversions = auditMetric.getConversions() / conversionAttribution;
		double auditCost = auditMetric.getCost();
		String query = String.format("UPDATE %s SET deliveryImpressions=%f,auditImpressions=%f,targetClicks=%f,deliveryClicks=%f,auditClicks=%f,targetConversions=%f,deliveryConversions=%f,auditConversions=%f,deliveryCost=%f,auditCost=%f,clickRatio=%f,conversionRatio=%f WHERE campaignRecordId=%d AND segmentId=%d;",
				Constants.DO_SEGMENT_PERFORMANCES, deliveryImpressions, auditImpressions, targetClicks, deliveryClicks, auditClicks,
				targetConversions, deliveryConversions, auditConversions, deliveryCost, auditCost, clickRatio, conversionRatio, campaignRecordId, segmentId);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		return statement.executeUpdate(query);
	}
	
	public int writeImpressionTargets(Parameters campaignParameters) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		long campaignId = campaignParameters.getLong(Constants.CAMPAIGN_ID, 0);
		long goalId = campaignParameters.getInt(Constants.GOAL_ID, 0);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = Constants.globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		boolean isEndOfCampaignPeriod = (time + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime);
		boolean manageAtf = (campaignParameters.getLong(Constants.MANAGE_ATF, 0) == 1);

		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		long tomorrow = DateUtils.getTime(time, 1);
		double dayLength = (double) (tomorrow - time) / (double) Constants.MILLISECONDS_PER_HOUR;
		double numberOfIntervals = Math.round(dayLength / deliveryInterval);
		deliveryInterval = dayLength / numberOfIntervals;
		String datetime = DateUtils.getDatetime(time);
		
		int adGroupAtfFactor = Constants.globalParameters.getInt(Constants.AD_GROUP_ATF_FACTOR, Integer.MAX_VALUE);
		
		int deliveredAdGroups = 0;
		for (AdGroup adGroup : adGroups) {
			int atf = (manageAtf ? (isEndOfCampaignPeriod ? 0 : adGroup.getAtf(adGroupAtfFactor)) : -1);
			String query = String.format("INSERT INTO %s (campaignId,goalId,segmentId,adGroupId,adGroupName,currentImpressionTarget,currentMaxCpm,nextImpressionTarget,atf,date,deliveryInterval,comment) ",
					Constants.DO_IMPRESSION_TARGETS)
			+ String.format("VALUES (%d,%d,%d,%d,'%s',%.0f,%f,%.0f,%d,'%s',%f,'%s');", campaignId, goalId, segmentId, adGroup.getId(), adGroup.getName(),
					adGroup.getTargetMetric().getImpressions(), adGroup.getMaxCpm(), adGroup.getNextTargetMetric().getImpressions(), atf, datetime, deliveryInterval,
					adGroup.getComment());
			deliveredAdGroups += statement.executeUpdate(query);
		}
		
		return deliveredAdGroups;
	}
	
	public int writeDeliveryMetric(long campaignRecordId) throws SQLException {
		double deliveryImpressions = deliveryMetric.getImpressions();
		double deliveryClicks = deliveryMetric.getClicks();
		double deliveryConversions = deliveryMetric.getConversions();
		double deliveryCost = deliveryMetric.getCost();
		String query = String.format("UPDATE %s SET deliveryImpressions=%.0f,deliveryClicks=%f,deliveryConversions=%f,deliveryCost=%f WHERE campaignRecordId=%d AND segmentId=%d;",
				Constants.DO_SEGMENT_PERFORMANCES, deliveryImpressions, deliveryClicks, deliveryConversions, deliveryCost, campaignRecordId, segmentId);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		return statement.executeUpdate(query);
	}
}
