package com.sharethis.delivery.base;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.MathUtils;

public class State {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	protected long id;
	protected long time;
	protected int segmentCount;
	protected List<Segment> segments;
	protected Parameters campaignParameters;
	protected int updateSegmentCount;
	protected double clickAttribution;
	protected double conversionAttribution;
	protected StatusType status;

	protected Errors errors;

	@SuppressWarnings("unused")
	private State() {
		segmentCount = 0;
		segments = new ArrayList<Segment>();
		status = new StatusType();
	}

	public State(long id, long time, int segmentCount, Parameters campaignParameters) {
		this.id = id;
		this.time = time;
		this.segmentCount = segmentCount;
		this.campaignParameters = campaignParameters;
		this.segments = new ArrayList<Segment>(segmentCount);
		clickAttribution = conversionAttribution = 1.0;
		errors = new Errors();
		status = new StatusType();
	}

	public State(Parameters campaignParameters) {
		this.segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		this.segments = new ArrayList<Segment>(segmentCount);
		this.campaignParameters = campaignParameters;

		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		double attribution = campaignParameters.getDouble(Constants.KPI_ATTRIBUTION_FACTOR, 1.0);
		clickAttribution = (campaignType.isCpc() ? attribution : 1.0);
		conversionAttribution = (!campaignType.isCpc() ? attribution : 1.0);
		errors = new Errors();
		status = new StatusType();
	}

	public static State parseState(ResultSet result, Parameters campaignParameters) throws SQLException {
		State state = new State(campaignParameters);
		state.id = result.getLong("id");
		state.time = DateUtils.parseTime(result, "date");
		state.status = StatusType.parseStatus(result);
		return state;
	}

	public boolean parseLogicalState(ResultSet result) throws SQLException {
		result.beforeFirst();
		segments.clear();
		while (result.next()) {
			Segment segment = Segment.parseSegment(result, time, clickAttribution, conversionAttribution);
			segments.add(segment);
		}
		if (segments.size() == segmentCount) {
			return true;
		} else {
			errors.add("Segment count mismatch: read " + segments.size() + ", should have: " + segmentCount);
			return false;
		}
	}

	public boolean parsePhysicalState(ResultSet result) throws SQLException, ParseException {
		result.beforeFirst();
		segments.clear();
		for (int segmentId = 0; segmentId < segmentCount; segmentId++) {
			Segment segment = new Segment(segmentId, time, clickAttribution, conversionAttribution);
			segments.add(segment);
		}
		while (result.next()) {
			int segmentId = result.getInt("segment");
			Segment segment = segments.get(segmentId);
			if (!segment.readAdGroup(result)) {
				errors = segment.getErrors();
				return false;
			}
		}
		for (Segment segment : segments) {
			int count = segment.size();
			if (count == 0) {
				errors.add("No ad groups: segmentId = " + segment.getId());
				errors.log();
				// return false;
			}
		}
		return true;
	}

	public void setEstimatorStatus() {
		status.setEstimator();
	}

	public void setImpressionStatus() {
		status.setImpression();
	}

	public void setKpiStatus() {
		status.setKpi();
	}

	public void setAuditStatus() {
		status.setAudit();
	}

	public boolean getKpiStatus() {
		return status.getKpi();
	}

	public long getId() {
		return id;
	}

	public Errors getErrors() {
		Errors errors2 = errors;
		errors = new Errors();
		return errors2;
	}

	public long getTime() {
		return time;
	}

	public Segment getSegment(int i) {
		return segments.get(i);
	}

	public List<Segment> getSegments() {
		return segments;
	}

	public Metric getDeliveredMetric(int p) {
		return segments.get(p).getDeliveryMetric();
	}

	public Metric getTotalAuditMetric() {
		Metric totalMetric = new Metric();
		for (Segment segment : segments) {
			Metric metric = segment.getAuditMetric();
			totalMetric.add(metric);
		}
		return totalMetric;
	}

	public Metric getTotalDeliveredMetric() {
		Metric totalMetric = new Metric();
		for (Segment segment : segments) {
			Metric metric = segment.getDeliveryMetric();
			totalMetric.add(metric);
		}
		return totalMetric;
	}

	public Metric getTotalTargetMetric() {
		Metric totalMetric = new Metric();
		for (Segment segment : segments) {
			Metric metric = segment.getTargetMetric();
			totalMetric.add(metric);
		}
		return totalMetric;
	}

	public void updateClickRatios(double[] clickRatios) {
		for (int p = 0; p < segments.size(); p++) {
			segments.get(p).setClickRatio(clickRatios[p]);
		}
	}

	public void updateConversionRatios(double[] conversionRatios) {
		for (int p = 0; p < segments.size(); p++) {
			segments.get(p).setConversionRatio(conversionRatios[p]);
		}
	}

	public boolean isActive() {
		return (campaignParameters.getInt(Constants.STATUS, 0) > 0);
	}

	public int size() {
		return segments.size();
	}

	public boolean readPriceVolumeCurves(long startTime, long endTime) throws SQLException, ParseException {
		boolean succeeded = true;
		for (Segment segment : segments) {
			if (!segment.readPriceVolumeCurves(startTime, endTime)) {
				log.info("Default price-volume curve used");
				segment.setDefaultPriceVolumeCurves();
				succeeded = false;
				errors.add(segment.getErrors());
			}
		}
		return succeeded;
	}

	public void setImpressionTargets(Metric[] impressionTarget, Metric[] nextImpressionTarget, Comments comments) {
		for (int p = 0; p < segmentCount; p++) {
			segments.get(p).setImpressionTargets(impressionTarget[p], nextImpressionTarget[p], comments.get(p));
		}
	}

	public double[] getMaxImpressions() {
		double[] maxImpressions = new double[segmentCount];
		for (int p = 0; p < segmentCount; p++) {
			maxImpressions[p] = segments.get(p).getMaxImpressions();
		}
		return maxImpressions;
	}

	public double[] getPredictedEcpm() {
		double[] predictedEcpm = new double[segmentCount];
		for (int p = 0; p < segmentCount; p++) {
			predictedEcpm[p] = segments.get(p).getTargetEcpm();
		}
		return predictedEcpm;
	}

	public Set<AdGroup> getAdGroups() {
		Set<AdGroup> adGroups = new HashSet<AdGroup>();
		for (Segment segment : segments) {
			adGroups.addAll(segment.getAdGroups());
		}
		return adGroups;
	}

	public int updateDoTables(Metric cumAuditMetric, Metric cumDeliveredMetric, String target, String trend,
			String delivery) throws SQLException {
		updateSegmentTable();
		int updatedRecords = updatePerformanceTable(cumAuditMetric, cumDeliveredMetric, target, trend, delivery);
		return updatedRecords;
	}

	public int updatePerformanceTable(Metric cumAuditMetric, Metric cumDeliveryMetric, String target, String trend,
			String delivery) throws SQLException {
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		double margin = MathUtils.round(cumDeliveryMetric.getMargin(campaignParameters), 1);
		double ecpm = MathUtils.round(cumDeliveryMetric.getEcpm(), 2);
		double ekpi = MathUtils.round(campaignType.getEkpi(cumDeliveryMetric), 2);

		double cumCost = cumDeliveryMetric.getCost();
		int cumAuditImpressions = (int) cumAuditMetric.getImpressions();
		int cumDeliveryImpressions = (int) cumDeliveryMetric.getImpressions();
		int cumExcessImpressions = getExcessImpressions(cumDeliveryMetric);
		int cumAuditClicks = (int) (cumAuditMetric.getClicks() / clickAttribution);
		int cumDeliveryClicks = (int) (cumDeliveryMetric.getClicks() / clickAttribution);
		int cumExcessClicks = getExcessClicks(cumDeliveryMetric);
		double cumAuditConversions = cumAuditMetric.getConversions() / conversionAttribution;
		double cumDeliveryConversions = cumDeliveryMetric.getConversions() / conversionAttribution;
		double cumExcessConversions = getExcessConversions(cumDeliveryMetric);

		Metric deliveryMetric = getTotalDeliveredMetric(); //was cumDeliveryMetric
		double effRatio = (campaignType.isCpc() ? deliveryMetric.getClickRatio() : deliveryMetric
				.getConversionRatio());
		Metric targetMetric = getTotalTargetMetric();
		double goalRatio = (campaignType.isCpc() ? targetMetric.getClickRatio() : targetMetric.getConversionRatio());
		double singularity = 0;
		String query = String
				.format("UPDATE %s SET status = status | %s,target='%s',trend='%s',delivery='%s',margin=%f,ecpm=%f,ekpi=%f,cumCost=%f,cumAuditImpressions=%d,cumDeliveryImpressions=%d,cumExcessImpressions=%d,cumAuditClicks=%d,cumDeliveryClicks=%d,cumExcessClicks=%d,cumAuditConversions=%f,cumDeliveryConversions=%f,cumExcessConversions=%f,effRatio=%f,goalRatio=%f,singularity=%f WHERE id=%d;",
						Constants.DO_CAMPAIGN_PERFORMANCES, status.toString(), target, trend, delivery, margin, ecpm,
						(Double.isNaN(ekpi) ? null : ekpi), cumCost, cumAuditImpressions, cumDeliveryImpressions,
						cumExcessImpressions, cumAuditClicks, cumDeliveryClicks, cumExcessClicks, cumAuditConversions,
						cumDeliveryConversions, cumExcessConversions, effRatio, goalRatio, singularity, id);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		return statement.executeUpdate(query);
	}

	public void updateSegmentTable() throws SQLException {
		for (Segment segment : segments) {
			segment.updateSegmentTable(id);
		}
	}

	public int getUpdateSegmentCount() {
		return updateSegmentCount;
	}

	public void writeDeliveryMetrics() throws IOException, ParseException, SQLException {
		updateSegmentCount = 0;
		String datetime = DateUtils.getDatetime(time);
		for (Segment segment : segments) {
			int count = segment.writeDeliveryMetric(id);
			if (count == 1) {
				updateSegmentCount++;
			} else {
				log.error(String.format("Record in %s for date = %s updated %d times",
						Constants.DO_SEGMENT_PERFORMANCES, datetime, count));
			}
		}

		if (updateSegmentCount > 0) {
			String query = String.format("UPDATE %s SET status = status | %s WHERE id = %d;",
					Constants.DO_CAMPAIGN_PERFORMANCES, status.toString(), id);
			Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
			statement.executeUpdate(query);
		}

		log.info(deliveryImpressionSummary());
	}

	private String deliveryImpressionSummary() {
		StringBuilder builder = new StringBuilder();
		String datetime = DateUtils.getDatetime(time);
		builder.append(String.format("Record in %s for date = %s updated with: ", Constants.DO_SEGMENT_PERFORMANCES,
				datetime));
		for (Segment segment : segments) {
			builder.append(String.format("%,d (%,d, %.2f)  ", segment.getDeliveryImpressions(), segment.getDeliveryClicks(), segment.getDeliveryEcpm()));
		}
		return builder.toString();
	}

	public boolean addDeliveryMetrics(Map<Long, Metric> deliveryMetrics) throws DOException, ParseException {
		for (Segment segment : segments) {
			if (!segment.addDeliveryMetrics(deliveryMetrics)) {
				return false;
			}
		}
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		if (campaignType.isCpc()) {
			status.setKpi();
		}
		status.setImpression();
		return true;
	}

	public Map<Long, String> getAdGroupIds() {
		Map<Long, String> adGroupIds = new HashMap<Long, String>();
		for (Segment segment : segments) {
			adGroupIds.putAll(segment.getAdGroupIds());
		}
		return adGroupIds;
	}

	private double getCumImpressionTarget() {
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		long currentTime = time + 24L * Constants.MILLISECONDS_PER_HOUR;
		double factor = (double) (currentTime - startTime) / (double) (endTime - startTime);
		double targetImpressions = factor * campaignParameters.getDouble(Constants.TARGET_IMPRESSIONS, 0.0);
		return (int) Math.round(targetImpressions);
	}

	private int getExcessImpressions(Metric cumDeliveryMetric) {
		return (int) Math.round(cumDeliveryMetric.getImpressions() - getCumImpressionTarget());
	}

	private int getExcessClicks(Metric cumDeliveryMetric) {
		double targetRatio = campaignParameters.getDouble(Constants.TARGET_CLICKS, 0.0)
				/ campaignParameters.getDouble(Constants.TARGET_IMPRESSIONS, 0.0);
		double cumClickTarget = targetRatio * cumDeliveryMetric.getImpressions();
		return (int) Math.round(cumDeliveryMetric.getClicks() - cumClickTarget);
	}

	private double getExcessConversions(Metric cumDeliveryMetric) {
		double targetRatio = campaignParameters.getDouble(Constants.TARGET_CONVERSIONS, 0.0)
				/ campaignParameters.getDouble(Constants.TARGET_IMPRESSIONS, 0.0);
		double cumConversionTarget = targetRatio * cumDeliveryMetric.getImpressions();
		return (cumDeliveryMetric.getConversions() - cumConversionTarget);
	}
}
