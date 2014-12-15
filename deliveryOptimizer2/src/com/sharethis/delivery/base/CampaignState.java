package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.MathUtils;

public class CampaignState extends State {
	private long campaignId;
	private int goalId;
	private int deliveredAdGroups;
	
	public CampaignState(Parameters campaignParameters, long time) {
		super(campaignParameters);
		this.campaignId = campaignParameters.getLong(Constants.CAMPAIGN_ID, 0);
		this.goalId = campaignParameters.getInt(Constants.GOAL_ID, 0);
		this.time = time;
		deliveredAdGroups = 0;
	}
	
	public boolean readState() throws SQLException, ParseException {	
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d AND yield > 0;", 
				Constants.DO_AD_GROUP_MAPPINGS, campaignId);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		ResultSet result = statement.executeQuery(query);
		return parsePhysicalState(result);
	}
	
	public boolean readPriceVolumeCurves(String pvDate) throws SQLException, ParseException {
		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		long startTime = (pvDate == null || pvDate.trim().length() == 0 ? time : DateUtils.parseTime(pvDate));
		long endTime = startTime;
		startTime -= deliveryInterval;
		return super.readPriceVolumeCurves(startTime, endTime);
	}

	public double[] getCurrentImpressionDelivery() throws SQLException {
		double[] delivery = new double[segmentCount];
		for (int p = 0; p < segmentCount; p++) {
			delivery[p] = segments.get(p).getCurrentImpressionDelivery();
		}
		return delivery;
	}

	public boolean wouldOverDeliver(double[] impressionTarget) throws SQLException {
		double overDelivery = 0.0;
		double targetDelivery = 0.0;
		for (int p = 0; p < segmentCount; p++) {
			overDelivery += Math.max(segments.get(p).getCurrentImpressionDelivery() - impressionTarget[p], 0.0);
			targetDelivery += impressionTarget[p];
		}
		return (overDelivery > 0.1 * targetDelivery);
	}
	
	public boolean writeCampaignState() throws SQLException, ParseException {
		long campaignRecordId = writeTargetState();
		for (Segment segment : segments) {
			segment.writeTargetSegment(campaignRecordId);
		}
		return true;
	}
	
	public boolean writeImpressionTargets() throws SQLException, ParseException {
		deliveredAdGroups = 0;
		if (hasNotStarted()) {
			resetSegmentImpressionTargets();
			errors.add("Impression targets not updated: Campaign has not started");
			errors.log();
			return false;
		}
		if (hasEnded()) {
			resetSegmentImpressionTargets();
			errors.add("Impression targets not updated: Campaign has ended");
			errors.log();
			return false;
		}
		if (time < DateUtils.getToday(0)) {
			resetSegmentImpressionTargets();
			errors.add("Impression targets not updated: Delivery time in the past");
			errors.log();
			return false;
		}
		if (time >= DateUtils.getToday(1)) {
			resetSegmentImpressionTargets();
			errors.add("Impression targets not updated: Delivery time in the future");
			errors.log();
			return false;
		}
//		if (willOverDeliver()) {
//			resetSegmentImpressionTargets();
//			errors.add("Impression targets not updated: New impression targets would lead to over delivery");
//			errors.log();
//			return false;
//		}
		for (Segment segment : segments) {
			deliveredAdGroups += segment.writeImpressionTargets(campaignParameters);
		}
		return true;
	}
	
	public void resetSegmentImpressionTargets() throws SQLException {
		for (Segment segment : segments) {
			segment.resetImpressionTargets();
		}
	}
	
	public int getDeliveredRecords() {
		return deliveredAdGroups;
	}

	private long writeTargetState() throws SQLException, ParseException {
		String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		double dayLength = DateUtils.getHoursPerDay(time);
		double numberOfIntervals = Math.round(dayLength / deliveryInterval);
		deliveryInterval = dayLength / numberOfIntervals;
		
		String datetime = DateUtils.getDatetime(time);
		double budget = campaignParameters.getDouble(Constants.BUDGET, 0.0);
		double kpi = campaignParameters.getDouble(Constants.KPI, 0.0);
		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		String strategy = campaignParameters.get(Constants.STRATEGY);
		int leaveOutImpressions = campaignParameters.getInt(Constants.LEAVE_OUT_IMPRESSIONS, 0);
		Metric targetMetric = super.getTotalTargetMetric();
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		double goalRatio = MathUtils.round(campaignType.isCpc() ? targetMetric.getClickRatio() : targetMetric.getConversionRatio(), 3);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("INSERT INTO %s (campaignId,campaignName,goalId,status,date,deliveryInterval,goalRatio,budget,kpi,cpm,strategy,leaveOutImpressions) VALUES (%d,'%s',%d,%d,'%s',%f,%f,%f,%f,%f,'%s',%d);", 
				Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, campaignName, goalId, 0, datetime, deliveryInterval,
				goalRatio, budget, kpi, cpm, strategy, leaveOutImpressions);
		int rowCount = statement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
		ResultSet result = statement.getGeneratedKeys();
		if (result.next()) {
		       return result.getLong(1);
		} else {
			throw new SQLException(String.format("Insert to %s generated no key for %s", Constants.DO_CAMPAIGN_PERFORMANCES, campaignName));
		}
	}
	
	public long getTime() {
		return time;
	}

	public boolean hasEnded() {
		return time >= campaignParameters.getLong(Constants.END_DATE, 0L);
	}
	
	public boolean hasNotStarted() {
		return time < campaignParameters.getLong(Constants.START_DATE, 0L);
	}
}
