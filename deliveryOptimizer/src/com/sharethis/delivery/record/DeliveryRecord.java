package com.sharethis.delivery.record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import com.sharethis.delivery.base.AdGroup;
import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class DeliveryRecord {
	private long time;
	private Segment[] segments;
	private String[] deliveryMsg;
	
	public DeliveryRecord(long time, Segment[] segments, String[] deliveryMsg) {
		this.time = time;
		this.segments = segments;
		this.deliveryMsg = deliveryMsg;
	}
	
	public static DeliveryRecord getPausedRecord(long time, Segment[] segments) {
		int segmentSize = segments.length;
		int[] intZeros = new int[segmentSize];
		double[] doubleZeros = new double[segmentSize];
		String[] msg = new String[segmentSize];
		for (int i = 0; i < segmentSize; i++) {
			intZeros[i] = 0;
			doubleZeros[i] = 0.0;
			msg[i] = "Campaign paused";
		}
		for (int i = 0; i < segments.length; i++) {
			segments[i].setImpressionTargets(0, 0);
		}
		return new DeliveryRecord(time, segments, msg);
	}
	
	public boolean validate(Parameters campaignParameters, Parameters globalParameters) throws ParseException {
		int impressions = Math.max(getTotalImpressionTarget(), getTotalNextImpressionTarget());
		if (impressions > globalParameters.getInt(Constants.DAILY_IMPRESSION_LIMIT, Constants.MAX_IMPRESSION_TARGET)) {
			resetImpressionTargets();
			resetDeliveryMessages("Error: daily impression limit exceeded");
			return false;
		}
		
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double days = (double) (endTime - startTime) / (double) (24 * Constants.MILLISECONDS_PER_HOUR);
		double dailyBudgetLimit = 10.0 * campaignParameters.getDouble(Constants.BUDGET, 0.0) / days;
		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		double customerSpend = 0.001 * cpm * impressions;
		double maxSpend = globalParameters.getDouble(Constants.DAILY_SPEND_LIMIT, Constants.MAX_DAILY_SPEND);
		maxSpend = Math.min(maxSpend, dailyBudgetLimit);
		
		if (customerSpend > maxSpend) {
			resetImpressionTargets();
			resetDeliveryMessages("Error: daily customer spend limit exceeded");
			return false;
		}
		return true;
	}
		
	public String[] getDeliveryComments() {
		return deliveryMsg;
	}
	
	public long getTime() {
		return time;
	}
	
	private int getTotalImpressionTarget() {
		int imps = 0;
		for (Segment segment : segments) {
			imps += segment.getTotalImpressionTarget();
		}
		return imps; 
	}
	
	private int getTotalNextImpressionTarget() {
		int imps = 0;
		for (Segment segment : segments) {
			imps += segment.getTotalNextImpressionTarget();
		}
		return imps; 
	}
	
	private void resetImpressionTargets() {
		for (Segment segment : segments) {
			segment.setImpressionTargets(0, 0);
		}
	}
	
	private void resetDeliveryMessages(String msg) {
		for (int i = 0; i < deliveryMsg.length; i++) {
			deliveryMsg[i] = msg;
		}
	}
	
	public void updateDeliveryMessages(String msg) {
		for (int i = 0; i < deliveryMsg.length; i++) {
			if (deliveryMsg[i].length() == 0) {
				deliveryMsg[i] = msg;
			} else {
				deliveryMsg[i] += "; " + msg;
			}
		}
	}
	
	public void addGoalMessages(boolean conversionTargetMet, boolean impressionTargetMet) {
		if (conversionTargetMet && impressionTargetMet) {
			updateDeliveryMessages("Conversion and impression goals met");
		} else if (conversionTargetMet) {
			updateDeliveryMessages("Conversion goal met");
		} else if (impressionTargetMet) {
			updateDeliveryMessages("Impression goal met");
		}
	}
	
	public int writeImpressionTargets(Parameters globalParameters, Parameters campaignParameters, long campaignId, int goalId) throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		String datetime = DateUtils.getDatetime(time);
		
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		boolean isEndOfCampaignPeriod = (time + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime);
		int adGroupAtfFactor = globalParameters.getInt(Constants.AD_GROUP_ATF_FACTOR, Integer.MAX_VALUE);
		
		int deliveredRecords = 0;
		String query = String.format("DELETE FROM %s WHERE campaignId = %d;", Constants.DO_IMPRESSION_TARGETS, campaignId);
		statement.executeUpdate(query);
		for (int s = 0; s < segments.length; s++) {
			Segment segment = segments[s];
			for (AdGroup adGroup : segment.getAdGroups()) {
				int atf = (isEndOfCampaignPeriod ? 0 : adGroup.getAtf(adGroupAtfFactor));
				query = String.format("INSERT INTO %s (campaignId,goalId,adGroupId,adGroupName,currentImpressionTarget,currentMaxCpm,nextImpressionTarget,atf,date,deliveryInterval,comment) ",
								Constants.DO_IMPRESSION_TARGETS)
						+ String.format("VALUES (%d,%d,%d,'%s',%d,%f,%d,%d,'%s',%f,'%s');", campaignId, goalId, adGroup.getId(), adGroup.getName(),
								adGroup.getImpressionTarget(), adGroup.getMaxCpm(), adGroup.getNextImpressionTarget(), atf, datetime, deliveryInterval,
								deliveryMsg[s]);
				deliveredRecords += statement.executeUpdate(query);
			}
		}
		connection.close();
		connection = null;
		return deliveredRecords;
	}
}
