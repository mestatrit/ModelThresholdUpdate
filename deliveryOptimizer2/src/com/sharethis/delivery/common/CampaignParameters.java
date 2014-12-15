package com.sharethis.delivery.common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sharethis.delivery.base.CampaignType;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class CampaignParameters extends Parameters {

	public CampaignParameters(long campaignId, int goalId) throws SQLException {
		super();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d and goalId = %d;",
				Constants.DO_CAMPAIGN_SETTINGS, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			put(Constants.CAMPAIGN_NAME, result.getString(Constants.CAMPAIGN_NAME));
			put(Constants.CAMPAIGN_ID, result.getString(Constants.CAMPAIGN_ID));
			put(Constants.GOAL_ID, result.getString(Constants.GOAL_ID));
			put(Constants.CAMPAIGN_TYPE, result.getString(Constants.CAMPAIGN_TYPE));
			put(Constants.KPI, result.getString(Constants.KPI));
			put(Constants.MAX_EKPI, result.getString(Constants.MAX_EKPI));
			put(Constants.CPM, result.getString(Constants.CPM));
			if (!CampaignType.valueOf(this).isValid()) {
				throw new SQLException("Unknown campaign type: " + this.get(Constants.CAMPAIGN_TYPE));
			}
			put(Constants.STATUS, result.getString(Constants.STATUS));
			put(Constants.BUDGET, result.getString(Constants.BUDGET));
			String margin = result.getString(Constants.MIN_MARGIN);
			if (result.wasNull()) {
				put(Constants.MIN_MARGIN, "-1000000.0");
			} else {
				put(Constants.MIN_MARGIN, margin);
			}
			put(Constants.SEGMENTS, result.getString(Constants.SEGMENTS));
			put(Constants.DELIVERY_INTERVAL, result.getString(Constants.DELIVERY_INTERVAL));
			int reversionIntervals = result.getInt(Constants.REVERSION_INTERVALS);
			if (reversionIntervals > 0) {
				put(Constants.REVERSION_INTERVALS, result.getString(Constants.REVERSION_INTERVALS));
			}
			put(Constants.MANAGE_ATF, result.getString(Constants.MANAGE_ATF));
			put(Constants.HOLIDAY_DELIVERY_FACTOR, result.getString(Constants.HOLIDAY_DELIVERY_FACTOR));
			put(Constants.KPI_ATTRIBUTION_FACTOR, result.getString(Constants.KPI_ATTRIBUTION_FACTOR));			
			put(Constants.LEAVE_OUT_IMPRESSIONS, result.getString(Constants.LEAVE_OUT_IMPRESSIONS));
			put(Constants.STRATEGY, result.getString(Constants.STRATEGY));
			put(Constants.MODULATE, result.getString(Constants.MODULATE));
			put(Constants.CAMPAIGN_DELIVERY_MARGIN, result.getString(Constants.CAMPAIGN_DELIVERY_MARGIN));
			put(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN, result.getString(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN));
			put(Constants.START_DATE, Long.toString(DateUtils.parseTime(result, Constants.START_DATE)));
			put(Constants.END_DATE, Long.toString(DateUtils.parseTime(result, Constants.END_DATE)));
			put(Constants.PRECEDING_CAMPAIGN_IDS, result.getString(Constants.PRECEDING_CAMPAIGN_IDS));
			int segments = getInt(Constants.SEGMENTS, 0);
			long campaignRecordId = result.getLong("id");
			if (result.next()) {
				put(Constants.STATUS, "0");
				throw new SQLException("Multiple campaign setting entries for: " + get(Constants.CAMPAIGN_NAME)
						+ ", campaignId: " + campaignId + ", goalId: " + goalId);
			} else {
				CampaignType campaignType = CampaignType.valueOf(this);
				double impliedKpiRatio = campaignType.getImpliedKpiRatio();
//				String impliedRatio = String.format("%.2e", 0.1 * impliedKpiRatio);
				
				query = String.format("SELECT * FROM %s WHERE campaignRecordId = %d;", Constants.DO_SEGMENT_SETTINGS, campaignRecordId);
				result = statement.executeQuery(query);
				int segmentCount = 0;
				while (result.next()) {
					segmentCount++;
					int active = result.getInt("active");
					int segmentId = result.getInt("segmentId");
					String priorRatio, priorWeight;
					if (active == 1) {
						priorRatio = result.getString("priorRatio");
						priorWeight = result.getString("priorWeight");
					} else {
						String impliedRatio = (segmentId == 0 ? String.format("%.2e", 0.1 * impliedKpiRatio) : "0.0");
						priorRatio = impliedRatio;
						priorWeight = "10000.0";
					}
					String key = String.format("%s%d", Constants.PRIOR_RATIO, segmentId);
					put(key, priorRatio);
					key = String.format("%s%d", Constants.PRIOR_WEIGHT, segmentId);
					put(key, priorWeight);	
				}
				if (segmentCount != segments) {
					throw new SQLException("Campaign name: " + get(Constants.CAMPAIGN_NAME) + ": wrong number of segments read: " + segmentCount);
				}
			}
		} else {
			put(Constants.STATUS, "0");
		}
	}
	

}
