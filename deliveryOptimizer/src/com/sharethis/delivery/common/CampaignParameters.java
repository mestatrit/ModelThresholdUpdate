package com.sharethis.delivery.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class CampaignParameters extends Parameters {

	public CampaignParameters(long campaignId, int goalId, String campaignSettingsTable) throws SQLException {
		super();
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d and goalId = %d;", campaignSettingsTable, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			put(Constants.CAMPAIGN_NAME, result.getString(Constants.CAMPAIGN_NAME));
			put(Constants.CAMPAIGN_ID, result.getString(Constants.CAMPAIGN_ID));
			put(Constants.GOAL_ID, result.getString(Constants.GOAL_ID));
			put(Constants.STATUS, result.getString(Constants.STATUS));
			put(Constants.BUDGET, result.getString(Constants.BUDGET));
			String margin = result.getString(Constants.MIN_MARGIN);
			if (result.wasNull()) {
				put(Constants.MIN_MARGIN, "-1000000.0");
			} else {
				put(Constants.MIN_MARGIN, margin);
			}
			put(Constants.CPA, result.getString(Constants.CPA));
			put(Constants.MAX_ECPA, result.getString(Constants.MAX_ECPA));
			put(Constants.CPM, result.getString(Constants.CPM));
			put(Constants.SEGMENTS, result.getString(Constants.SEGMENTS));
			put(Constants.DELIVERY_INTERVAL, result.getString(Constants.DELIVERY_INTERVAL));
			int reversionIntervals = result.getInt(Constants.REVERSION_INTERVALS);
			if (reversionIntervals > 0) {
				put(Constants.REVERSION_INTERVALS, result.getString(Constants.REVERSION_INTERVALS));
			}
			put(Constants.LEAVE_OUT_IMPRESSIONS, result.getString(Constants.LEAVE_OUT_IMPRESSIONS));
			put(Constants.STRATEGY, result.getString(Constants.STRATEGY));
			put(Constants.MODULATE, result.getString(Constants.MODULATE));
			put(Constants.CAMPAIGN_DELIVERY_MARGIN, result.getString(Constants.CAMPAIGN_DELIVERY_MARGIN));
			put(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN, result.getString(Constants.END_OF_CAMPAIGN_DELIVERY_MARGIN));
			put(Constants.START_DATE, Long.toString(DateUtils.parseTime(result, Constants.START_DATE)));
			put(Constants.END_DATE, Long.toString(DateUtils.parseTime(result, Constants.END_DATE)));
			put(Constants.PRECEDING_CAMPAIGN_IDS, result.getString(Constants.PRECEDING_CAMPAIGN_IDS));
			put(Constants.PRIOR_WEIGHT, result.getString(Constants.PRIOR_WEIGHT));
			int segments = getInt(Constants.SEGMENTS, 0);
			for (int i = 0; i < segments; i++) {
				String key = String.format("%s%d", Constants.CONVERSION_RATIO, i + 1);
				put(key, result.getString(key));
			}
		} else {
			put(Constants.STATUS, "0");
		}
		connection.close();
		connection = null;
	}
}
