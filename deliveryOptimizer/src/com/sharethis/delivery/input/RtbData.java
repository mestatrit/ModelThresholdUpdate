package com.sharethis.delivery.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import com.sharethis.delivery.base.Campaign;
import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class RtbData extends Campaign implements Data {

	private RtbData() {
		segmentCount = 0;
	}

	public RtbData(long campaignId, int goalId) {
		this();
		initialize(campaignId, goalId);
	}
	
	public boolean read() throws ParseException, SQLException {
		if (!readParameters(campaignId, goalId))
			return false;
		if (!readSegments(campaignId, true, true))
			return false;
		if (!readDeliveredImpressions(null))
			return false;

		return true;
	}
	
	private boolean readDeliveredImpressions(String pvDate) throws SQLException, ParseException {
		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		long startTime = (pvDate == null || pvDate.trim().length() == 0 ? segments[0].getTime() : DateUtils.parseTime(pvDate));
		long endTime = startTime + deliveryInterval;
		for (Segment segment : segments) {
			if (!segment.readDeliveredImpressions(startTime, endTime)) {
				return false;
			}
		}
		return true;
	}
	
	public void writeCampaignPerformances() throws SQLException {
		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		long startTime = segments[0].getTime();
		long endTime = startTime + deliveryInterval;
		String startDatetime = DateUtils.getDatetime(startTime);
		String endDatetime = DateUtils.getDatetime(endTime);
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();		
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d AND goalId = %d AND '%s' <= date AND date < '%s' AND conversions IS NULL ORDER BY date DESC;",
				campaignPerformancesTable, campaignId, goalId, startDatetime, endDatetime);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long id = result.getLong("id");
			long recordTime = DateUtils.parseTime(result, "date");
			String datetime = DateUtils.getDatetime(recordTime);
			if (recordTime >= startTime && recordTime < endTime) {
//				double conversionRatio = getConversionRatio(statement);
//				double impressions = (double) segments[0].getDeliveredImpressions();
//				int avgConversions = (int) Math.floor(conversionRatio * impressions);
				int avgConversions = getPredictedConversions(statement);
				query = String.format("UPDATE %s SET conversions = %d, ecpm1 = %.2f, ecpm2 = %.2f, impressions = %d, imps_d_1 = %d, imps_d_2 = %d WHERE id = %d;", campaignPerformancesTable, avgConversions, segments[0].getDeliveredEcpm(), segments[1].getDeliveredEcpm(), segments[0].getDeliveredImpressions() + segments[1].getDeliveredImpressions(), segments[0].getDeliveredImpressions(), segments[1].getDeliveredImpressions(), id);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					log.info(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s updated with RT Imps = %,d (%.2f) and nonRT Imps = %,d (%.2f)", campaignPerformancesTable, campaignId, goalId, datetime, segments[0].getDeliveredImpressions(), segments[0].getDeliveredEcpm(), segments[1].getDeliveredImpressions(), segments[1].getDeliveredEcpm()));
				} else {
					log.error(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s updated %d times", campaignPerformancesTable, campaignId, goalId, datetime, count));
				}
			} else {
				log.info(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s not updated", campaignPerformancesTable, campaignId, goalId, datetime));
			}
		} else {
			log.info(String.format("Record in %s with campaignId = %d, goalId = %d not updated because of missing target data or set conversions", campaignPerformancesTable, campaignId, goalId));	
		}
	}
	
	public void writeAdGroupSettings() {
	}
}
