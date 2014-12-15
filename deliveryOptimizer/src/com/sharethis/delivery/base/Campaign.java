package com.sharethis.delivery.base;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.CampaignParameters;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public abstract class Campaign {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	protected long campaignId;
	protected int goalId;
	protected int status;
	protected Segment[] segments;
	protected int segmentCount;
	protected Parameters campaignParameters;
	protected String campaignSettingsTable, campaignPerformancesTable, campaignPerformancesBackupTable;
	
	protected void initialize(long campaignId, int goalId) {
		this.campaignId = campaignId;
		this.goalId = goalId;
		this.campaignSettingsTable = Constants.DO_CAMPAIGN_SETTINGS;
		this.campaignPerformancesTable = Constants.DO_CAMPAIGN_PERFORMANCES;
		this.campaignPerformancesBackupTable = Constants.DO_CAMPAIGN_PERFORMANCES_BACKUP;
	}
	
	public boolean isActive() {
		return (status > 0);
	}
	
	public boolean readParameters(long campaignId, int goalId) throws ParseException, SQLException {
		campaignParameters = new CampaignParameters(campaignId, goalId, campaignSettingsTable);
		status = campaignParameters.getInt(Constants.STATUS, 0);
		if (status == 0)
			return false;

		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		
		if (segmentCount != 2) {
			String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
			log.error(campaignName + " does not have 2 audience segments");
			return false;
		} else {
			return true;
		}
	}
	
	public boolean readSegments(long campaignId, boolean readAllAdGroups, boolean conversionsIsNull) throws ParseException, SQLException {
		int yield = (readAllAdGroups ? -1 : 0);
		segments = new Segment[segmentCount];
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();		
		long recordTime = 0L;
		String query = null;
		if (conversionsIsNull) {
			query = String.format("SELECT date FROM %s WHERE campaignId = %d AND goalId = %d AND conversions IS NULL ORDER BY date DESC;",
				    campaignPerformancesTable, campaignId, goalId);
		} else {
			query = String.format("SELECT date FROM %s WHERE campaignId = %d AND goalId = %d AND conversions IS NOT NULL ORDER BY date DESC;",
					campaignPerformancesTable, campaignId, goalId);
		}
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			recordTime = DateUtils.parseTime(result, "date");
			if (result.next() && conversionsIsNull) {
				return false;
			}
		} else {
			recordTime = DateUtils.getToday(-1);//campaignParameters.getLong(Constants.START_DATE, 0L) - (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		}
		
		int adGroupCount = 0;
		for (int segmentId = 0; segmentId < segmentCount; segmentId++) {
			Segment segment = new Segment();
			query = String.format("SELECT * FROM %s WHERE campaignId = %d AND segment = %d AND yield > %d;", Constants.DO_AD_GROUP_MAPPINGS,
					campaignId, segmentId + 1, yield);
			result = statement.executeQuery(query);
			if (!segment.readAdGroups(result)) {
				return false;
			}
			segment.setTime(recordTime);
			segments[segmentId] = segment;
			adGroupCount += segment.size();
		}
		log.info("Ad groups read: " + adGroupCount);
		connection.close();
		connection = null;
		return true;
	}
	
	public boolean readPriceVolumeCurves(String pvDate) throws SQLException, ParseException {
		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		for (Segment segment : segments) {
			long startTime = (pvDate == null || pvDate.trim().length() == 0 ? segments[0].getTime() : DateUtils.parseTime(pvDate));
			long endTime = startTime + deliveryInterval;
			if (!segment.readPriceVolumeCurves(startTime, endTime)) {
				long campaignStartTime = campaignParameters.getLong(Constants.START_DATE, 0L);
				startTime += deliveryInterval;
				if (campaignStartTime == startTime) {
					log.info("Default price-volume curve used");
					segment.setDefaultPriceVolumeCurves();
				} else {
					return false;
				}
			}
		}
		return true;
	}
	
	public int getPredictedConversions(Statement statement) throws SQLException {
		String query = String.format("SELECT predictedConversions FROM %s WHERE campaignId = %d AND goalId = %d AND conversions IS NULL AND imps_d_1 IS NULL ORDER BY date LIMIT 1;",
				campaignPerformancesTable, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		return (result.next() ? result.getInt("predictedConversions") : 0);
	}
	
	public double getConversionRatio(Statement statement) throws SQLException {
		String query = String.format("SELECT 1000*SUM(conversions)/SUM(imps_d_1) AS ratio FROM %s WHERE campaignId = %d AND goalId = %d AND conversions IS NOT NULL AND imps_d_1 > 0;",
				campaignPerformancesTable, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		return (result.next() ? 0.001 * result.getDouble("ratio") : 0.0);
	}
}
