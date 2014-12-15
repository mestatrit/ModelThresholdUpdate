package com.sharethis.delivery.rtb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class TargetDelivery {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private Map<Long, Integer> impressionTargets;
	private Map<Long, Integer> nextImpressionTargets;
	private Map<Long, Double> cpmTargets;
	private Map<Long, Integer> atfTargets;
	private Map<Long, String> adGroupNames;
	private String urlRead, urlWrite;
	
	public TargetDelivery(String urlRead, String urlWrite) {
		impressionTargets = new HashMap<Long, Integer>();
		nextImpressionTargets = new HashMap<Long, Integer>();
		cpmTargets = new HashMap<Long, Double>();
		atfTargets = new HashMap<Long, Integer>();
		adGroupNames = new HashMap<Long, String>();
		this.urlRead = urlRead;
		this.urlWrite = urlWrite;
	}

	public boolean readTargets() throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(urlRead, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery("SELECT * FROM " + Constants.DO_IMPRESSION_TARGETS + ";");
		int count = 0;
		while (result.next()) {
			long startTime = DateUtils.parseTime(result.getString("date"));
			long adGroupId = result.getLong("adGroupId");
			double deliveryInterval = result.getDouble("deliveryInterval");
			long currentTime = DateUtils.currentTime();
			long endTime = startTime + (long) Math.round(deliveryInterval * 24 * Constants.MILLISECONDS_PER_HOUR);
			if (currentTime > startTime && currentTime < endTime) {
				impressionTargets.put(adGroupId, result.getInt("currentImpressionTarget"));
				nextImpressionTargets.put(adGroupId, result.getInt("nextImpressionTarget"));
				cpmTargets.put(adGroupId, result.getDouble("currentMaxCpm"));
				atfTargets.put(adGroupId, result.getInt("atf"));
				adGroupNames.put(adGroupId, result.getString("adGroupName"));
				count++;
			} else {
				log.error("impressionTargets table has expired entry for adGroupId = " + adGroupId);
			}
		}
		log.info("Number of ad groups read from impressionTargets table: " + count);
		connection.close();
		connection = null;
		return (count > 0);
	}

	public void writeTargets() throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(urlWrite, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String dayOfWeek = DateUtils.getDayOfWeek();
		String nextDayOfWeek = DateUtils.getDayOfWeek(1);
		String updateDate = DateUtils.getDatetimeUtc();
		int totalCount = 0;
		for (long adGroupId : impressionTargets.keySet()) {
			String query = String.format("SELECT COUNT(*) from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_DELIVERY, adGroupId);
			ResultSet result = statement.executeQuery(query);
			int rowCount = (result.next() ? result.getInt(1) : 0);
			if (rowCount == 1) {
				int impressions = impressionTargets.get(adGroupId);
				int nextImpressions = nextImpressionTargets.get(adGroupId);
				String adGroupName = adGroupNames.get(adGroupId);
				query = String.format("UPDATE %s SET impr%s = %d, impr%s = %d, updateDate = '%s' WHERE adGroupId = %d;",
						Constants.RTB_AD_GROUP_DELIVERY, dayOfWeek, impressions, nextDayOfWeek, nextImpressions, updateDate, adGroupId);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					totalCount++;
					log.info(String.format("Record in %s with adGroupId = %d updated by %,9d (%s)", Constants.RTB_AD_GROUP_DELIVERY, adGroupId, impressions, adGroupName));
				} else {
					log.error(String.format("Record in %s with adGroupId = %d updated %d times", Constants.RTB_AD_GROUP_DELIVERY, adGroupId, rowCount));
				}
			} else {
				log.error(String.format("%s has %d records with adGroupId = %d: not updated", Constants.RTB_AD_GROUP_DELIVERY, rowCount, adGroupId));
			}
		}
		log.info("Number of ad groups updated in " + Constants.RTB_AD_GROUP_DELIVERY + " table: " + totalCount + " at " + updateDate + " UTC");
		connection.close();
		connection = null;
	}
	
	public void writeAdx(Parameters propertyParameters) throws DOException {
		AdWordsData adWordsData = new AdWordsData(cpmTargets, adGroupNames);
		adWordsData.readAdGroupSettings(propertyParameters);
		adWordsData.writeAdx(propertyParameters);
	}

	public void writeRtb() throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(urlWrite, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String updateDate = DateUtils.getDatetimeUtc();
		int totalCount = 0;
		for (long adGroupId : impressionTargets.keySet()) {
			String query = String.format("SELECT COUNT(*) from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
			ResultSet result = statement.executeQuery(query);
			int rowCount = (result.next() ? result.getInt(1) : 0);
			if (rowCount == 1) {
				query = String.format("SELECT optimizationFlags from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
				result = statement.executeQuery(query);
				if (result.next()) {
					long oldOptimizationFlags = result.getLong("optimizationFlags");
					long newOptimizationFlags = oldOptimizationFlags;
					int atf = atfTargets.get(adGroupId);
					if (atf == 1) {
						newOptimizationFlags |= Constants.ATF_PLACEMENT;
					} else {
						newOptimizationFlags &= ~Constants.ATF_PLACEMENT;
					}
					String adGroupName = adGroupNames.get(adGroupId);
					query = String.format("UPDATE %s SET optimizationFlags = %d, updateDate = '%s' WHERE adGroupId = %d;",
							Constants.RTB_AD_GROUP_PROPERTY, newOptimizationFlags, updateDate, adGroupId);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						totalCount++;
						log.info(String.format("Record in %s with adGroupId = %d: %5d -> %5d (atf: %d, %s)", Constants.RTB_AD_GROUP_PROPERTY, adGroupId, oldOptimizationFlags, newOptimizationFlags, atf, adGroupName));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d updated %d times", Constants.RTB_AD_GROUP_PROPERTY, adGroupId, rowCount));
					}
				} else {
					log.error(String.format("OptimizationFlags in %s with adGroupId = %d not updated", Constants.RTB_AD_GROUP_PROPERTY, adGroupId));
				}
			} else {
				log.error(String.format("%s has %d records with adGroupId = %d: not updated", Constants.RTB_AD_GROUP_PROPERTY, rowCount, adGroupId));
			}
		}
		log.info("Number of ad groups updated in " + Constants.RTB_AD_GROUP_PROPERTY + " table: " + totalCount + " at " + updateDate + " UTC");
		connection.close();
		connection = null;
	}
		
	public static void truncateImpressionTargets() throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("TRUNCATE TABLE %s;", Constants.DO_IMPRESSION_TARGETS);
		statement.executeUpdate(query);
		connection.close();
		connection = null;
		log.info(Constants.DO_IMPRESSION_TARGETS + " table truncated");
	}
}
