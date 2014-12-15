package com.sharethis.delivery.rtb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class TargetDelivery {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private Map<Long, Integer> impressionTargets;
	private Map<Long, Integer> nextImpressionTargets;
	private Map<Long, Double> cpmTargets;
	private Map<Long, Integer> atfTargets;
	private Map<Long, String> adGroupNames;
	private String urlRead, urlWriteRtb, urlWriteAdPlatform;
	private Errors errors;
	
	public TargetDelivery(String urlRead, String urlWriteRtb, String urlWriteAdPlatform) {
		impressionTargets = new HashMap<Long, Integer>();
		nextImpressionTargets = new HashMap<Long, Integer>();
		cpmTargets = new HashMap<Long, Double>();
		atfTargets = new HashMap<Long, Integer>();
		adGroupNames = new HashMap<Long, String>();
		this.urlRead = urlRead;
		this.urlWriteRtb = urlWriteRtb;
		this.urlWriteAdPlatform = urlWriteAdPlatform;
		errors = new Errors();
	}

	public boolean readTargets() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(urlRead);
		ResultSet result = statement.executeQuery("SELECT * FROM " + Constants.DO_IMPRESSION_TARGETS + ";");
		int count = 0;
		while (result.next()) {
			String startDate = result.getString("date");
			long startTime = DateUtils.parseTime(startDate);
			long adGroupId = result.getLong("adGroupId");
			String adGroupName = result.getString("adGroupName");
			
			double deliveryInterval = result.getDouble("deliveryInterval");
			long today = DateUtils.getToday(0);
			long tomorrow = DateUtils.getToday(1);
			double dayLength = (double) (tomorrow - today) / (double) Constants.MILLISECONDS_PER_HOUR;
			double numberOfIntervals = Math.round(dayLength / deliveryInterval);
			deliveryInterval = dayLength / numberOfIntervals;
			
			long currentTime = DateUtils.currentTime();
			long endTime = startTime + (long) Math.round(deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
			if (currentTime > startTime && currentTime < endTime) {
				impressionTargets.put(adGroupId, result.getInt("currentImpressionTarget"));
				nextImpressionTargets.put(adGroupId, result.getInt("nextImpressionTarget"));
				cpmTargets.put(adGroupId, result.getDouble("currentMaxCpm"));
				atfTargets.put(adGroupId, result.getInt("atf"));
				adGroupNames.put(adGroupId, adGroupName);
				count++;
			} else {
				errors.add(adGroupId, adGroupName, "Not delivered: Date not current (" + startDate + ")");
				log.error("impressionTargets table has expired entry for " + adGroupName + ", adGroupId = " + adGroupId);
			}
		}
		log.info("Number of ad groups read from impressionTargets table: " + count);
		return (count > 0);
	}

	public void writeTargets() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(urlWriteRtb);
		String dayOfWeek = DateUtils.getDayOfWeek();
		String nextDayOfWeek = DateUtils.getDayOfWeek(1);
		String updateDate = DateUtils.getDatetimeUtc();
		int totalCount = 0;
		for (long adGroupId : impressionTargets.keySet()) {
			String adGroupName = adGroupNames.get(adGroupId);
			String query = String.format("SELECT COUNT(*) from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_DELIVERY, adGroupId);
			ResultSet result = statement.executeQuery(query);
			int rowCount = (result.next() ? result.getInt(1) : 0);
			if (rowCount == 1) {
				int impressions = impressionTargets.get(adGroupId);
				int nextImpressions = nextImpressionTargets.get(adGroupId);
				query = String.format("UPDATE %s SET impr%s = %d, impr%s = %d, updateDate = '%s' WHERE adGroupId = %d;",
						Constants.RTB_AD_GROUP_DELIVERY, dayOfWeek, impressions, nextDayOfWeek, nextImpressions, updateDate, adGroupId);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					totalCount++;
					log.info(String.format("Record in %s with adGroupId = %d updated by %,9d (%s)", Constants.RTB_AD_GROUP_DELIVERY, adGroupId, impressions, adGroupName));
				} else {
					errors.add(adGroupId, adGroupName, Constants.RTB_AD_GROUP_DELIVERY + ": Updated multiple times");
					log.error(String.format("Record in %s with adGroupId = %d updated %d times", Constants.RTB_AD_GROUP_DELIVERY, adGroupId, rowCount));
				}
			} else {
				errors.add(adGroupId, adGroupName, Constants.RTB_AD_GROUP_DELIVERY + ": Not updated");
				log.error(String.format("%s has %d records with adGroupId = %d: not updated", Constants.RTB_AD_GROUP_DELIVERY, rowCount, adGroupId));
			}
		}
		log.info("Number of ad groups updated in " + Constants.RTB_AD_GROUP_DELIVERY + " table: " + totalCount + " at " + updateDate + " UTC");
	}
	
	public void writeBids() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(urlWriteAdPlatform);
		long updateDate = DateUtils.currentTime();
		int totalCount = 0;
		for (long adGroupId : cpmTargets.keySet()) {
			String adGroupName = adGroupNames.get(adGroupId);
			String query = String.format("SELECT COUNT(*) from %s WHERE networkAdgId = %d;", Constants.ADPLATFORM_DB_AD_GROUP, adGroupId);
			ResultSet result = statement.executeQuery(query);
			int rowCount = (result.next() ? result.getInt(1) : 0);
			if (rowCount == 1) {
				long maxPrice = (long) Math.round(1.0e6 * cpmTargets.get(adGroupId));
//				query = String.format("UPDATE %s SET maxPrice = %d, updateDate = %d WHERE networkAdgId = %d;",
//						Constants.ADPLATFORM_DB_AD_GROUP, maxPrice, updateDate/1000L, adGroupId);
				query = String.format("UPDATE %s SET maxPrice = %d WHERE networkAdgId = %d;",
						Constants.ADPLATFORM_DB_AD_GROUP, maxPrice, adGroupId);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					totalCount++;
					log.info(String.format("Record in %s with adGroupId = %d updated by %6.2f (%s)", Constants.ADPLATFORM_DB_AD_GROUP, adGroupId, 1.0e-6 * maxPrice, adGroupName));
				} else {
					errors.add(adGroupId, adGroupName, Constants.ADPLATFORM_DB_AD_GROUP + ": Updated multiple times");
					log.error(String.format("Record in %s with adGroupId = %d updated %d times", Constants.ADPLATFORM_DB_AD_GROUP, adGroupId, rowCount));
				}
			} else {
				errors.add(adGroupId, adGroupName, Constants.ADPLATFORM_DB_AD_GROUP + ": Not updated");
				log.error(String.format("%s has %d records with adGroupId = %d: not updated", Constants.ADPLATFORM_DB_AD_GROUP, rowCount, adGroupId));
			}
		}
		log.info("Number of ad groups updated in " + Constants.ADPLATFORM_DB_AD_GROUP + " table: " + totalCount + " at " + DateUtils.getDatetimeUtc(updateDate) + " UTC");
	}
	
	public boolean writeAdx(Parameters propertyParameters) throws DOException {
		AdWordsData adWordsData = new AdWordsData(cpmTargets, adGroupNames);
		adWordsData.readAdGroupSettings(propertyParameters);
		if (adWordsData.writeAdx(propertyParameters)) {
			return true;
		} else {
			errors = adWordsData.getError();
			return false;
		}
	}

	public void writeRtb() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(urlWriteRtb);
		String updateDate = DateUtils.getDatetimeUtc();
		int totalCount = 0;
		for (long adGroupId : impressionTargets.keySet()) {
			String adGroupName = adGroupNames.get(adGroupId);
			int atf = atfTargets.get(adGroupId);
			if (atf < 0) continue;
			String query = String.format("SELECT COUNT(*) from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
			ResultSet result = statement.executeQuery(query);
			int rowCount = (result.next() ? result.getInt(1) : 0);
			if (rowCount == 1) {
				query = String.format("SELECT optimizationFlags from %s WHERE adGroupId = %d;", Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
				result = statement.executeQuery(query);
				if (result.next()) {
					long oldOptimizationFlags = result.getLong("optimizationFlags");
					long newOptimizationFlags = oldOptimizationFlags;
					if (atf == 1) {
						newOptimizationFlags |= Constants.ATF_PLACEMENT;
					} else {
						newOptimizationFlags &= ~Constants.ATF_PLACEMENT;
					}
					query = String.format("UPDATE %s SET optimizationFlags = %d, updateDate = '%s' WHERE adGroupId = %d;",
							Constants.RTB_AD_GROUP_PROPERTY, newOptimizationFlags, updateDate, adGroupId);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						totalCount++;
						log.info(String.format("Record in %s with adGroupId = %d: %5d -> %5d (atf: %d, %s)", Constants.RTB_AD_GROUP_PROPERTY, adGroupId, oldOptimizationFlags, newOptimizationFlags, atf, adGroupName));
					} else {
						errors.add(adGroupId, adGroupName, Constants.RTB_AD_GROUP_PROPERTY + ": Updated multiple times");
						log.error(String.format("Record in %s with adGroupId = %d updated %d times", Constants.RTB_AD_GROUP_PROPERTY, adGroupId, rowCount));
					}
				} else {
					errors.add(adGroupId, adGroupName, Constants.RTB_AD_GROUP_PROPERTY + ": OptimizationFlags not updated");
					log.error(String.format("OptimizationFlags in %s with adGroupId = %d not updated", Constants.RTB_AD_GROUP_PROPERTY, adGroupId));
				}
			} else {
				errors.add(adGroupId, adGroupName, Constants.RTB_AD_GROUP_PROPERTY + ": Not updated");
				log.error(String.format("%s has %d records with adGroupId = %d: not updated", Constants.RTB_AD_GROUP_PROPERTY, rowCount, adGroupId));
			}
		}
		log.info("Number of ad groups updated in " + Constants.RTB_AD_GROUP_PROPERTY + " table: " + totalCount + " at " + updateDate + " UTC");
	}
	
	
	public void reportErrors() throws SQLException {
		if (!errors.isEmpty()) {
			errors.setTask("Deliver");
			errors.updateErrorTable();
		}
	}
	
	public int size() {
		return impressionTargets.size();
	}
}
