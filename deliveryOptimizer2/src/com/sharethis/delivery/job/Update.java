package com.sharethis.delivery.job;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Map;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Update extends Runner {

	private Update(String[] args) {
		initialize(args);
	}

	private void parseUpdateProperties() throws DOException {
		parseProperties();
	}

	private void updateLookalikeExpiryDate(Map<Long, String> adGroupIds) throws SQLException, ParseException {
		if (DateUtils.getIntDayOfWeek() != 0) {
			log.info("Lookalike expiry dates not reviewed");
			return;
		}
		log.info("Lookalike expiry dates: reviewing ...");
		String today = DateUtils.getToday();
		long thresholdDate = DateUtils.getToday(8);
		Statement statementLA = DbUtils.getStatement(Constants.URL_LOOKALIKE);
		Statement statementADP = DbUtils.getStatement(Constants.URL_ADPLATFORM);
		String query = String.format("SELECT value FROM lookalike.globalParameters WHERE parameter='Do Not Process After Days';");
		ResultSet result = statementADP.executeQuery(query);
		int days = (result.next() ? result.getInt("value") : 30);
		for (long adGroupId : adGroupIds.keySet()) {
			query = String.format("SELECT a.id AS id, a.updateDate AS updateDate, b.name AS audienceName, c.name AS adGroupName " +
			                      "FROM lookalike.adGroups a, adplatform.audience b, adplatform.adgroup c, " +
					                   "adplatform.mappingAdGrpAndAudGrp d, adplatform.mappingAudGrpAndAud e " +
		                          "WHERE c.id=d.adGroupId AND d.audGrpId=e.audGrpId AND e.audId=b.id " + 
					              "AND b.networkId=1 AND b.id = a.pixelId AND d.audGrpId <> 0 AND c.networkAdgId=%d ORDER BY b.name;", adGroupId);
			result = statementADP.executeQuery(query);
			StringBuilder info = new StringBuilder();
			while (result.next()) {
				long id = result.getLong("id");
				long time = DateUtils.parseDateTime(result, "updateDate");
				String audienceName = result.getString("audienceName");
				if (DateUtils.getTime(time, days) < thresholdDate) {
					info.append(String.format("\n   %-40s: updateDate %s -> %s", audienceName, DateUtils.getDatetime(time), today));
					query = String.format("UPDATE %s SET updateDate = '%s' WHERE id = %d;", Constants.LOOKALIKE_DB_AD_GROUPS, today, id);
					statementLA.executeUpdate(query);
				}
			}
			if (info.length() > 0) {
				log.info(adGroupIds.get(adGroupId) + ":" + info.toString());
			}
		}
		log.info("Lookalike expiry dates: Done");
	}
	
	public void updateAdGroupSettings(Map<Long, String> adGroupIds) throws IOException, ParseException, SQLException {
		Statement adplatformStatement = DbUtils.getStatement(Constants.URL_ADPLATFORM);
		Statement updateStatement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String datetime = DateUtils.getDatetime();
		for (long adGroupId : adGroupIds.keySet()) {
			String query = String.format("SELECT a.networkId AS networkId, a.maxPrice AS maxCpm, a.frequencyCapValue AS fcAdGroup, c.frequencyCapImpression AS fcCampaign FROM %s a, %s c WHERE networkAdgId = %d AND a.campaignId = c.id;", Constants.ADPLATFORM_DB_AD_GROUP, Constants.ADPLATFORM_DB_CAMPAIGN, adGroupId);
			ResultSet result = adplatformStatement.executeQuery(query);
			if (result.next()) {
				double maxCpm = 1.0e-6 * result.getLong("maxCpm");
				int frequencyCap = (result.getInt("networkId") == 1 ? result.getInt("fcCampaign") : result.getInt("fcAdGroup"));
				query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS, adGroupId);
				result = updateStatement.executeQuery(query);
				if (result.next()) {
					long id = result.getLong("id");
					query = String.format("UPDATE %s SET maxCpm = %.2f, frequencyCap = %d, updateDate='%s' WHERE id = %d;",
							Constants.DO_AD_GROUP_SETTINGS, maxCpm, frequencyCap, datetime, id);
					int count = updateStatement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %10d updated with maxCpm = %6.2f and FC = %2d",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, maxCpm, frequencyCap));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d updated %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
					}
				} else {
					query = String
							.format("INSERT INTO %s (adGroupId,adGroupName,maxCpm,updateDate,createDate) VALUES (%d,'%s',%f,'%s','%s');",
									Constants.DO_AD_GROUP_SETTINGS, adGroupId, adGroupIds.get(adGroupId),
									maxCpm, datetime, datetime);
					int count = updateStatement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %d inserted with maxCpm = %.2f",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, maxCpm));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d inserted %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
					}
				}
			} else {
				log.info(String.format("%s: Ad group: %s (%d) not missing in %s",
						Constants.DO_AD_GROUP_SETTINGS, adGroupIds.get(adGroupId), adGroupId, Constants.ADPLATFORM_DB_AD_GROUP));
			}
		}
	}
	
	protected int run() {
		try {
			parseUpdateProperties();

			if (!propertyParameters.getBoolean(Constants.RUN_UPDATE, false)) {
				log.info("Update stage skipped");
				return 0;
			}
			boolean readAllAgGroups = false;
			Map<Long, String> adGroupIds = getAdGroupIds(readAllAgGroups);
			updateLookalikeExpiryDate(adGroupIds);
			updateAdGroupSettings(adGroupIds);
/*
			AdWordsData adWordsData = new AdWordsData(adGroupIds);
			if (adWordsData.readAdGroupSettings(propertyParameters)) {
				adWordsData.writeAdGroupSettings();
			} else {
				log.error("Error reading max cpm values from AdWords");
				return 1;
			}
*/
			updateAdminDate("Update");
			DbUtils.close();
		} catch (Exception e) {
			log.error("Update Exception (run):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		Update update = new Update(args);
		System.exit(update.run());
	}
}
