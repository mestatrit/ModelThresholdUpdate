package com.sharethis.delivery.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import com.sharethis.delivery.base.AdGroup;
import com.sharethis.delivery.base.Campaign;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.PriceVolumeCurve;
import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class AdxData extends Campaign implements Data {
	private boolean isUpdated;
	private Parameters propertyParameters;

	private AdxData() {
		segmentCount = 2;
		isUpdated = false;
	}

	public AdxData(Parameters propertyParameters, long campaignId, int goalId) {
		this();
		initialize(campaignId, goalId);
		this.propertyParameters = propertyParameters;
	}

	public boolean read() throws ParseException, DOException, SQLException {
		long midnight = DateUtils.getToday(0);
		long now = DateUtils.currentTime();
		if (now - midnight < 3L * Constants.MILLISECONDS_PER_HOUR) {
			log.error("adx stats not ready for transfer");
			return false;
		}
		if (!readParameters(campaignId, goalId))
			return false;
		if (!readSegments(campaignId, true, true))
			return false;
		if (!readDeliveredImpressions()) {
			return false;
		}
		return true;
	}

	private boolean readDeliveredImpressions() throws DOException, ParseException {
		for (Segment segment : segments) {
			List<Long> adGroupIds = segment.getAdGroupIds();
			AdWordsData adWordsData = new AdWordsData(adGroupIds);
			Map<Long, PriceVolumeCurve> priceVolumeCurves = adWordsData.readStats(propertyParameters);
			if (!segment.addPriceVolumeCurves(priceVolumeCurves)) {
				return false;
			}
		}
		return true;
	}

	public void writeCampaignPerformances() throws IOException, ParseException, SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0)
				* Constants.MILLISECONDS_PER_HOUR);
		long startTime = segments[0].getTime();
		long endTime = startTime + deliveryInterval;
		String startDatetime = DateUtils.getDatetime(startTime);
		String endDatetime = DateUtils.getDatetime(endTime);
		log.error(startDatetime + "  " + endDatetime);
		String query = String
				.format("SELECT * FROM %s WHERE campaignId = %d AND goalId = %d AND '%s' <= date AND date < '%s' AND conversions IS NULL ORDER BY date DESC;",
						campaignPerformancesTable, campaignId, goalId, startDatetime, endDatetime);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long id = result.getLong("id");
			long recordTime = DateUtils.parseTime(result, "date");
			String datetime = DateUtils.getDatetime(recordTime);
			if (recordTime >= startTime && recordTime < endTime) {
				if (segments[0] == null) {
					log.error("segment 1 null");
				}
				if (segments[1] == null) {
					log.error("segment 2 null");
				}
				
//				double conversionRatio = getConversionRatio(statement);
//				double impressions = (double) segments[0].getDeliveredImpressions();
//				int avgConversions = (int) Math.floor(conversionRatio * impressions);
				int avgConversions = getPredictedConversions(statement);
				
				query = String
						.format("UPDATE %s SET conversions = %d, ecpm1 = %.2f, ecpm2 = %.2f, impressions = %d, imps_d_1 = %d, imps_d_2 = %d, maxCpm1 = %.2f, maxCpm2 = %.2f WHERE id = %d;",
								campaignPerformancesTable, avgConversions, segments[0].getDeliveredEcpm(),
								segments[1].getDeliveredEcpm(),
								segments[0].getDeliveredImpressions() + segments[1].getDeliveredImpressions(),
								segments[0].getDeliveredImpressions(), segments[1].getDeliveredImpressions(),
								segments[0].getTotalMaxCpm(), segments[1].getTotalMaxCpm(), id);
				int count = statement.executeUpdate(query);
				isUpdated = true;
				if (count == 1) {
					log.info(String
							.format("Record in %s with campaignId = %d, date = %s updated with RT Imps = %,d (%.2f) and nonRT Imps = %,d (%.2f)",
									campaignPerformancesTable, campaignId, datetime,
									segments[0].getDeliveredImpressions(), segments[0].getDeliveredEcpm(),
									segments[1].getDeliveredImpressions(), segments[1].getDeliveredEcpm()));
				} else {
					log.error(String.format("Record in %s with campaignId = %d, date = %s updated %d times",
							campaignPerformancesTable, campaignId, datetime, count));
				}
			} else {
				log.info(String.format("Record in %s with campaignId = %d, date = %s not updated",
						campaignPerformancesTable, campaignId, datetime));
			}
		} else {
			log.info(String.format(
					"Record in %s with campaignId = %d not updated because of missing target data or set conversions",
					campaignPerformancesTable, campaignId));
		}
	}

	public void writeAdGroupSettings() throws IOException, ParseException, SQLException {
		if (!isUpdated) {
			log.info(String.format("Records in %s with campaignId = %d, goalId = %d not updated",
					Constants.DO_AD_GROUP_SETTINGS, campaignId, goalId));
			return;
		}
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String datetime = DateUtils.getDatetime();
		for (Segment segment : segments) {
			for (AdGroup adGroup : segment.getAdGroups()) {
				String query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS,
						adGroup.getId());
				ResultSet result = statement.executeQuery(query);
				if (result.next()) {
					long id = result.getLong("id");
					query = String.format("UPDATE %s SET maxCpm = %.2f, updateDate='%s' WHERE id = %d;",
							Constants.DO_AD_GROUP_SETTINGS, adGroup.getMaxCpm(), datetime, id);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %d updated with maxCpm = %.2f",
								Constants.DO_AD_GROUP_SETTINGS, adGroup.getId(), adGroup.getMaxCpm()));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d updated %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroup.getId(), count));
					}
				} else {
					query = String
							.format("INSERT INTO %s (adGroupId,adGroupName,maxCpm,updateDate,createDate) VALUES (%d,'%s',%f,'%s','%s');",
									Constants.DO_AD_GROUP_SETTINGS, adGroup.getId(), adGroup.getName(),
									adGroup.getMaxCpm(), datetime, datetime);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %d inserted with maxCpm = %.2f",
								Constants.DO_AD_GROUP_SETTINGS, adGroup.getId(), adGroup.getMaxCpm()));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d inserted %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroup.getId(), count));
					}
				}
			}
		}
	}

	public boolean read(String adxFile) throws IOException, ParseException, SQLException {
		if (!readParameters(campaignId, goalId))
			return false;
		if (!readSegments(campaignId, true, true))
			return false;

		long campaignEndTime = campaignParameters.getLong(Constants.END_DATE, 0L);

		File file = new File(adxFile);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = reader.readLine();
		SimpleDateFormat format = new SimpleDateFormat("MMM dd, yyyy");
		int i1 = line.indexOf("(") + 1;
		int i2 = line.indexOf(")");
		long startTime = format.parse(line.substring(i1, i2)).getTime();
		long endTime = startTime
				+ (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0)
						* Constants.MILLISECONDS_PER_HOUR);
		while ((line = reader.readLine()) != null) {
			String[] values = line.split("\t");
			if (campaignEndTime <= endTime || values[0].equalsIgnoreCase("enabled") && !values[3].contains("paused")) {
				String adxAdGroupName = values[1];
				for (int i = 0; i < segments.length; i++) {
					Segment segment = segments[i];
					if (segment.hasAdGroup(adxAdGroupName) && segment.getTime() == startTime) {
						int deliveredImpressions = Integer.parseInt(values[8].trim());
						int deliveredClicks = Integer.parseInt(values[7].trim());
						double deliveredEcpm = Double.parseDouble(values[17].trim());
						double maxCpm = Double.parseDouble(values[6].trim());
						segment.addPriceVolumeCurve(adxAdGroupName, deliveredImpressions, deliveredClicks,
								deliveredEcpm, maxCpm, startTime);
						log.info(String.format(
								"Segment: %d, Ad group: %s, Impressions: %,d, eCPM: %.2f, max CPM: %.2f", i + 1,
								adxAdGroupName, deliveredImpressions, deliveredEcpm, maxCpm));
					}
				}
			}
		}
		reader.close();
		return true;
	}
}
