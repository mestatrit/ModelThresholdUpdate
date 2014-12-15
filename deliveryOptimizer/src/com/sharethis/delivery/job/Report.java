package com.sharethis.delivery.job;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sharethis.common.helper.AppConfig;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Report extends Runner {
	private final static int maxCampaignNameLength = 27;
	private StringBuilder reportBuilder;
	private Map<Long, Double> cpm;
	private Map<Long, Integer> fc;
	private Map<Long, Boolean> atf;
	private String reportType;
	private static boolean initialized = false;

	private Report(String[] args) {
		initialize(args);
		reportBuilder = new StringBuilder();
	}

	private void parseReportProperties() throws DOException {
		options.addOption(Constants.REPORT_TYPE_PROPERTY, true, "report type (adg, cpg, aud, all)");
		parseProperties();
		if (!initialized) {
			Constants.URL_DELIVERY_OPTIMIZER = propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER);
			Constants.URL_RTB_STATISTICS = propertyParameters.get(Constants.URL_RTB_STATISTICS);
			Constants.URL_RTB = propertyParameters.get(Constants.URL_RTB);
			Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
			Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
		}
		reportType = line.getOptionValue(Constants.REPORT_TYPE_PROPERTY).toLowerCase();
		initialized = true;
	}

	protected void readAdGroupSettings(List<Long> adGroupIds) throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		cpm = new HashMap<Long, Double>();
		fc = new HashMap<Long, Integer>();
		for (long adGroupId : adGroupIds) {
			String query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS,
					adGroupId);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				double maxCpm = result.getDouble("maxCpm");
				cpm.put(adGroupId, maxCpm);
				int frequencyCap = result.getInt("frequencyCap");
				fc.put(adGroupId, frequencyCap);
			} else {
				log.error(String.format("Record in %s with adGroupId = %d missing", Constants.DO_AD_GROUP_SETTINGS,
						adGroupId));
				cpm.put(adGroupId, -1.0);
			}
		}
	}

	protected void readRtbSettings(List<Long> adGroupIds) throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_RTB, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		atf = new HashMap<Long, Boolean>();
		for (long adGroupId : adGroupIds) {
			String query = String.format("SELECT optimizationFlags FROM %s WHERE adGroupId = %d;",
					Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				long optimizationFlags = result.getLong("optimizationFlags");
				atf.put(adGroupId, (optimizationFlags & Constants.ATF_PLACEMENT) != 0);
				if (result.next()) {
					throw new SQLException(String.format("adGroupId = %d returns more than one row from %s", adGroupId,
							Constants.RTB_AD_GROUP_PROPERTY));
				}
			} else {
				log.error(String.format("Record in %s with adGroupId = %d missing", Constants.RTB_AD_GROUP_PROPERTY,
						adGroupId));
				atf.put(adGroupId, false);
			}
		}
	}

	protected void printCampaignSettings() throws SQLException, ParseException {
		reportBuilder.append(String.format("%-27s %4s  %4s  %11s  %26s  %6s %7s %10s %10s  %-10s  %-10s\n", "Campaign Name", "Goal",
				"Type", "Budget", "Strategy", "KPI", "eKPI", "Margin-%", "Complete-%", "Start Date", "End Date"));
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT t1.campaignName, t1.goalId, t2.type, t2.budget, t2.strategy, t2.cpa, t1.ecpa, t1.margin, "
						+ "0.01*round(10000*t1.cumImpressions/round(1000*t2.budget/t2.cpm)) AS complete, t2.startDate, t2.endDate, "
						+ "t1.ecpm, t2.cpm, round(1000*t2.budget/t2.cpm) AS targetImpressions, t1.cumImpressions FROM campaignPerformances t1, campaignSettings t2 "
						+ "WHERE t2.startDate <= t1.date AND t1.date <= t2.endDate AND t1.campaignId = t2.campaignId AND t1.goalId = t2.goalId "
						+ "AND t1.date = (SELECT MAX(date) FROM campaignPerformances WHERE a1 IS NOT NULL) ORDER BY t1.campaignName, t1.goalId;");
		ResultSet result = statement.executeQuery(query);
		double cost = 0.0;
		double spent = 0.0;
		double totalBudget = 0.0;
		int totalTarget = 0;
		int totalDelivery = 0;
		while (result.next()) {
			long time = DateUtils.parseTime(result, "startDate");
			String startDate = DateUtils.getDatetime(time).split(" ")[0];
			time = DateUtils.parseTime(result, "endDate");
			String endDate = DateUtils.getDatetime(time).split(" ")[0];
			String type = result.getString("type").toUpperCase();
			boolean isRoas = type.equals("ROAS");
			double cpa = result.getDouble("cpa");
			if (isRoas) {
				cpa = 1.0/cpa;
			}
			double ecpa = result.getDouble("ecpa");
			String secpa = (result.wasNull() ? "    --" : String.format("%7.1f", isRoas ? 1.0/ecpa : ecpa));
			double budget = result.getDouble("budget");
			if (result.getInt("goalId") == 1) {
				cost += result.getDouble("ecpm") * result.getInt("cumImpressions");
				spent += result.getDouble("cpm") * result.getInt("cumImpressions");
				totalBudget += budget;
				totalTarget += result.getInt("targetImpressions");
				totalDelivery += result.getInt("cumImpressions");
			}
			String campaignName = normalizeName(result.getString("campaignName"));
			reportBuilder.append(String.format("%-27s %4d  %4s  %,11.2f  %26s  %6.0f %7s %9.1f  %9.2f   %10s  %10s\n",
					 campaignName, result.getInt("goalId"), type, budget, result.getString("strategy"), cpa, secpa, result.getDouble("margin"), result.getDouble("complete"), startDate, endDate));
		}
		double margin = 100.0 * (1.0 - cost / spent);
		reportBuilder.append(String.format("%-27s %4s  %4s  %,11.2f  %26s  %6s  %6s %9.1f  %9.2f   %10s  %10s\n",
				 "TOTAL", "", "", totalBudget, "", "", "", margin, 100.0 * (double) totalDelivery / (double) totalTarget, "", ""));
		reportBuilder.append("\n");
		connection.close();
		connection = null;
	}
	
	protected void printAdGroupResults(List<Long> adGroupIds) throws SQLException, ParseException {
		if (adGroupIds.isEmpty()) {
			return;
		}
		long currentTime = DateUtils.currentTime();
		reportBuilder.append(String.format("%-65s%3s  %2s  %7s   %9s   %9s  %7s    %s\n", "Ad Group Name", "ATF", "FC", "Max CPM", "Target",
				"Delivery", "Rate", "Report Time"));
		Connection connection = DbUtils.getConnection(Constants.URL_RTB_STATISTICS, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		int totalTarget = 0;
		int totalDelivery = 0;
		int totalRate = 0;
		int adGroupCount = 0;
		String reportDate = DateUtils.getDatetimeUtc(-90);
		for (long adGroupId : adGroupIds) {
			String query = String
					.format("SELECT adGroupName, target, delivery, rate, createDate FROM deliveryReport " +
							"WHERE adGroupId = %d AND createDate > '%s' AND " +
							"delivery = (SELECT MAX(delivery) FROM deliveryReport WHERE adGroupId = %d AND createDate > '%s')" +
							"ORDER BY createDate DESC LIMIT 1;",
							adGroupId, reportDate, adGroupId, reportDate);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				long time = DateUtils.parseDateTime(result, "createDate");
				String date = DateUtils.getDatetimeWithZone(time);
				if (currentTime < time + 3L * Constants.MILLISECONDS_PER_HOUR) {
					adGroupCount++;
					int target = result.getInt("target");
					int delivery = result.getInt("delivery");
					int rate = result.getInt("rate");
					totalTarget += target;
					totalDelivery += delivery;
					totalRate += rate;
					reportBuilder.append(String.format("%-65s%3d  %2d  %7.2f   %,9d   %,9d %,8d    %s\n", result.getString("adGroupName"),
							(atf.get(adGroupId) ? 1 : 0), fc.get(adGroupId), cpm.get(adGroupId), target,
							delivery, rate, date));
				} else {
					reportBuilder.append(String.format("%-62s NO CURRENT DATA AVAILABLE (last update: %s)\n", result.getString("adGroupName"), date));
				}
			}
		}
		if (adGroupCount > 0) {
			reportBuilder.append(String.format("%-65s%3s  %2s  %7s %,11d %,11d  %,7d    %s\n", "TOTAL", "", "", "", totalTarget, totalDelivery, totalRate, ""));
		}
		reportBuilder.append("\n");
		connection.close();
		connection = null;
	}

	protected void printCampaignResults() throws SQLException, ParseException {
		reportBuilder.append(String.format("%-27s%5s  %4s  %10s  %10s  %10s  %10s %6s  %6s  %6s  %5s  %5s  %8s   %s\n", "Campaign Name",
				"Goal", "Type", "Target", "Delivery", "Audit", "Complete-%", "KPI", "eKPI1", "eKPI2", "CPM", "eCPM", "Margin-%",
				"Date"));
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT t1.campaignName, t1.goalId, t2.type, t1.margin, round(1000*t2.budget/t2.cpm) AS targetImpressions, t1.cumImpressions, t1.cumAuditImpressions, 0.01*round(10000*t1.cumImpressions/round(1000*t2.budget/t2.cpm)) AS complete, t2.cpa, 0.001*t2.cpm/t1.a1 AS ecpa1, 0.001*t2.cpm/t1.a2 AS ecpa2, t2.cpm, t1.ecpm, t1.date FROM campaignPerformances t1, campaignSettings t2 WHERE t2.startDate <= t1.date AND t1.date <= t2.endDate AND t1.campaignId = t2.campaignId AND t1.goalId = t2.goalId AND t1.date = (SELECT MAX(date) FROM campaignPerformances WHERE a1 IS NOT NULL) ORDER BY t1.campaignName, t1.goalId;");
		ResultSet result = statement.executeQuery(query);
		double cost = 0.0;
		double spent = 0.0;
		double budget = 0.0;
		int totalTarget = 0;
		int totalDelivery = 0;
		while (result.next()) {
			long time = DateUtils.parseTime(result, "date");
			String date = DateUtils.getDatetime(time).split(" ")[0];
			String type = result.getString("type").toUpperCase();
			boolean isRoas = type.equals("ROAS");
			double cpa = result.getDouble("cpa");
			if (isRoas) {
				cpa = 1.0/cpa;
			}
			double ecpa1 = result.getDouble("ecpa1");
			String secpa1 = (result.wasNull() ? "    --" : String.format("%6.0f", isRoas ? 1.0/ecpa1 : ecpa1));
			double ecpa2 = result.getDouble("ecpa2");
			String secpa2 = (result.wasNull() ? "    --" : String.format("%6.0f", isRoas ? 1.0/ecpa2 : ecpa2));
			if (result.getInt("goalId") == 1) {
				cost += result.getDouble("ecpm") * result.getInt("cumImpressions");
				spent += result.getDouble("cpm") * result.getInt("cumImpressions");
				budget += result.getDouble("cpm") * result.getInt("targetImpressions");
				totalTarget += result.getInt("targetImpressions");
				totalDelivery += result.getInt("cumImpressions");
			}
			String campaignName = normalizeName(result.getString("campaignName"));
			int cumAuditImpressions = result.getInt("cumAuditImpressions");
			String strCumAuditImpressions = (result.wasNull() ? "        --" : String.format("%,10d", cumAuditImpressions));
			reportBuilder.append(String.format("%-27s%5d  %4s  %,10d  %,10d  %10s %10.2f  %6.0f  %6s  %6s  %5.2f  %5.2f %8.1f    %s\n",
					campaignName, result.getInt("goalId"), type, result.getInt("targetImpressions"),
					result.getInt("cumImpressions"), strCumAuditImpressions, result.getDouble("complete"), cpa, secpa1, secpa2, 
					result.getDouble("cpm"), result.getDouble("ecpm"), result.getDouble("margin"), date));
		}
		double margin = 100.0 * (1.0 - cost / spent);
		reportBuilder.append(String.format("%-27s%5s  %4s %,11d %,11d  %10s %10.2f  %6s  %6s  %6s  %5.2f  %5.2f %8.1f    %s\n", "TOTAL", "", "",
				totalTarget, totalDelivery, "", 100.0 * (double) totalDelivery / (double) totalTarget, "", "", "", budget
						/ totalTarget, cost / totalDelivery, margin, ""));
		reportBuilder.append("\n");
		connection.close();
		connection = null;
	}

	protected void printAudiencesResults(List<Long> adGroupIds, List<String> adGroupNames) throws DOException {
		AdWordsData adWordsData = new AdWordsData(adGroupIds, adGroupNames);
		reportBuilder.append(adWordsData.writeAudiencesReport(propertyParameters));
	}
	
	private String normalizeName(String name) {
		return (name.length() > maxCampaignNameLength ? name.substring(0, maxCampaignNameLength) : name);
	}

	protected void test() throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT startDate FROM campaignSettings WHERE campaignId = 4004;");
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long time = DateUtils.parseTime(result, "startDate");
			String date = DateUtils.getDatetime(time);
			log.info(String.format("date: %s", date));
		}
		log.info("");
		connection.close();
		connection = null;
	}

	protected int run() {
		try {
			parseReportProperties();
			
			boolean readAllAdGroups = true;
			List<Long> adGroupIds = getAdGroupIds(readAllAdGroups);
			readAdGroupSettings(adGroupIds);
			readRtbSettings(adGroupIds);
			if (reportType.contains("prm") || reportType.contains("all")) {
				printCampaignSettings();
			}
			if (reportType.contains("adg") || reportType.contains("all")) {
				printAdGroupResults(adGroupIds);
			}
			if (reportType.contains("cpg") || reportType.contains("all")) {
				printCampaignResults();
			}
			if (reportType.contains("aud") || reportType.contains("all")) {
				List<String> adGroupNames = getAdGroupNames(readAllAdGroups);
				printAudiencesResults(adGroupIds, adGroupNames);
			}
			if (reportBuilder.length() > 0) {
				return 0;
			} else {
				log.error("Report is empty: type = " + reportType);
				return 1;
			}
		} catch (Exception e) {
			log.error("ERROR: Report Exception (run): ", e);
			return 1;
		}
	}

	protected String getString() {
		return reportBuilder.toString();
	}

	public static void main(String[] args) {
		Report report = new Report(args);
		System.out.println(report.run() == 0 ? report.getString() : "ERROR: No report generated");
	}

	public static String getReport(Map<String, String> vars) {
		com.sharethis.common.helper.AppConfig appConfig = AppConfig.getInstance();
		String[] args = new String[4];
		args[0] = "-dop";
		args[1] = appConfig.get("-dop");
		args[2] = "-" + Constants.REPORT_TYPE_PROPERTY;
		args[3] = (vars.containsKey(Constants.REPORT_TYPE_PROPERTY) ? vars.get(Constants.REPORT_TYPE_PROPERTY) : "all");
		Report report = new Report(args);
		return (report.run() == 0 ? report.getString() : "ERROR: No report generated");
	}
}
