package com.sharethis.delivery.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import com.sharethis.common.helper.AppConfig;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.StatusType;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Report extends Runner {
	private final static int maxCampaignNameLength = 27;
	private final static int maxAdGroupNameLength = 63;
	private StringBuilder reportBuilder;
	private Map<Long, Double> cpm;
	private Map<Long, Integer> fc;
	private Map<Long, Boolean> atf;
	private String reportType;
	private String userFilter;
	private String reportTime;
	private String notes = "";

	private Report(String[] args) {
		initialize(args);
		reportBuilder = new StringBuilder();
	}

	private void parseReportProperties() throws DOException {
		options.addOption(Constants.REPORT_TYPE_PROPERTY, true, "report type (adg, cpg, aud, all)");
		options.addOption(Constants.REPORT_USER_PROPERTY, true, "report user");
		options.addOption(Constants.REPORT_TIME_PROPERTY, true, "report time");
		parseProperties();
		reportType = line.getOptionValue(Constants.REPORT_TYPE_PROPERTY).toLowerCase();
		String reportUser = line.getOptionValue(Constants.REPORT_USER_PROPERTY);
		userFilter = (reportUser.equals("all") ? "" : " AND t2.owner = '" + reportUser + "'");
		reportTime = line.getOptionValue(Constants.REPORT_TIME_PROPERTY);
		if (reportTime.equalsIgnoreCase(Constants.DEFAULT)) {
			reportTime = DateUtils.getDatetime();
		} else if (reportTime.equalsIgnoreCase(Constants.MIDNIGHT)) {
			reportTime = DateUtils.getDate(-1) + " 23:55:00";
		}
		else {
			reportTime = reportTime.replace("_", " ");
		}
	}

	protected void readAdGroupSettings(Map<Long, String> adGroupIds) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		cpm = new HashMap<Long, Double>();
		fc = new HashMap<Long, Integer>();
		for (Map.Entry<Long, String> entry : adGroupIds.entrySet()) {
			long adGroupId = entry.getKey();
			String adGroupName = entry.getValue();
			String query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS,
					adGroupId);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				double maxCpm = result.getDouble("maxCpm");
				cpm.put(adGroupId, maxCpm);
				int frequencyCap = result.getInt("frequencyCap");
				fc.put(adGroupId, frequencyCap);
			} else {
				notes += String.format("   %s (%d) not set on DO: Ad group has never delivered or is paused on optimizer?\n", adGroupName, adGroupId);
				cpm.put(adGroupId, -1.0);
			}
		}
	}

	protected void readRtbSettings(Map<Long, String> adGroupIds) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_RTB);
		atf = new HashMap<Long, Boolean>();
		for (Map.Entry<Long, String> entry : adGroupIds.entrySet()) {
			long adGroupId = entry.getKey();
			String adGroupName = entry.getValue();
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
				notes += String.format("   %s (%d) not set on RTB: Ad group paused on adx?\n", adGroupName, adGroupId);
				atf.put(adGroupId, false);
			}
		}
	}

	protected void printCampaignSettings() throws SQLException, ParseException {
		reportBuilder.append(String.format("%-27s %4s  %4s  %11s  %26s  %6s %7s %10s %10s  %-10s  %-10s\n", "Campaign Name", "Goal",
				"Type", "Budget", "Strategy", "KPI", "eKPI", "Margin-%", "Complete-%", "Start Date", "End Date"));
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String
				.format("SELECT t1.campaignName, t1.goalId, t2.type, t2.budget, t2.strategy, t2.kpi, t1.ekpi, t1.margin, "
						+ "0.01*round(10000*t1.cumDeliveryImpressions/round(1000*t2.budget/t2.cpm)) AS complete, t2.startDate, t2.endDate, "
						+ "t1.ecpm, t2.cpm, round(1000*t2.budget/t2.cpm) AS targetImpressions, t1.cumDeliveryImpressions FROM %s t1, %s t2 "
						+ "WHERE t2.startDate <= t1.date AND t1.date <= t2.endDate AND t1.campaignId = t2.campaignId AND t1.goalId = t2.goalId "
						+ "AND t1.date = (SELECT MAX(date) FROM %s WHERE status > 0) %s ORDER BY t1.campaignName, t1.goalId;",
						Constants.DO_CAMPAIGN_PERFORMANCES, Constants.DO_CAMPAIGN_SETTINGS, Constants.DO_CAMPAIGN_PERFORMANCES, userFilter);
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
			double kpi = result.getDouble("kpi");
			if (isRoas) {
				kpi = 1.0/kpi;
			}
			double ekpi = result.getDouble("ekpi");
			String sekpi = "    --";
			if (!result.wasNull() && !(isRoas && ekpi == 0)) {
				double value = (isRoas ? 1.0/ekpi : ekpi);
				sekpi = (value >= 1e+5 || value <= 1e-5 ? String.format("%7.1e", value) : String.format("%7.1f", value));
			}
			double budget = result.getDouble("budget");
			if (result.getInt("goalId") == 1) {
				cost += result.getDouble("ecpm") * result.getInt("cumDeliveryImpressions");
				spent += result.getDouble("cpm") * result.getInt("cumDeliveryImpressions");
				totalBudget += budget;
				totalTarget += result.getInt("targetImpressions");
				totalDelivery += result.getInt("cumDeliveryImpressions");
			}
			String campaignName = normalizeName(result.getString("campaignName"), maxCampaignNameLength);
			reportBuilder.append(String.format("%-27s %4d  %4s  %,11.2f  %26s  %6.1f %7s %9.1f  %9.2f   %10s  %10s\n",
					 campaignName, result.getInt("goalId"), type, budget, result.getString("strategy"), kpi, sekpi, result.getDouble("margin"), result.getDouble("complete"), startDate, endDate));
		}
		double margin = 100.0 * (1.0 - cost / spent);
		reportBuilder.append(String.format("%-27s %4s  %4s  %,11.2f  %26s  %6s  %6s %9.1f  %9.2f   %10s  %10s\n",
				 "TOTAL", "", "", totalBudget, "", "", "", margin, 100.0 * (double) totalDelivery / (double) totalTarget, "", ""));
		reportBuilder.append("\n");
	}
	
	protected void printAdGroupResults(Map<Long, String> adGroupIds, String reportTime) throws SQLException, ParseException {
		if (adGroupIds.isEmpty()) {
			return;
		}
		reportBuilder.append(String.format("%-63s%3s  %2s  %7s   %9s   %9s  %9s    %s\n", "Ad Group Name", "ATF", "FC", "Max CPM", "Target",
				"Delivery", "Rate", "Report Time"));
		Statement statement = DbUtils.getStatement(Constants.URL_RTB_STATISTICS);
		int totalTarget = 0;
		int totalDelivery = 0;
		int totalRate = 0;
		int adGroupCount = 0;
		String reportUtcTime = DateUtils.getDatetimeUtc(reportTime);
		long reportLocalTime = DateUtils.parseTime(reportTime);
		for (long adGroupId : adGroupIds.keySet()) {
			String query = String
					.format("SELECT adGroupName, target, delivery, rate, createDate FROM %s WHERE adGroupId = %d AND createDate <= '%s' ORDER BY createDate DESC LIMIT 1;",
							Constants.RTB_STATISTICS_DB_DELIVERY_REPORT, adGroupId, reportUtcTime);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				long time = DateUtils.parseDateTime(result, "createDate");
				String date = DateUtils.getDatetimeWithZone(time);
				String adGroupName = normalizeName(result.getString("adGroupName"), maxAdGroupNameLength);
				if (reportLocalTime < time + 3L * Constants.MILLISECONDS_PER_HOUR) {
					adGroupCount++;
					int target = result.getInt("target");
					int delivery = result.getInt("delivery");
					int rate = result.getInt("rate");
					totalTarget += target;
					totalDelivery += delivery;
					totalRate += rate;
					String strFc =  (fc.get(adGroupId) == null ? "--" : String.format("%2d", fc.get(adGroupId)));
					String strCpm = (cpm.get(adGroupId) < 0 ? "--" : String.format("%7.2f", cpm.get(adGroupId)));
					reportBuilder.append(String.format("%-63s%3d  %2s  %7s %,11d %,11d  %,9d    %s\n", adGroupName,
							(atf.get(adGroupId) ? 1 : 0), strFc, strCpm, target, delivery, rate, date));
				} else {
					reportBuilder.append(String.format("%-63sPAUSED ON ADX: NO CURRENT DATA AVAILABLE (last update: %s)\n", adGroupName, date));
				}
			}
		}
		if (adGroupCount > 0) {
			reportBuilder.append(String.format("%-63s%3s  %2s  %7s %,11d %,11d  %,9d    %s\n", "TOTAL", "", "", "", totalTarget, totalDelivery, totalRate, ""));
		}
		reportBuilder.append("\n");
	}

	protected void printCampaignResults() throws SQLException, ParseException {
		reportBuilder.append(String.format("%-27s%5s  %4s  %10s  %10s  %10s  %10s %6s  %6s  %6s  %5s  %5s  %8s   %s\n", "Campaign Name",
				"Goal", "Type", "Target", "Delivery", "Audit", "Complete-%", "KPI", "eKPI1", "eKPI2", "CPM", "eCPM", "Margin-%",
				"Date"));
		Statement statement = DbUtils.getNewStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String
				.format("SELECT t1.id, t1.campaignName, t1.goalId, t1.status, t2.type, t1.margin, round(1000*t2.budget/t2.cpm) AS targetImpressions, t1.cumDeliveryImpressions, t1.cumAuditImpressions, 0.01*round(10000*t1.cumDeliveryImpressions/round(1000*t2.budget/t2.cpm)) AS complete, t2.kpi, t2.cpm, t1.ecpm, t1.date FROM %s t1, %s t2 WHERE t2.startDate <= t1.date AND t1.date <= t2.endDate AND t1.campaignId = t2.campaignId AND t1.goalId = t2.goalId AND t1.date = (SELECT MAX(date) FROM %s WHERE status) %s ORDER BY t1.campaignName, t1.goalId;", Constants.DO_CAMPAIGN_PERFORMANCES, Constants.DO_CAMPAIGN_SETTINGS, Constants.DO_CAMPAIGN_PERFORMANCES, userFilter);
		ResultSet result = statement.executeQuery(query);
		double cost = 0.0;
		double spent = 0.0;
		double budget = 0.0;
		int totalTarget = 0;
		int totalDelivery = 0;
		while (result.next()) {
			long id = result.getLong("id");
			long time = DateUtils.parseTime(result, "date");
			String date = DateUtils.getDatetime(time).split(" ")[0];
			String type = result.getString("type").toUpperCase();
			double ecpm = result.getDouble("ecpm");
			double cpm = result.getDouble("cpm");
			boolean isRoas = type.equals("ROAS");
			double kpi = result.getDouble("kpi");
			if (isRoas) {
				kpi = 1.0/kpi;
			}

			if (result.getInt("goalId") == 1) {
				cost += ecpm * result.getInt("cumDeliveryImpressions");
				spent += cpm * result.getInt("cumDeliveryImpressions");
				budget += cpm * result.getInt("targetImpressions");
				totalTarget += result.getInt("targetImpressions");
				totalDelivery += result.getInt("cumDeliveryImpressions");
			}
			int cumAuditImpressions = result.getInt("cumAuditImpressions");
			String campaignName = normalizeName(result.getString("campaignName"), maxCampaignNameLength);
			int goalId = result.getInt("goalId");
			int targetImpressions = result.getInt("targetImpressions");
			int cumDeliveryImpressions = result.getInt("cumDeliveryImpressions");
			double complete = result.getDouble("complete");
			double margin = result.getDouble("margin");
			
			String strCumAuditImpressions = (StatusType.parseStatus(result).getAudit() ? String.format("%,11d", cumAuditImpressions) : "        --");
			
			double[] ratios = getRatios(type, id);
			double ekpi1 = 0.001 * cpm / ratios[0];
			String sekpi1 = (ratios[0] == 0 ? "    --" : String.format("%6.1f", isRoas ? 1.0/ekpi1 : ekpi1));
			double ekpi2 = 0.001 * cpm / ratios[1];
			String sekpi2 = (ratios[1] == 0 ? "    --" : String.format("%6.1f", isRoas ? 1.0/ekpi2 : ekpi2));
			
			reportBuilder.append(String.format("%-27s%5d  %4s %,11d %,11d %11s %10.2f  %6.1f  %6s  %6s  %5.2f  %5.2f %8.1f    %s\n",
					campaignName, goalId, type, targetImpressions,
					cumDeliveryImpressions, strCumAuditImpressions, complete, kpi, sekpi1, sekpi2, 
					cpm, ecpm, margin, date));
		}
		double margin = 100.0 * (1.0 - cost / spent);
		reportBuilder.append(String.format("%-27s%5s  %4s %,11d %,11d  %10s %10.2f  %6s  %6s  %6s  %5.2f  %5.2f %8.1f    %s\n", "TOTAL", "", "",
				totalTarget, totalDelivery, "", 100.0 * (double) totalDelivery / (double) totalTarget, "", "", "", budget
						/ totalTarget, cost / totalDelivery, margin, ""));
		reportBuilder.append("\n");
	}
	
	private double[] getRatios(String type, long id) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		double[] ratios = new double[2];
		String query = String.format("SELECT segmentId, clickRatio, conversionRatio FROM %s WHERE campaignRecordId = %d;", Constants.DO_SEGMENT_PERFORMANCES, id);
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			int segmentId = result.getInt("segmentId");
			double ratio = (type.equalsIgnoreCase("CPC") ? result.getDouble("clickRatio") : result.getDouble("conversionRatio"));
			ratios[segmentId] = ratio;
		}
		return ratios;
	}

	protected void printAudiencesResults(Map<Long, String> adGroupIds) throws DOException, ParseException {
		AdWordsData adWordsData = new AdWordsData(adGroupIds);
		reportBuilder.append(adWordsData.writeAudiencesReport(propertyParameters));
	}

	protected void printNotes() {
		if (notes.length() > 0) {
			reportBuilder.append("Notes:\n").append(notes).append("\n");
		}
	}
	
	private String normalizeName(String name, int maxLength) {
		return (name.length() > maxLength ? name.substring(0, maxLength) : name);
	}

	
	protected void test() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT startDate FROM campaignSettings WHERE campaignId = 4004;");
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long time = DateUtils.parseTime(result, "startDate");
			String date = DateUtils.getDatetime(time);
			log.info(String.format("date: %s", date));
		}
		log.info("");
	}

	protected int run() {
		try {
			parseReportProperties();
			
			boolean readAllAdGroups = true;
			Map<Long, String> adGroupIds = getAdGroupIds(readAllAdGroups, userFilter);
//			Map<String> adGroupNames = getAdGroupNames(readAllAdGroups);
			readAdGroupSettings(adGroupIds);
			readRtbSettings(adGroupIds);
			printNotes();
			if (reportType.contains("prm") || reportType.contains("all")) {
				printCampaignSettings();
			}
			if (reportType.contains("adg") || reportType.contains("all")) {
				printAdGroupResults(adGroupIds, reportTime);
			}
			if (reportType.contains("cpg") || reportType.contains("all")) {
				printCampaignResults();
			}
			if (reportType.contains("aud") || reportType.contains("all")) {
				printAudiencesResults(adGroupIds);
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
		log.info(report.run() == 0 ? report.getString() : "ERROR: No report generated");
	}

	public static String getReport(Map<String, String> vars) {
		com.sharethis.common.helper.AppConfig appConfig = AppConfig.getInstance();
		String[] args = new String[8];
		args[0] = "-dop";
		args[1] = appConfig.get("-dop");
		args[2] = "-" + Constants.REPORT_TYPE_PROPERTY;
		args[3] = (vars.containsKey(Constants.REPORT_TYPE_PROPERTY) ? vars.get(Constants.REPORT_TYPE_PROPERTY) : "all");
		args[4] = "-" + Constants.REPORT_USER_PROPERTY;
		args[5] = (vars.containsKey(Constants.REPORT_TYPE_PROPERTY) ? vars.get(Constants.REPORT_USER_PROPERTY) : "all");
		args[6] = "-" + Constants.REPORT_TIME_PROPERTY;
		args[7] = (vars.containsKey(Constants.REPORT_TIME_PROPERTY) ? vars.get(Constants.REPORT_TIME_PROPERTY) : "default");
		Report report = new Report(args);
		return (report.run() == 0 ? report.getString() : "ERROR: No report generated");
	}
}
