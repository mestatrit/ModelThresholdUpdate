package com.sharethis.performance.report;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.api.adwords.lib.AdWordsService;
import com.google.api.adwords.lib.AdWordsUser;
import com.google.api.adwords.v201302.cm.AdGroup;
import com.google.api.adwords.v201302.cm.AdGroupPage;
import com.google.api.adwords.v201302.cm.AdGroupServiceInterface;
import com.google.api.adwords.v201302.cm.BiddingStrategyConfiguration;
import com.google.api.adwords.v201302.cm.CpcBid;
import com.google.api.adwords.v201302.cm.DateRange;
import com.google.api.adwords.v201302.cm.Predicate;
import com.google.api.adwords.v201302.cm.PredicateOperator;
import com.google.api.adwords.v201302.cm.Selector;
import com.google.api.adwords.v201302.cm.Stats;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Metric;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.input.AdWordsData;
import com.sharethis.delivery.job.Runner;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.StringUtils;

public class Report extends Runner {
	private StringBuilder reportBuilder;
	private Map<Long, Metric> deliveredMetrics;
	private Map<Long, Boolean> model;
	private Map<Long, String> network;
	private Map<Long, Integer> impressionTarget;
	private Map<Long, Double> maxPrice;
	private Map<Long, Double> minCtr;
	private Map<Long, Integer> atf;
	private Map<Long, String> targetDevice;
	private Map<Integer, String> networkNames;
	private String startDate, endDate;
	private int minClk, minImp;
	private String data;
	private String ntwk;

	private String[] devices = new String[]{"Phone","Tablet","Desktop","Others"};
	
	private Report(String[] args) {
		initialize(args);
		reportBuilder = new StringBuilder();
		networkNames = new HashMap<Integer, String>();
		networkNames.put(1, "ADX");
		networkNames.put(5, "ANX");
		
	}

	private void parseReportProperties() throws DOException {
		options.addOption(Constants.REPORT_STARTDATE_PROPERTY, true, "start date");
		options.addOption(Constants.REPORT_ENDDATE_PROPERTY, true, "end date");
		options.addOption(Constants.REPORT_MINCLK_PROPERTY, true, "min click");
		options.addOption(Constants.REPORT_MINIMP_PROPERTY, true, "min impression");
		options.addOption(Constants.REPORT_NETWORK_PROPERTY, true, "network");
		options.addOption(Constants.INPUT_DATA_PROPERTY, true, "data");
		parseProperties();
		startDate = line.getOptionValue(Constants.REPORT_STARTDATE_PROPERTY);
		endDate = line.getOptionValue(Constants.REPORT_ENDDATE_PROPERTY);
		minClk = Integer.parseInt(line.getOptionValue(Constants.REPORT_MINCLK_PROPERTY));
		minImp = Math.max(10, Integer.parseInt(line.getOptionValue(Constants.REPORT_MINIMP_PROPERTY)));
		ntwk = line.getOptionValue(Constants.REPORT_NETWORK_PROPERTY).toUpperCase();
		if (!ntwk.equals("ALL") && !networkNames.containsValue(ntwk)) {
			throw new DOException("Unknown network argument: should be ALL, ADX, or ANX");
		}
		data = line.getOptionValue(Constants.INPUT_DATA_PROPERTY);
	}
	
	protected Map<Long, String> getAdGroupIds() throws SQLException {
		String networkCondition = "> 0";
		for (int k : networkNames.keySet()) {
			if (networkNames.get(k).equals(ntwk)) {
				networkCondition = String.format("= %d", k);
				break;
			}
		}
		Statement statement = DbUtils.getStatement(Constants.URL_RTB);
		String query = String
				.format("SELECT DISTINCT rtb.adGroupId AS adGroupId, adg.name AS adGroupName FROM RTB.%s rtb, adplatform.%s adg WHERE rtb.adGroupId = adg.networkAdgId AND (rtb.optimizationFlags & %s) > 0 AND adg.networkId %s AND adg.networkAdgId > 15 ORDER BY adg.name;", 
						Constants.RTB_AD_GROUP_PROPERTY, Constants.ADPLATFORM_DB_AD_GROUP, Constants.CTR_OPTIMIZATION, networkCondition);
		ResultSet result = statement.executeQuery(query);
		System.out.println("Query: " + result.toString());
		Map<Long, String> adGroupIds = new LinkedHashMap<Long, String>();
		while (result.next()) {
			adGroupIds.put(result.getLong("adGroupId"), result.getString("adGroupName"));
		}
		return adGroupIds;
	}
	
	public Map<Long, String> prune(Map<Long, String> adGroupIds) throws DOException, ParseException {
		Map<Long, String> ids = new LinkedHashMap<Long, String>();
		for (long adGroupId : adGroupIds.keySet()) {
			if (model.get(adGroupId)) {
				String adGroupName = adGroupIds.get(adGroupId);
				ids.put(adGroupId, adGroupName);
			}
		}
		return ids;
	}
	
	public boolean readDeliveredMetrics(Map<Long, String> adGroupIds) throws DOException, ParseException, SQLException {
		if (data.equalsIgnoreCase("adx")) {
			System.out.println("AdX Report");
			AdWordsData adWordsData = new AdWordsData(adGroupIds);
			deliveredMetrics = adWordsData.readDeliveryMetrics(startDate, endDate, propertyParameters);
		} else {
			deliveredMetrics = readDeliveryMetricsRtb(adGroupIds, startDate, endDate, propertyParameters);	
		}
		return true;
	}
	
	public Map<Long, Metric> readDeliveryMetricsRtb(Map<Long, String> adGroupIds, String startDate, String endDate, Parameters propertyParameters) throws SQLException, ParseException {
		String start = DateUtils.parseDatetimeUtc(startDate, "00:00:00");
		String end = DateUtils.parseDatetimeUtc(endDate, "23:59:59");
		System.out.println("ShareThis Report");
		Map<Long, Metric> deliveredMetrics = new HashMap<Long, Metric>();
		Statement rtbStatement = DbUtils.getStatement(Constants.URL_RTB_CAMPAIGN_STATISTICS);
		for (Map.Entry<Long, String> entry : adGroupIds.entrySet()) {
			long adGroupId = entry.getKey();
			String adGroupName = entry.getValue();
			String query = String.format("SELECT SUM(imprcnt) AS impressions, SUM(clkcnt) AS clicks, 0.001*SUM(cost) AS cost, SUM(nullCostCount) AS costCountCorrection FROM %s WHERE adgId = %d AND date >= '%s' AND date < '%s';",
					Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS, adGroupId, start, end);
			ResultSet result = rtbStatement.executeQuery(query);
			if (result.next()) {
				double impressions = (double) result.getInt("impressions");
				double costCountCorrection = (double) result.getInt("costCountCorrection");
				double clicks = (double) result.getInt("clicks");
				double cost = result.getDouble("cost");
				if (impressions > costCountCorrection) {
					cost *= impressions / (impressions - costCountCorrection);
				} else {
					cost = 0.0;
				}
				Metric metric = new Metric(impressions, clicks, 0.0, cost);
				metric.setTime(0L);
				deliveredMetrics.put(adGroupId, metric);
			} else {
				throw new SQLException(String.format("adGroup = %s (%d) returns no rows from %s", adGroupName, adGroupId,
						Constants.RTB_CAMPAIGN_STATISTICS_DB_ADGROUP_STATS));
			}
		}
		return deliveredMetrics;
	}
	
	protected void printAdGroupResults(Map<Long, String> adGroupIds) throws SQLException, ParseException {
		if (adGroupIds.isEmpty()) {
			return;
		}
		int count = 0;
		int success = 0;
		int totClks = 0;
		int totImps = 0;
		double totCost = 0.0;
		int numDevice = devices.length;
		long[] totClks_device = new long[numDevice];
		long[] totImps_device = new long[numDevice];
		double[] totCost_device = new double[numDevice];
		for(int i_device=0; i_device<numDevice; i_device++){
			totClks_device[i_device] = 0;
			totImps_device[i_device] = 0;
			totCost_device[i_device] = 0;
		}
		
		reportBuilder.append(String.format("%15s  %-100s  %5s  %5s  %5s  %8s  %12s  %11s  %7s  %6s  %3s  %6s   %12s\n", 
				"adGroupId", "AdGroupName", "CTR-%", "eCPC", "eCPM", "Clicks", "Impressions", "Target", "Network", 
				"MaxCPM", "ATF", "MinCTR", "TargetDevice"));
		for (long adGroupId : adGroupIds.keySet()) {
			String adGroupName = adGroupIds.get(adGroupId);
			Metric metric = (deliveredMetrics.containsKey(adGroupId) ? deliveredMetrics.get(adGroupId) : new Metric());
			double ctr = 100.0 * metric.getClickRatio();
			int clks = (int) Math.round(metric.getClicks());
			int imps = (int) Math.round(metric.getImpressions());
			double eCpm = metric.getEcpm();
			double eCpc = metric.getEcpc();
			int target = impressionTarget.get(adGroupId);
			double price = maxPrice.get(adGroupId);
			double ctrThreshold = minCtr.get(adGroupId);
			int flag = atf.get(adGroupId);
			String adGroupNtwk = network.get(adGroupId);
			boolean validNetwork = (adGroupNtwk != null && (ntwk.equals("ALL") || ntwk.equals(adGroupNtwk)));
			String tDevice = targetDevice.get(adGroupId);
			String row = String.format("%15s  %-100s  %5.3f  %5.2f  %5.2f  %,8d  %,12d  %,11d  %7s  %6.2f  %2d   %6.2f   %12s\n", 
					adGroupId, adGroupName, ctr, eCpc, eCpm, clks, imps, target, adGroupNtwk, price, flag, ctrThreshold, tDevice);
			if (clks >= minClk && imps >= minImp && model.get(adGroupId) && validNetwork) {
				reportBuilder.append(row);
				count++;
				success += (ctr >= 0.1 ? 1 : 0);
				totClks += clks;
				totImps += imps;
				totCost += imps * eCpm;
				for(int i_device=0; i_device<numDevice-1; i_device++){
					if(devices[i_device].equalsIgnoreCase(tDevice)){
						totClks_device[i_device] += clks;
						totImps_device[i_device] += imps;
						totCost_device[i_device] += imps * eCpm;
					}
				}
			}
		}
		
		long totClks_tmp = 0;
		long totImps_tmp = 0;
		long totCost_tmp = 0;
		for(int i_device=0; i_device<numDevice-1; i_device++){
			totClks_tmp += totClks_device[i_device];
			totImps_tmp += totImps_device[i_device];
			totCost_tmp += totCost_device[i_device];		
		}

		totClks_device[numDevice-1] = totClks - totClks_tmp;
		totImps_device[numDevice-1] = totImps - totImps_tmp;
		totCost_device[numDevice-1] = totCost - totCost_tmp;
		
		String row = String.format("%15s  %-100s  %5.3f  %5.2f  %5.2f  %,8d  %,12d\n", "Overall", "Total",
				100.0*totClks/totImps, 0.001*totCost/(double)totClks, totCost/(double)totImps, totClks, totImps);
		reportBuilder.append(row);
		for(int i_device=0; i_device<numDevice; i_device++){
			String row_device = String.format("%15s  %-100s  %5.3f  %5.2f  %5.2f  %,8d  %,12d\n", "Overall", devices[i_device],
					100.0*totClks_device[i_device]/totImps_device[i_device], 0.001*totCost_device[i_device]/(double)totClks_device[i_device], 
					totCost_device[i_device]/(double)totImps_device[i_device], totClks_device[i_device], totImps_device[i_device]);
			reportBuilder.append(row_device);
		}		
		row = String.format("%15s  %-100s  %,5d\n", "Overall", "AdGroupCount", count);
		reportBuilder.append(row);
		row = String.format("%15s  %-100s  %4.0f%%\n", "Overall", "Ratio(CTR>=0.1%)", 100.0*(double)success/(double)count);
		reportBuilder.append(row);
	}
	
	protected void readRtbSettings(Map<Long, String> adGroupIds) throws SQLException {
		Statement rtbStatement = DbUtils.getStatement(Constants.URL_RTB);
		Statement adStatement = DbUtils.getStatement(Constants.URL_ADPLATFORM);
		model = new HashMap<Long, Boolean>();
		impressionTarget = new HashMap<Long, Integer>();
		maxPrice = new HashMap<Long, Double>();
		minCtr = new HashMap<Long, Double>();
		atf = new HashMap<Long, Integer>();
		network = new HashMap<Long, String>();
		targetDevice = new HashMap<Long, String>();
		for (Map.Entry<Long, String> entry : adGroupIds.entrySet()) {
			long adGroupId = entry.getKey();
			String query = String.format("SELECT optimizationFlags, minCTR4AdGroup as minCtr FROM %s WHERE adGroupId = %d;",
					Constants.RTB_AD_GROUP_PROPERTY, adGroupId);
			ResultSet result = rtbStatement.executeQuery(query);
			if (result.next()) {
				long optimizationFlags = result.getLong("optimizationFlags");
				double ctr = 100.0 * result.getDouble("minCtr");
//				model.put(adGroupId, (optimizationFlags & Constants.CTR_OPTIMIZATION) != 0);
				model.put(adGroupId, (optimizationFlags & (Constants.CTR_OPTIMIZATION + 0*Constants.CTR_LEARNER)) != 0);
				if (result.next()) {
					throw new SQLException(String.format("adGroupId = %d returns more than one row from %s", adGroupId,
							Constants.RTB_AD_GROUP_PROPERTY));
				}
				atf.put(adGroupId, (optimizationFlags & Constants.ATF_PLACEMENT) != 0 ? 1 : 0);
				minCtr.put(adGroupId, ctr);
			} else {
				model.put(adGroupId, false);
				atf.put(adGroupId, 0);
			}
			
			String day = DateUtils.getShortDayOfWeek(0);
			query = String.format("SELECT impr%s AS target FROM RTB_AdGroupDelivery WHERE adGroupId = %d;",
					day, adGroupId);
			result = rtbStatement.executeQuery(query);
			if (result.next()) {
				int target = result.getInt("target");
				impressionTarget.put(adGroupId, target);
				if (result.next()) {
					throw new SQLException(String.format("adGroupId = %d returns more than one row from %s", adGroupId,
							"RTB_AdGroupDelivery"));
				}
			} else {
				impressionTarget.put(adGroupId, 0);
			}
			
			query = String.format("SELECT name, maxPrice, networkId FROM adgroup WHERE networkAdgId = %d;", adGroupId);
			result = adStatement.executeQuery(query);
			if (result.next()) {
				adGroupIds.put(adGroupId, result.getString("name"));
				double price = 1.0e-6 * (double) result.getInt("maxPrice");
				maxPrice.put(adGroupId, price);
				int networkId = result.getInt("networkId");
				network.put(adGroupId, networkNames.get(networkId));
				if (result.next()) {
					throw new SQLException(String.format("adGroupId = %d returns more than one row from %s", adGroupId,
							"adgroup"));
				}
			} else {
				maxPrice.put(adGroupId, 0.0);
			}
			
			//acquiring the targeted device fro ad group
			//select adGroupId, networkId, networkCriterionId, adGroupCriterionType, criterionType, criterionId from adplatform.AdGroup2MobileTargetView;
			//adGroupCriterionType='device'
			query = String.format("SELECT criterionType FROM AdGroup2MobileTargetView "
					+ "WHERE adGroupId = %d and adGroupCriterionType = 'device';", adGroupId);
			result = adStatement.executeQuery(query);
			String tDeviceList = null;
			if (result.next()) {
				tDeviceList = result.getString("criterionType");
				while (result.next()) {
					tDeviceList = tDeviceList +","+result.getString("criterionType");
				}				
				targetDevice.put(adGroupId, tDeviceList);
			} else {
				targetDevice.put(adGroupId, tDeviceList);
			}
		}
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
			Map<Long, String> adGroupIds = getAdGroupIds();
			readRtbSettings(adGroupIds);
			adGroupIds = prune(adGroupIds);
			readDeliveredMetrics(adGroupIds);
			printAdGroupResults(adGroupIds);
			if (reportBuilder.length() > 0) {
				return 0;
			} else {
				log.error("Report is empty");
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
}
