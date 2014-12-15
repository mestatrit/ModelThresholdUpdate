package com.sharethis.delivery.input;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.rpc.ServiceException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.api.adwords.lib.AdWordsService;
import com.google.api.adwords.lib.AdWordsUser;
import com.google.api.adwords.lib.AuthToken;
import com.google.api.adwords.lib.AuthTokenException;
import com.google.api.adwords.lib.utils.v201302.ReportDownloadResponse;
import com.google.api.adwords.lib.utils.v201302.ReportUtils;
import com.google.api.adwords.v201302.cm.AdGroup;
import com.google.api.adwords.v201302.cm.AdGroupOperation;
import com.google.api.adwords.v201302.cm.AdGroupPage;
import com.google.api.adwords.v201302.cm.AdGroupServiceInterface;
import com.google.api.adwords.v201302.cm.ApiException;
import com.google.api.adwords.v201302.cm.Campaign;
import com.google.api.adwords.v201302.cm.CampaignPage;
import com.google.api.adwords.v201302.cm.CampaignServiceInterface;
import com.google.api.adwords.v201302.cm.DateRange;
//import com.google.api.adwords.v201302.cm.ManualCPMAdGroupBids;
import com.google.api.adwords.v201302.cm.BiddingStrategyConfiguration;
import com.google.api.adwords.v201302.cm.Bids;
import com.google.api.adwords.v201302.cm.CpcBid;
import com.google.api.adwords.v201302.cm.Money;
import com.google.api.adwords.v201302.cm.Operator;
import com.google.api.adwords.v201302.cm.OrderBy;
import com.google.api.adwords.v201302.cm.Predicate;
import com.google.api.adwords.v201302.cm.PredicateOperator;
import com.google.api.adwords.v201302.cm.Selector;
import com.google.api.adwords.v201302.cm.SortOrder;
import com.google.api.adwords.v201302.cm.Stats;
import com.google.api.adwords.v201302.cm.UserList;
import com.google.api.adwords.v201302.cm.UserListPage;
import com.google.api.adwords.v201302.cm.UserListServiceInterface;
import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Metric;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.Sort;
import com.sharethis.delivery.util.StringUtils;

public class AdWordsData {
	private static final Logger log = Logger.getLogger(Constants.ADX_LOGGER_NAME);
	private static final String number = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private List<Long> adGroupIds;
	private List<String> adGroupNames;
	private Map<Long, Double> oldCpm, newCpm;
	private Map<Long, Integer> oldFC;
	private Map<Long, Long> campaignIds;
	private Map<Long, String> oldAdGroupName, newAdGroupName;
	private boolean logReadAdGroups;
	private Errors errors = new Errors();

	public AdWordsData(Map<Long, String> adGroups) {
		adGroupIds = new ArrayList<Long>();
		adGroupNames = new ArrayList<String>();
		for (Map.Entry<Long, String> entry : adGroups.entrySet()) {
			adGroupIds.add(entry.getKey());
			adGroupNames.add(entry.getValue());
		}
		this.oldCpm = new HashMap<Long, Double>();
		this.oldAdGroupName = new HashMap<Long, String>();
		this.oldFC = new HashMap<Long, Integer>();
		this.campaignIds = new HashMap<Long, Long>();
		this.logReadAdGroups = true;
		log.setLevel(Level.INFO);
	}

	public AdWordsData(Map<Long, Double> newCpm, Map<Long, String> newAdGroupName) {
		this.adGroupIds = Sort.byValue(newAdGroupName, "asc");
		this.campaignIds = new HashMap<Long, Long>();
		this.oldCpm = new HashMap<Long, Double>();
		this.oldAdGroupName = new HashMap<Long, String>();
		this.oldFC = new HashMap<Long, Integer>();
		this.newCpm = newCpm;
		this.newAdGroupName = newAdGroupName;
		this.logReadAdGroups = false;
		log.setLevel(Level.INFO);
	}

	private String[] listToArray(List<Long> alist) {
		String[] array = new String[alist.size()];
		for (int i = 0; i < alist.size(); i++) {
			array[i] = alist.get(i).toString();
		}
		return array;
	}

	private String[] setToArray(Set<String> aset) {
		String[] array = aset.toArray(new String[aset.size()]);
		return array;
	}

	private AdWordsUser getAdWordsUser(Parameters propertyParameters) throws ServiceException, AuthTokenException {
		String email = propertyParameters.get(Constants.ADX_EMAIL);
		String password = propertyParameters.get(Constants.ADX_PASSWORD);
		String clientId = propertyParameters.get(Constants.ADX_CLIENT_ID);
		String userAgent = propertyParameters.get(Constants.ADX_USER_AGENT);
		String developerToken = propertyParameters.get(Constants.ADX_DEVELOPER_TOKEN);
		boolean useSandbox = Boolean.parseBoolean(propertyParameters.get(Constants.ADX_USE_SANDBOX));
		AdWordsUser adWordsUser = new AdWordsUser(email, password, clientId, userAgent, developerToken);
		AuthToken authToken = new AuthToken(email, password);
		adWordsUser.setAuthToken(authToken.getAuthToken());
		return adWordsUser;
	}

	private List<AdGroup> getAdGroups(AdGroupServiceInterface adGroupService, String[] adGroupIds, String[] statuses)
			throws RemoteException {
		List<AdGroup> list = new ArrayList<AdGroup>();
		Selector selector = new Selector();
		selector.setFields(new String[] { "Id", "CampaignId", "CampaignName", "Name", "Status", "MaxCpm" });
		selector.setOrdering(new OrderBy[] { new OrderBy("Name", SortOrder.ASCENDING) });
		Predicate campaignIdPred = new Predicate("AdGroupId", PredicateOperator.IN, adGroupIds);
		Predicate statusPred = new Predicate("Status", PredicateOperator.IN, statuses);
		selector.setPredicates(new Predicate[] { campaignIdPred, statusPred });
		AdGroupPage page = adGroupService.get(selector);
		if (page.getEntries() != null) {
			for (AdGroup adGroup : page.getEntries()) {
				list.add(adGroup);
			}
		}
		return list;
	}

	private int readFrequencyCap(long campaignId, CampaignServiceInterface campaignService) throws RemoteException,
			ServiceException, AuthTokenException {
		String[] campaignIds = new String[] { String.format("%d", campaignId) };
		Selector selector = new Selector();
		selector.setFields(new String[] { "Id", "Name", "FrequencyCapMaxImpressions" });
		Predicate campaignIdPred = new Predicate("CampaignId", PredicateOperator.IN, campaignIds);
		selector.setPredicates(new Predicate[] { campaignIdPred });
		CampaignPage page = campaignService.get(selector);
		if (page.getEntries() != null) {
			for (Campaign campaign : page.getEntries()) {
				return campaign.getFrequencyCap().getImpressions().intValue();
			}
		}
		return 0;
	}

	public boolean readAdGroupSettings(Parameters propertyParameters) throws DOException {
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201302.ADGROUP_SERVICE);
			CampaignServiceInterface campaignService = adWordsUser
					.getService(AdWordsService.V201302.CAMPAIGN_SERVICE);
			String[] adGroupIdStrings = listToArray(adGroupIds);
			List<AdGroup> adGroups = getAdGroups(adGroupService, adGroupIdStrings, new String[] { "ENABLED" });
			for (AdGroup adGroup : adGroups) {
//				ManualCPMAdGroupBids bids = (ManualCPMAdGroupBids) adGroup.getBids();
//				double maxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
				
				BiddingStrategyConfiguration newBiddingStrategyConfiguration = adGroup.getBiddingStrategyConfiguration();
				CpcBid newBid = (CpcBid) newBiddingStrategyConfiguration.getBids()[0];
				double maxCpm = 1.0e-6 * (double) newBid.getBid().getMicroAmount();
				
				int fc = readFrequencyCap(adGroup.getCampaignId(), campaignService);
				oldCpm.put(adGroup.getId(), maxCpm);
				oldFC.put(adGroup.getId(), fc);
				campaignIds.put(adGroup.getId(), adGroup.getCampaignId());
				String adGroupName = StringUtils.removeSpecialChars(adGroup.getName());
				oldAdGroupName.put(adGroup.getId(), adGroupName);
				if (logReadAdGroups) {
					String msg = String.format("Ad Group: %-50s AdGroupId: %d  MaxCpm: %5.2f  FC: %d",
							adGroup.getName(), adGroup.getId(), maxCpm, fc); // adGroup.getCampaignName()
					log.info(msg);
				}
			}
			return (adGroups.size() > 0);
		} catch (Exception e) {
			throw new DOException(e);
		}
	}

	public Map<Long, Metric> readDeliveryMetrics(long time, Parameters propertyParameters) throws ParseException, DOException {
		Map<Long, Metric> deliveredMetrics = new HashMap<Long, Metric>();
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201302.ADGROUP_SERVICE);
			String[] adGroupIdStrings = listToArray(adGroupIds);
			Selector selector = new Selector();
			selector.setFields(new String[] { "Id", "Name", "MaxCpm", "Impressions", "Clicks", "AverageCpm" });
			Predicate campaignIdPred = new Predicate("AdGroupId", PredicateOperator.IN, adGroupIdStrings);
			selector.setPredicates(new Predicate[] { campaignIdPred });

			String start = DateUtils.getShortDate(time);
			selector.setDateRange(new DateRange(start, start));
			AdGroupPage page = adGroupService.get(selector);
			if (page.getEntries() != null) {
				for (AdGroup adGroup : page.getEntries()) {
//					ManualCPMAdGroupBids bids = (ManualCPMAdGroupBids) adGroup.getBids();
//					double maxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
					
					BiddingStrategyConfiguration newBiddingStrategyConfiguration = adGroup.getBiddingStrategyConfiguration();
					CpcBid newBid = (CpcBid) newBiddingStrategyConfiguration.getBids()[0];
					double maxCpm = 1.0e-6 * (double) newBid.getBid().getMicroAmount();
					
					Stats stats = adGroup.getStats();
					double deliveredEcpm = 1.0e-6 * (double) stats.getAverageCpm().getMicroAmount();
					int deliveredImpressions = stats.getImpressions().intValue();
					int deliveredClicks = stats.getClicks().intValue();
					String adGroupName = StringUtils.removeSpecialChars(adGroup.getName());
					long adGroupId = adGroup.getId();
					String msg = String.format(
							"AdGroupId: %d  MaxCpm: %5.2f  eCpm = %5.2f  Imps: %,9d  Clicks: %3d  Ad Group: %s",
							adGroupId, maxCpm, deliveredEcpm, deliveredImpressions, deliveredClicks, adGroupName);
					log.info(msg);
					Metric metric = new Metric(deliveredImpressions, deliveredClicks, 0.0, 0.001 * deliveredEcpm * deliveredImpressions);
					metric.setTime(time);
					deliveredMetrics.put(adGroupId, metric);
					oldCpm.put(adGroupId, maxCpm);
					oldAdGroupName.put(adGroupId, adGroupName);
				}
			}
		} catch (Exception e) {
			throw new DOException(e);
		}
		return deliveredMetrics;
	}
	
	public Map<Long, Metric> readDeliveryMetrics(String startDate, String endDate, Parameters propertyParameters) throws ParseException, DOException {
		Map<Long, Metric> deliveredMetrics = new HashMap<Long, Metric>();
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201302.ADGROUP_SERVICE);
			String[] adGroupIdStrings = listToArray(adGroupIds);
			Selector selector = new Selector();
			selector.setFields(new String[] { "Id", "Name", "MaxCpm", "Impressions", "Clicks", "AverageCpm" });
			Predicate campaignIdPred = new Predicate("AdGroupId", PredicateOperator.IN, adGroupIdStrings);
			selector.setPredicates(new Predicate[] { campaignIdPred });

			selector.setDateRange(new DateRange(startDate, endDate));
			AdGroupPage page = adGroupService.get(selector);
			if (page.getEntries() != null) {
				for (AdGroup adGroup : page.getEntries()) {
//					ManualCPMAdGroupBids bids = (ManualCPMAdGroupBids) adGroup.getBids();
//					double maxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
					
					BiddingStrategyConfiguration newBiddingStrategyConfiguration = adGroup.getBiddingStrategyConfiguration();
					CpcBid newBid = (CpcBid) newBiddingStrategyConfiguration.getBids()[0];
					double maxCpm = 1.0e-6 * (double) newBid.getBid().getMicroAmount();
					
					Stats stats = adGroup.getStats();
					double deliveredEcpm = 1.0e-6 * (double) stats.getAverageCpm().getMicroAmount();
					int deliveredImpressions = stats.getImpressions().intValue();
					int deliveredClicks = stats.getClicks().intValue();
					String adGroupName = StringUtils.removeSpecialChars(adGroup.getName());
					long adGroupId = adGroup.getId();
					String msg = String.format(
							"AdGroupId: %d  MaxCpm: %5.2f  eCpm = %5.2f  Imps: %,9d  Clicks: %3d  Ad Group: %s",
							adGroupId, maxCpm, deliveredEcpm, deliveredImpressions, deliveredClicks, adGroupName);
//					log.info(msg);
					Metric metric = new Metric(deliveredImpressions, deliveredClicks, 0.0, 0.001 * deliveredEcpm * deliveredImpressions);
					metric.setTime(0L);
					deliveredMetrics.put(adGroupId, metric);
					oldCpm.put(adGroupId, maxCpm);
					oldAdGroupName.put(adGroupId, adGroupName);
				}
			}
		} catch (Exception e) {
			throw new DOException(e);
		}
		return deliveredMetrics;
	}
	
	public Map<Long, Double> getMaxCpm() {
		return oldCpm;
	}
	
	public Map<Long, String> getAdGroupName() {
		return oldAdGroupName;
	}
	
	private String getExpiryDate(String audienceName) throws SQLException {
		if (!audienceName.contains(".lal") && !audienceName.contains("LAL")) {
			return "";
		}
		Statement statement = DbUtils.getStatement(Constants.URL_ADPLATFORM);
		String query = String
				.format("SELECT t1.updateDate, t2.value FROM lookalike.adGroups t1, lookalike.globalParameters t2, adplatform.audience t3 " +
						"WHERE t1.pixelId = t3.id AND t3.name='%s' AND t2.parameter='Do Not Process After Days';", audienceName);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long time = DateUtils.parseDateTime(result, "updateDate");
			int days = result.getInt("value");
			time = DateUtils.getTime(time, days);
			String str = DateUtils.getShortDate(time);
			return str.substring(0, 4) + "-" + str.substring(4, 6) + "-" + str.substring(6, 8);
		} else {
			return "";
		}
	}
	
	public String writeAudiencesReport(Parameters propertyParameters) throws DOException {
		StringBuilder report = new StringBuilder();
		Map<Long, Map<String, String>> audienceData = new HashMap<Long, Map<String, String>>();
		Set<String> audiences = new HashSet<String>();
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			com.google.api.adwords.v201302.jaxb.cm.Selector selector = new com.google.api.adwords.v201302.jaxb.cm.Selector();
			List<String> fields = selector.getFields();
			fields.add("AdGroupId");
			fields.add("AdGroupName");
			fields.add("Status");
			fields.add("Criteria");
			fields.add("Impressions");
			fields.add("Clicks");
			fields.add("Cost");
//			selector.setFields(new String[] { "AdGroupId", "AdGroupName", "Status", "Criteria", "Impressions",
//					"Clicks", "Cost" });
			
			com.google.api.adwords.v201302.jaxb.cm.Predicate predicate1 = new com.google.api.adwords.v201302.jaxb.cm.Predicate();
			predicate1.setField("CriteriaType");
			predicate1.setOperator(com.google.api.adwords.v201302.jaxb.cm.PredicateOperator.IN);
			List<String> values1 = predicate1.getValues();
			values1.add("USER_LIST");
			
			com.google.api.adwords.v201302.jaxb.cm.Predicate predicate2 = new com.google.api.adwords.v201302.jaxb.cm.Predicate();
			predicate2.setField("AdGroupId");
			predicate2.setOperator(com.google.api.adwords.v201302.jaxb.cm.PredicateOperator.IN);
			List<String> values2 = predicate2.getValues();
			for (Long id : adGroupIds) {
				values2.add(id.toString());
			}
			
//			Predicate predicate1 = new Predicate("CriteriaType", PredicateOperator.IN, new String[] { "USER_LIST" });
//			Predicate predicate2 = new Predicate("AdGroupId", PredicateOperator.IN, listToArray(adGroupIds));
			List<com.google.api.adwords.v201302.jaxb.cm.Predicate> predicates = selector.getPredicates();
			predicates.add(predicate1);
			predicates.add(predicate2);
//			selector.setPredicates(new Predicate[] { predicate1, predicate2 });

			com.google.api.adwords.v201302.jaxb.cm.ReportDefinition reportDefinition = new com.google.api.adwords.v201302.jaxb.cm.ReportDefinition();
			reportDefinition.setReportName("Audiences report");
			reportDefinition.setDateRangeType(com.google.api.adwords.v201302.jaxb.cm.ReportDefinitionDateRangeType.TODAY);
			reportDefinition.setReportType(com.google.api.adwords.v201302.jaxb.cm.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT);
			reportDefinition.setDownloadFormat(com.google.api.adwords.v201302.jaxb.cm.DownloadFormat.TSV);
			reportDefinition.setIncludeZeroImpressions(true);
			reportDefinition.setSelector(selector);

			BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(new File(
					Constants.TEMPORARY_REPORT)));
			ReportDownloadResponse response = ReportUtils.downloadReport(adWordsUser, reportDefinition, stream);
			double totImps = 0.0;
			double totClks = 0.0;
			double totCost = 0.0;
			if (response.getHttpStatus() == HttpURLConnection.HTTP_OK) {
				File file = new File(Constants.TEMPORARY_REPORT);
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line = reader.readLine();
				while ((line = reader.readLine()) != null) {
					String[] values = line.split("\t");
					if (!values[4].matches(number)) continue;
					int imps = Integer.parseInt(values[4]);
					if (values[2].equalsIgnoreCase("enabled") || (values[2].equalsIgnoreCase("paused") && imps > 0)) {
						long adGroupId = Long.parseLong(values[0]);
						Map<String, String> audienceDataPerAdGroup = audienceData.get(adGroupId);
						if (audienceDataPerAdGroup == null) {
							audienceDataPerAdGroup = new HashMap<String, String>();
							audienceData.put(adGroupId, audienceDataPerAdGroup);
						}
						String audienceId = values[3].split("::")[1];
						if (!audiences.contains(audienceId)) {
							audiences.add(audienceId);
						}
						int clks = Integer.parseInt(values[5]);
						double cost = Double.parseDouble(values[6].replaceAll("\"", "").replaceAll(",", ""));
						String data = String.format("%7s %,10d %,5d  %5.3f  %5.2f", values[2].toUpperCase(), imps,
								clks, (imps == 0 ? 0.0 : 100.0 * (double) clks / (double) imps), (imps == 0 ? 0.0 : 1000.0 * cost / (double) imps));
						if (audienceDataPerAdGroup.containsKey(audienceId)) {
							log.error("Duplicate audience id: " + audienceId + " for ad group: " + values[1] + "  "
									+ audienceDataPerAdGroup.get(audienceId));
						}
						audienceDataPerAdGroup.put(audienceId, data);
						totImps += imps;
						totClks += clks;
						totCost += cost;
					}
				}
				file.delete();
			} else {
				log.error("Report was not downloaded. " + response.getHttpStatus() + ": "
						+ response.getHttpResponseMessage());
				return "";
			}

			if (audiences.isEmpty()) {
				log.info("All ad-group audiences have zero impressions");
				return "";
			}

			Map<String, String> audienceAttributes = new HashMap<String, String>();
			Set<String> invalidAudiences = new HashSet<String>();
			Map<String, String> expiry = new HashMap<String, String>();
			UserListServiceInterface userListService = adWordsUser
					.getService(AdWordsService.V201302.USER_LIST_SERVICE);
			Selector selector2 = new Selector();
			selector2.setFields(new String[] { "Id", "Name", "Size", "MembershipLifeSpan", "Status" });
			Predicate predicate = new Predicate("Id", PredicateOperator.IN, setToArray(audiences));
			selector2.setPredicates(new Predicate[] { predicate });
			UserListPage page = userListService.get(selector2);
			if (page.getEntries() != null) {
				for (UserList userList : page.getEntries()) {
					String audienceId = Long.toString(userList.getId());
					String name = userList.getName();
					expiry.put(audienceId, getExpiryDate(name));
					if (name.toLowerCase().contains("placeholder") || userList.getSize() == 0) {
						invalidAudiences.add(audienceId);
					}
					if (name.length() > 40) {
						name = name.substring(0, 40);
					}
					String attributes = String.format("%-40s %,10d %4d  %7s", name, userList.getSize(),
							userList.getMembershipLifeSpan(), userList.getStatus());
					if (audienceAttributes.containsKey(audienceId)) {
						log.error("Duplicate audience id for " + userList.getName());
					}
					audienceAttributes.put(audienceId, attributes);
				}
			}
			
			report.append(String.format("%-45s %10s %4s  %7s  %7s  %9s  %4s  %5s  %5s  %10s\n", "Ad Group/Audience Name", "Size",
					"Span", "State", "Status", "Imps", "Clks", "CTR-%", "eCPM", "LAL Expiry"));
			for (int i = 0; i < adGroupIds.size(); i++) {
				long adGroupId = adGroupIds.get(i);
				String adGroupName = adGroupNames.get(i);
				if (!audienceData.containsKey(adGroupId)) {
					report.append(adGroupName + ":\n");
					report.append("     NONE\n");
				} else {
					List<String> audienceIds = Sort.byAttribute(audienceData.get(adGroupId).keySet(),
							audienceAttributes, "asc");
					report.append(adGroupName + ":\n");
					int validCount = 0;
					int invalidCount = 0;
					for (String audienceId : audienceIds) {
						String attributes = audienceAttributes.get(audienceId);
						if (!invalidAudiences.contains(audienceId)) {
							validCount++;
							report.append("     " + attributes + "  " + audienceData.get(adGroupId).get(audienceId) 
									    + "  " + expiry.get(audienceId) + "\n");
						} else {
							invalidCount++;
						}
					}
					if (validCount == 0) {
						report.append("     NONE\n");
					}
					if (invalidCount > 0) {
						String msg = String.format("%d zero-size audience%s not shown", invalidCount, (invalidCount == 1 ? "" : "s"));
						report.append("     " + msg + "\n");
					}
				}
			}
			String data = String.format("%,11d %,5d  %5.3f  %5.2f\n", (int) totImps, (int) totClks, (totImps == 0 ? 0.0 : 100.0 * totClks
					/ totImps), (totImps == 0 ? 0.0 : 1000.0 * totCost / totImps));
			report.append("TOTAL:\n");
			report.append(String.format("%79s", "") + data);
		} catch (Exception e) {
			throw new DOException(e);
		}
		return report.toString();
	}

	public void writeAdGroupSettings() throws IOException, ParseException, SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String datetime = DateUtils.getDatetime();
		for (long adGroupId : adGroupIds) {
			if (!oldCpm.containsKey(adGroupId)) {
				log.info(String.format("Ad group %d disabled", adGroupId));
				continue;
			}
			String query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS,
					adGroupId);
			ResultSet result = statement.executeQuery(query);
			if (result.next()) {
				long id = result.getLong("id");
				query = String.format("UPDATE %s SET maxCpm = %.2f, frequencyCap = %d, updateDate='%s' WHERE id = %d;",
						Constants.DO_AD_GROUP_SETTINGS, oldCpm.get(adGroupId), oldFC.get(adGroupId), datetime, id);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					log.info(String.format("Record in %s with adGroupId = %d updated with maxCpm = %5.2f and FC = %d",
							Constants.DO_AD_GROUP_SETTINGS, adGroupId, oldCpm.get(adGroupId), oldFC.get(adGroupId)));
				} else {
					log.error(String.format("Record in %s with adGroupId = %d updated %d times",
							Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
				}
			} else {
				query = String
						.format("INSERT INTO %s (adGroupId,adGroupName,maxCpm,updateDate,createDate) VALUES (%d,'%s',%f,'%s','%s');",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, oldAdGroupName.get(adGroupId),
								oldCpm.get(adGroupId), datetime, datetime);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					log.info(String.format("Record in %s with adGroupId = %d inserted with maxCpm = %.2f",
							Constants.DO_AD_GROUP_SETTINGS, adGroupId, oldCpm.get(adGroupId)));
				} else {
					log.error(String.format("Record in %s with adGroupId = %d inserted %d times",
							Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
				}
			}
		}
	}

	public boolean writeAdx(Parameters propertyParameters) throws DOException {
		AdGroupServiceInterface adGroupService;
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			adGroupService = adWordsUser.getService(AdWordsService.V201302.ADGROUP_SERVICE);
		} catch (Exception e) {
			errors.add("All", "All", e.getMessage());
			log.error(e);
			return false;
		}
		for (long adGroupId : adGroupIds) {
			String adGroupName = newAdGroupName.get(adGroupId);
			
		    BiddingStrategyConfiguration biddingStrategyConfiguration = new BiddingStrategyConfiguration();
		    CpcBid bid = new CpcBid();
		    bid.setBid(new Money("Money", (long) Math.round(1000000 * newCpm.get(adGroupId))));
		    biddingStrategyConfiguration.setBids(new Bids[] {bid});

			
			
			
//			ManualCPMAdGroupBids bids = new ManualCPMAdGroupBids();
//			Bid bid = new Bid(new Money("Money", (long) Math.round(1000000 * newCpm.get(adGroupId))));
//			bids.setMaxCpm(bid);
			AdGroup adGroup = new AdGroup();
			adGroup.setId(adGroupId);
//			adGroup.setBids(bids);
		    adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration);
		    
			AdGroupOperation[] operations = new AdGroupOperation[] { new AdGroupOperation(Operator.SET, null, adGroup) };
			try {
				AdGroup[] newAdGroups = adGroupService.mutate(operations).getValue();
				for (AdGroup newAdGroup : newAdGroups) {
					BiddingStrategyConfiguration newBiddingStrategyConfiguration = newAdGroup.getBiddingStrategyConfiguration();
					CpcBid newBid = (CpcBid) newBiddingStrategyConfiguration.getBids()[0];
					double newMaxCpm = 1.0e-6 * (double) newBid.getBid().getMicroAmount();
//					bids = (ManualCPMAdGroupBids) newAdGroup.getBids();
//					double newMaxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
					String msg;
					if (oldCpm.containsKey(adGroupId)) {
						msg = String.format("Group: %-50s AdGroupId: %d  MaxCpm: %6.2f -> %6.2f", newAdGroup.getName(),
								newAdGroup.getId(), oldCpm.get(adGroupId), newMaxCpm);
					} else {
						msg = String.format("Group: %-50s AdGroupId: %d  MaxCpm:  pause -> %6.2f", newAdGroup.getName(),
								newAdGroup.getId(), newMaxCpm);
					}
					log.info(msg);
				}
			} catch (ApiException e) {
				errors.add(adGroupId, adGroupName, "Failed to set AdWords parameters: ApiException: " + e.getFaultString().replaceAll("\'", "").replaceAll("\"", ""));
				log.error(adGroupName + "(" + adGroupId + ") failed to set AdWords parameters: ApiException: ", e);
			} catch (RemoteException e) {
				errors.add(adGroupId, adGroupName, "Failed to set AdWords parameters: RemoteException: " + e.getMessage());
				log.error(adGroupName + "(" + adGroupId + ") failed to set AdWords parameters: RemoteException: ", e);
			}
		}
		return errors.isEmpty();
	}
	
	public Errors getError() {
		return errors;
	}
}
