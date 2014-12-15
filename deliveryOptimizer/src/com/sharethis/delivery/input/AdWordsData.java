package com.sharethis.delivery.input;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.rmi.RemoteException;
import java.sql.Connection;
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

import org.apache.log4j.Logger;

import com.google.api.adwords.lib.AdWordsService;
import com.google.api.adwords.lib.AdWordsUser;
import com.google.api.adwords.lib.AuthToken;
import com.google.api.adwords.lib.AuthTokenException;
import com.google.api.adwords.lib.utils.v201209.ReportDownloadResponse;
import com.google.api.adwords.lib.utils.v201209.ReportUtils;
import com.google.api.adwords.v201209.cm.AdGroup;
import com.google.api.adwords.v201209.cm.AdGroupOperation;
import com.google.api.adwords.v201209.cm.AdGroupPage;
import com.google.api.adwords.v201209.cm.AdGroupServiceInterface;
import com.google.api.adwords.v201209.cm.Bid;
import com.google.api.adwords.v201209.cm.Campaign;
import com.google.api.adwords.v201209.cm.CampaignPage;
import com.google.api.adwords.v201209.cm.CampaignServiceInterface;
import com.google.api.adwords.v201209.cm.DateRange;
import com.google.api.adwords.v201209.cm.ManualCPMAdGroupBids;
import com.google.api.adwords.v201209.cm.Money;
import com.google.api.adwords.v201209.cm.Operator;
import com.google.api.adwords.v201209.cm.OrderBy;
import com.google.api.adwords.v201209.cm.Predicate;
import com.google.api.adwords.v201209.cm.PredicateOperator;
import com.google.api.adwords.v201209.cm.Selector;
import com.google.api.adwords.v201209.cm.SortOrder;
import com.google.api.adwords.v201209.cm.Stats;
import com.google.api.adwords.v201209.cm.UserList;
import com.google.api.adwords.v201209.cm.UserListPage;
import com.google.api.adwords.v201209.cm.UserListServiceInterface;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.PriceVolumeCurve;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.Sort;

public class AdWordsData {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private List<Long> adGroupIds;
	private List<String> adGroupNames;
	private Map<Long, Double> oldCpm, newCpm;
	private Map<Long, Integer> oldFC;
	private Map<Long, Long> campaignIds;
	private Map<Long, String> oldAdGroupName;
	private boolean logReadAdGroups;


	public AdWordsData(List<Long> adGroupIds) {
		this(adGroupIds, new ArrayList<String>());
	}
	
	public AdWordsData(List<Long> adGroupIds, List<String> adGroupNames) {
		this.adGroupIds = adGroupIds;
		this.adGroupNames = adGroupNames;
		this.oldCpm = new HashMap<Long, Double>();
		this.oldAdGroupName = new HashMap<Long, String>();
		this.oldFC = new HashMap<Long, Integer>();
		this.campaignIds = new HashMap<Long, Long>();
		this.logReadAdGroups = true;
	}

	public AdWordsData(Map<Long, Double> newCpm, Map<Long, String> newAdGroupName) {
		this.adGroupIds = Sort.byValue(newAdGroupName, "asc");
		this.campaignIds = new HashMap<Long, Long>();
		this.oldCpm = new HashMap<Long, Double>();
		this.oldAdGroupName = new HashMap<Long, String>();
		this.oldFC = new HashMap<Long, Integer>();
		this.newCpm = newCpm;
		this.logReadAdGroups = false;
	}

	private String[] listToArray(List<Long> list) {
		String[] array = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			array[i] = list.get(i).toString();
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
		AdWordsUser adWordsUser = new AdWordsUser(email, password, clientId, userAgent, developerToken, useSandbox);
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
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201209.ADGROUP_SERVICE);
			CampaignServiceInterface campaignService = adWordsUser
					.getService(AdWordsService.V201209.CAMPAIGN_SERVICE);
			String[] adGroupIdStrings = listToArray(adGroupIds);
			List<AdGroup> adGroups = getAdGroups(adGroupService, adGroupIdStrings, new String[] { "ENABLED" });
			for (AdGroup adGroup : adGroups) {
				ManualCPMAdGroupBids bids = (ManualCPMAdGroupBids) adGroup.getBids();
				double maxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
				int fc = readFrequencyCap(adGroup.getCampaignId(), campaignService);
				oldCpm.put(adGroup.getId(), maxCpm);
				oldFC.put(adGroup.getId(), fc);
				campaignIds.put(adGroup.getId(), adGroup.getCampaignId());
				oldAdGroupName.put(adGroup.getId(), adGroup.getName());
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

	public Map<Long, PriceVolumeCurve> readStats(Parameters propertyParameters) throws ParseException, DOException {
		Map<Long, PriceVolumeCurve> priceVolumeCurves = new HashMap<Long, PriceVolumeCurve>();
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201209.ADGROUP_SERVICE);
			String[] adGroupIdStrings = listToArray(adGroupIds);
			Selector selector = new Selector();
			selector.setFields(new String[] { "Id", "Name", "MaxCpm", "Impressions", "Clicks", "AverageCpm" });
			Predicate campaignIdPred = new Predicate("AdGroupId", PredicateOperator.IN, adGroupIdStrings);
			selector.setPredicates(new Predicate[] { campaignIdPred });
			long time = DateUtils.getToday(-1);
			String start = DateUtils.getShortDate(time);
			selector.setDateRange(new DateRange(start, start));
			AdGroupPage page = adGroupService.get(selector);
			if (page.getEntries() != null) {
				for (AdGroup adGroup : page.getEntries()) {
					ManualCPMAdGroupBids bids = (ManualCPMAdGroupBids) adGroup.getBids();
					double maxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
					Stats stats = adGroup.getStats();
					double deliveredEcpm = 1.0e-6 * (double) stats.getAverageCpm().getMicroAmount();
					int deliveredImpressions = stats.getImpressions().intValue();
					int deliveredClicks = stats.getClicks().intValue();
					String adGroupName = adGroup.getName();
					long adGroupId = adGroup.getId();
					String msg = String.format(
							"AdGroupId: %d  MaxCpm: %5.2f  eCpm = %5.2f  Imps: %,9d  Clicks: %3d  Ad Group: %s",
							adGroupId, maxCpm, deliveredEcpm, deliveredImpressions, deliveredClicks, adGroupName);
					log.info(msg);
					PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(deliveredEcpm, deliveredImpressions,
							deliveredClicks, maxCpm, time);
					priceVolumeCurves.put(adGroupId, priceVolumeCurve);
				}
			}
		} catch (Exception e) {
			throw new DOException(e);
		}
		return priceVolumeCurves;
	}

	public String writeAudiencesReport(Parameters propertyParameters) throws DOException {
//		if (true) return "Audience report temporarily discontinued while migrating to the new AdWords API version";
		StringBuilder report = new StringBuilder();
		Map<Long, Map<String, String>> audienceData = new HashMap<Long, Map<String, String>>();
		Set<String> audiences = new HashSet<String>();
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			com.google.api.adwords.v201209.jaxb.cm.Selector selector = new com.google.api.adwords.v201209.jaxb.cm.Selector();
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
			
			com.google.api.adwords.v201209.jaxb.cm.Predicate predicate1 = new com.google.api.adwords.v201209.jaxb.cm.Predicate();
			predicate1.setField("CriteriaType");
			predicate1.setOperator(com.google.api.adwords.v201209.jaxb.cm.PredicateOperator.IN);
			List<String> values1 = predicate1.getValues();
			values1.add("USER_LIST");
			
			com.google.api.adwords.v201209.jaxb.cm.Predicate predicate2 = new com.google.api.adwords.v201209.jaxb.cm.Predicate();
			predicate2.setField("AdGroupId");
			predicate2.setOperator(com.google.api.adwords.v201209.jaxb.cm.PredicateOperator.IN);
			List<String> values2 = predicate2.getValues();
			for (Long id : adGroupIds) {
				values2.add(id.toString());
			}
			
//			Predicate predicate1 = new Predicate("CriteriaType", PredicateOperator.IN, new String[] { "USER_LIST" });
//			Predicate predicate2 = new Predicate("AdGroupId", PredicateOperator.IN, listToArray(adGroupIds));
			List<com.google.api.adwords.v201209.jaxb.cm.Predicate> predicates = selector.getPredicates();
			predicates.add(predicate1);
			predicates.add(predicate2);
//			selector.setPredicates(new Predicate[] { predicate1, predicate2 });

			com.google.api.adwords.v201209.jaxb.cm.ReportDefinition reportDefinition = new com.google.api.adwords.v201209.jaxb.cm.ReportDefinition();
			reportDefinition.setReportName("Audiences report");
			reportDefinition.setDateRangeType(com.google.api.adwords.v201209.jaxb.cm.ReportDefinitionDateRangeType.TODAY);
			reportDefinition.setReportType(com.google.api.adwords.v201209.jaxb.cm.ReportDefinitionReportType.CRITERIA_PERFORMANCE_REPORT);
			reportDefinition.setDownloadFormat(com.google.api.adwords.v201209.jaxb.cm.DownloadFormat.TSV);
			reportDefinition.setIncludeZeroImpressions(false);
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
					if (values[2].equalsIgnoreCase("enabled") || values[2].equalsIgnoreCase("paused")) {
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
						int imps = Integer.parseInt(values[4]);
						int clks = Integer.parseInt(values[5]);
						double cost = Double.parseDouble(values[6].replaceAll("\"", "").replaceAll(",", ""));
						String data = String.format("%7s  %,9d  %,4d  %5.3f  %5.2f", values[2].toUpperCase(), imps,
								clks, 100.0 * clks / imps, 1000.0 * cost / (double) imps);
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
			UserListServiceInterface userListService = adWordsUser
					.getService(AdWordsService.V201209.USER_LIST_SERVICE);
			Selector selector2 = new Selector();
			selector2.setFields(new String[] { "Id", "Name", "Size", "MembershipLifeSpan", "Status" });
			Predicate predicate = new Predicate("Id", PredicateOperator.IN, setToArray(audiences));
			selector2.setPredicates(new Predicate[] { predicate });
			UserListPage page = userListService.get(selector2);
			if (page.getEntries() != null) {
				for (UserList userList : page.getEntries()) {
					String audienceId = Long.toString(userList.getId());
					String name = userList.getName();
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
			
			report.append(String.format("%-45s %10s %4s  %7s  %7s  %9s  %4s  %5s  %5s\n", "Ad Group/Audience Name", "Size",
					"Span", "State", "Status", "Imps", "Clks", "CTR-%", "eCPM"));
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
					for (String audienceId : audienceIds) {
						report.append("     " + audienceAttributes.get(audienceId) + "  "
								+ audienceData.get(adGroupId).get(audienceId) + "\n");
					}
				}
			}
			String data = String.format("%,9d  %,4d  %5.3f  %5.2f\n", (int) totImps, (int) totClks, 100.0 * totClks
					/ totImps, 1000.0 * totCost / totImps);
			report.append("TOTAL:\n");
			report.append(String.format("%81s", "") + data);
		} catch (Exception e) {
			throw new DOException(e);
		}
		return report.toString();
	}

	public void writeAdGroupSettings() throws IOException, ParseException, SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
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

	public void writeAdx(Parameters propertyParameters) throws DOException {
		long exceptionAdGroupId = 0L;
		try {
			AdWordsUser adWordsUser = getAdWordsUser(propertyParameters);
			AdGroupServiceInterface adGroupService = adWordsUser.getService(AdWordsService.V201209.ADGROUP_SERVICE);
			for (long adGroupId : adGroupIds) {
				exceptionAdGroupId = adGroupId;
				ManualCPMAdGroupBids bids = new ManualCPMAdGroupBids();
				Bid bid = new Bid(new Money("Money", (long) Math.round(1000000 * newCpm.get(adGroupId))));
				bids.setMaxCpm(bid);
				AdGroup adGroup = new AdGroup();
				adGroup.setId(adGroupId);
				adGroup.setBids(bids);
				AdGroupOperation[] operations = new AdGroupOperation[] { new AdGroupOperation(Operator.SET, null,
						adGroup) };
				AdGroup[] newAdGroups = adGroupService.mutate(operations).getValue();
				for (AdGroup newAdGroup : newAdGroups) {
					bids = (ManualCPMAdGroupBids) newAdGroup.getBids();
					double newMaxCpm = 1.0e-6 * (double) bids.getMaxCpm().getAmount().getMicroAmount();
					String msg;
					if (oldCpm.containsKey(adGroupId)) {
						msg = String.format("Group: %-50s AdGroupId: %d  MaxCpm: %5.2f -> %5.2f", newAdGroup.getName(),
								newAdGroup.getId(), oldCpm.get(adGroupId), newMaxCpm);
					} else {
						msg = String.format("Group: %-50s AdGroupId: %d  MaxCpm: pause -> %5.2f", newAdGroup.getName(),
								newAdGroup.getId(), newMaxCpm);
					}
					log.info(msg);
				}
			}
		} catch (Exception e) {
			throw new DOException("Ad group " + exceptionAdGroupId + " failed to set AdWords parameters: ", e);
		}
	}
}
