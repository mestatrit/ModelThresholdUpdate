package com.sharethis.delivery.input;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sharethis.delivery.base.CampaignType;
import com.sharethis.delivery.base.StatusType;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

@SuppressWarnings("serial")
public class AuditData extends Data {
	private long reportId;
	private String kpiColumnName;
	private Map<Long, Integer> kpis, imps;
	private String missingDates;

	public AuditData(long campaignId, int goalId) {
		super(campaignId, goalId);
		segmentCount = 0;
		kpis = new HashMap<Long, Integer>();
		imps = new HashMap<Long, Integer>();
		missingDates = "";
	}
	
	@Override
	public boolean read() throws ParseException, SQLException {
		if (!readCampaignParameters())
			return false;
		if (!readReportId()) {
			return false;
		} else {
			return readDeliveredMetrics();
		}
	}
	
	private boolean readReportId() throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT reportId, kpiColumnName FROM %s WHERE campaignId = %d AND goalId = %d;", Constants.DO_THIRD_PARTY_KPI_MAPPINGS, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		reportId = 0L;
		kpiColumnName = "";
		if (result.next()) {
			reportId = result.getLong("reportId");
			kpiColumnName = result.getString("kpiColumnName").trim().toLowerCase();
			if (result.next()) {
				reportId = 0L;
				errors.add("Dublicate audit reports set: rejected");
			}
		}
		if (reportId > 0L && kpiColumnName.length() > 0) {
			return true;
		} else {
			errors.add("No audit report set");
			return false;
		}
	}

	public boolean readDeliveredMetrics() throws ParseException, SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_ADPLATFORM);
		String startDate = DateUtils.getDatetime(campaignParameters.getLong(Constants.START_DATE, 0L));
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d AND status = 1 AND keyDate >= '%s' ORDER BY keyDate ASC;", Constants.ADPLATFORM_DB_KPI_REPORT, reportId, startDate);
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			int kpi = result.getInt(kpiColumnName);
			int imp = result.getInt("impressions");
			long recordTime = DateUtils.parseTime(result, "keyDate");
			kpis.put(recordTime, kpi);
			imps.put(recordTime, imp);
		}
		if (kpis.isEmpty()) {
			log.info(String.format("Zero kpis read from cnvReportBody for campaignId = %d, goalId = %d", campaignId, goalId));
		}
		return true;
	}
		
	public void writeDeliveredMetrics() throws SQLException {
		long lastMissingKpiDate = 0L;
		String today = DateUtils.getToday();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);	
		String query = String.format("SELECT id, date FROM %s WHERE campaignId = %d AND goalId = %d AND date < '%s' ORDER BY date ASC;",
				Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, today);
		ResultSet result = statement.executeQuery(query);
		StringBuilder mia = new StringBuilder();
		List<Long> ids = new ArrayList<Long>();
		List<Long> recordTimes = new ArrayList<Long>();
		while (result.next()) {
			long id = result.getLong("id");
			long recordTime = DateUtils.parseTime(result, "date");
			ids.add(id);
			recordTimes.add(recordTime);
		}
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		String auditKpiColumn = (campaignType.isCpc() ? "auditClicks" : "auditConversions");
		String deliveryKpiColumn = (campaignType.isCpc() ? "deliveryClicks" : "deliveryConversions");
		StatusType status = new StatusType();
		status.setAudit();
		if (!campaignType.isCpc()) {
			status.setKpi();
		}
		for (int i = 0; i < ids.size(); i++) {
			long id = ids.get(i);
			long recordTime = recordTimes.get(i);
			String datetime = DateUtils.getDatetime(recordTime);
			if (kpis.containsKey(recordTime)) {
				for (int segmentId = 0; segmentId < segmentCount; segmentId++) {
					double auditKpis = (segmentId == 0 ? kpis.get(recordTime) : 0.0);
					double auditImpressions = (segmentId == 0 ? imps.get(recordTime) : 0.0);
					double auditCost = (segmentId == 0 ? 0.001 * auditImpressions * campaignParameters.getDouble(Constants.CPM, 0.0) : 0.0);
					if (campaignType.isCpc()) {
						query = String.format("UPDATE %s SET auditImpressions = %.0f, %s = %f, auditCost = %f WHERE campaignRecordId = %d AND segmentId = %d;", Constants.DO_SEGMENT_PERFORMANCES, auditImpressions, auditKpiColumn, auditKpis, auditCost, id, segmentId);
					} else {
						query = String.format("UPDATE %s SET auditImpressions = %.0f, %s = %f, %s = %f, auditCost = %f WHERE campaignRecordId = %d AND segmentId = %d;", Constants.DO_SEGMENT_PERFORMANCES, auditImpressions, auditKpiColumn, auditKpis, deliveryKpiColumn, auditKpis, auditCost, id, segmentId);						
					}
					int count = statement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with campaignId = %d, goalId = %d, segmentId = %d, date = %s updated with kpis = %f",
								Constants.DO_SEGMENT_PERFORMANCES, campaignId, goalId, segmentId, datetime, auditKpis));
						query = String.format("UPDATE %s SET status = status | %s WHERE id = %d;", Constants.DO_CAMPAIGN_PERFORMANCES, status.toString(), id);
						statement.executeUpdate(query);
					} else {
						log.error(String.format("Record in %s with campaignId = %d, goalId = %d, segmentId = %d, date = %s updated %d times",
								Constants.DO_SEGMENT_PERFORMANCES, campaignId, goalId, segmentId, datetime, count));
					}
				}
			} else {
				String date = datetime.split(" ")[0];
				lastMissingKpiDate = recordTime;
				mia.append(" ").append(date);
				log.error(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s not updated", Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, datetime));
			}
		}
		if (mia.length() > 0) {
			errors.add("Missing kpi dates: " + mia.toString());
			long daysSinceLastMissingKpi = (DateUtils.currentTime() - lastMissingKpiDate) / (24 * Constants.MILLISECONDS_PER_HOUR);
			if (daysSinceLastMissingKpi < 3L) {
				missingDates = String.format("%s (%d, %d):\n     %s\n", campaignParameters.get(Constants.CAMPAIGN_NAME), campaignId, goalId, mia.toString());
			}
		}
	}
	
	public String getMissingDates() {
		return missingDates;
	}
	
	public void writeAdGroupSettings() {
	}
}
