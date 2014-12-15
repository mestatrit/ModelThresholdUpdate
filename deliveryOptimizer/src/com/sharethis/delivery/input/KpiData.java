package com.sharethis.delivery.input;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sharethis.delivery.base.Campaign;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class KpiData extends Campaign implements Data {
	private long reportId;
	private String kpiColumnName;
	private Map<Long, Integer> kpis, imps;
	private String missingDates;

	private KpiData() {
		segmentCount = 0;
		kpis = new HashMap<Long, Integer>();
		imps = new HashMap<Long, Integer>();
		missingDates = "";
	}

	public KpiData(long campaignId, int goalId) {
		this();
		initialize(campaignId, goalId);
	}
	
	protected boolean readReportId() throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT reportId, kpiColumnName FROM %s WHERE campaignId = %d AND goalId = %d;", Constants.DO_THIRD_PARTY_KPI_MAPPINGS, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);
		reportId = 0L;
		kpiColumnName = "";
		if (result.next()) {
			reportId = result.getLong("reportId");
			kpiColumnName = result.getString("kpiColumnName").trim().toLowerCase();
			if (result.next()) {
				reportId = 0L;
			}
		}
		connection.close();
		connection = null;
		return (reportId > 0L && kpiColumnName.length() > 0);
	}

	public boolean readKpis() throws ParseException, SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_CAMPAIGN, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String startDate = DateUtils.getDatetime(campaignParameters.getLong(Constants.START_DATE, 0L));
		String query = String.format("SELECT * FROM cnvReportBody WHERE campaignId = %d AND status = 1 and keyDate >= '%s' ORDER BY keyDate ASC;", reportId, startDate);
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
	
	public boolean read() throws ParseException, SQLException {
		if (!readParameters(campaignId, goalId))
			return false;
		if (readReportId()) {
			return readKpis();
		} else {
			return false;
		}
	}
	
	public void writeCampaignPerformances() throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();		
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d AND goalId = %d AND imps_d_1 IS NOT NULL AND imps_d_2 IS NOT NULL ORDER BY date ASC;",
				campaignPerformancesTable, campaignId, goalId);
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
		int cumAuditImpressions = 0;
		for (int i = 0; i < ids.size(); i++) {
			long id = ids.get(i);
			long recordTime = recordTimes.get(i);
			String datetime = DateUtils.getDatetime(recordTime);
			if (kpis.containsKey(recordTime)) {
				cumAuditImpressions += imps.get(recordTime);
				query = String.format("UPDATE %s SET conversions = %d, auditImpressions = %d, cumAuditImpressions = %d WHERE id = %d;", campaignPerformancesTable, kpis.get(recordTime), imps.get(recordTime), cumAuditImpressions, id);
				int count = statement.executeUpdate(query);
				if (count == 1) {
					log.info(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s updated with conversions = %d",
									campaignPerformancesTable, campaignId, goalId, datetime, kpis.get(recordTime)));
				} else {
					log.error(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s updated %d times",
							campaignPerformancesTable, campaignId, goalId, datetime, count));
				}
			} else {
				mia.append(" ").append(datetime.split(" ")[0]);
				log.error(String.format("Record in %s with campaignId = %d, goalId = %d, date = %s not updated", campaignPerformancesTable, campaignId, goalId, datetime));
			}
		}
		if (mia.length() > 0) {
			missingDates = String.format("%s (%d, %d):\n     %s\n", campaignParameters.get(Constants.CAMPAIGN_NAME), campaignId, goalId, mia.toString());
		}
	}
	
	public String getMissingDates() {
		return missingDates;
	}
	
	public void writeAdGroupSettings() {
	}
}
