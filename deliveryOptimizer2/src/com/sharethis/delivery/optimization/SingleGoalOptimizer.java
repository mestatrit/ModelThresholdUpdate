package com.sharethis.delivery.optimization;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import com.sharethis.delivery.base.AdGroup;
import com.sharethis.delivery.base.CampaignState;
import com.sharethis.delivery.base.CampaignStates;
import com.sharethis.delivery.base.CampaignType;
import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.base.StrategyType;
import com.sharethis.delivery.common.CampaignParameters;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class SingleGoalOptimizer extends Optimizer {
	private CampaignStates pastCampaignStates;
	private CampaignState futureCampaignState;
	private StateModel stateModel;
	private int deletedRecords, updatedRecords, deliveredRecords;
	private Errors errors;

	private SingleGoalOptimizer() {
		deletedRecords = updatedRecords = deliveredRecords = 0;
		errors = new Errors();
	}

	public SingleGoalOptimizer(long campaignId, int goalId) {
		this();
		this.campaignId = campaignId;
		this.goalId = goalId;
	}

	public int getGoalId() {
		return goalId;
	}

	public int getReadCount() {
		return pastCampaignStates.size() - pastCampaignStates.getCurrentCampaignIndex();
	}

	public int getDeletedCount() {
		return deletedRecords;
	}

	public int getUpdatedCount() {
		return updatedRecords;
	}

	public int getDeliveredCount() {
		return deliveredRecords;
	}

	public String getDailyConversionFactor() {
		return stateModel.getDailyConversionFactor();
	}

	public Errors getErrors() {		
		errors.setTask("Optimize");
		errors.setIds(campaignParameters);
		return errors;
	}
	
	public String getCampaignName() {
		return campaignParameters.get(Constants.CAMPAIGN_NAME);
	}
	
	public boolean readCampaignData(String pvDate) throws ParseException, SQLException {
		if (!readCampaignParameters(campaignId, goalId)) {
			return false;
		}

		pastCampaignStates = new CampaignStates(campaignParameters);
		if (!pastCampaignStates.readStates()) {
			errors = pastCampaignStates.getErrors();
			return false;
		}

		long targetTime = pastCampaignStates.getTargetTime();
		futureCampaignState = new CampaignState(campaignParameters, targetTime);
		if (!futureCampaignState.readState()) {
			errors = futureCampaignState.getErrors();
			return false;
		}
		if (!futureCampaignState.readPriceVolumeCurves(pvDate)) {
			errors = futureCampaignState.getErrors();
		}

		backupCampaignPerformanceData(campaignId, goalId);
		stateModel = new StateModel(campaignParameters, pastCampaignStates, futureCampaignState);

		return true;
	}

	private boolean readCampaignParameters(long campaignId, int goalId) throws ParseException, SQLException {
		campaignParameters = new CampaignParameters(campaignId, goalId);
		String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
		
		int status = campaignParameters.getInt(Constants.STATUS, 0);
		if (status == 0) {
			errors.add("Status inactive");
			errors.log(campaignName);
			return false;
		}

		int segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		if (segmentCount != 1 && segmentCount != 2) {
			errors.add("Unsupported number of audience segments: " + segmentCount);
			errors.log(campaignName);
			return false;
		}
		
		double budget = campaignParameters.getDouble(Constants.BUDGET, 0.0);
		if (budget < 50.0) {
			errors.add("Budget too small: " + budget);
			errors.log(campaignName);
			return false;
		}

		StrategyType strategy = campaignParameters.getStrategy();
		if (!strategy.isValid()) {
			errors.add("Unsupported strategy: " + strategy.getType());
			errors.log(campaignName);
			return false;
		}
		
		double kpi = campaignParameters.getDouble(Constants.KPI, 0.0);
		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		
		if (cpm < 0.1) {
			errors.add("CPM too small: " + cpm);
			errors.log(campaignName);
			return false;
		}
		
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		if (kpi <= 0) {
			errors.add(campaignType.toString() + " too small: " + kpi);
			errors.log(campaignName);
			return false;
		}
		
		double targetImpressions = 1000.0 * budget / cpm;
		double targetClicks = (campaignType.isCpc() ? budget / kpi : 1.0e-3 * targetImpressions);
		double targetConversions = (!campaignType.isCpc() ? budget / kpi : 1.0e-5 * targetImpressions);
		campaignParameters.put(Constants.TARGET_IMPRESSIONS, String.format("%.1f", targetImpressions));
		campaignParameters.put(Constants.TARGET_CLICKS, String.format("%.1f", targetClicks));
		campaignParameters.put(Constants.TARGET_CONVERSIONS, String.format("%.1f", targetConversions));

		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		double intervals = (double) (endTime - startTime) / (deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
		campaignParameters
				.put(Constants.IMPRESSIONS_PER_INTERVAL, String.format("%.1f", targetImpressions / intervals));
		campaignParameters.put(Constants.CLICKS_PER_INTERVAL, String.format("%.1f", targetClicks / intervals));
		campaignParameters
				.put(Constants.CONVERSIONS_PER_INTERVAL, String.format("%.1f", targetConversions / intervals));

		return true;
	}

	private void backupCampaignPerformanceData(long campaignId, int goalId) throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("DELETE FROM %s WHERE campaignId = %d AND goalId = %d;",
				Constants.DO_CAMPAIGN_PERFORMANCES_BACKUP, campaignId, goalId);
		statement.executeUpdate(query);
		query = String.format("INSERT INTO %s SELECT * FROM %s WHERE campaignId = %d AND goalId = %d;",
				Constants.DO_CAMPAIGN_PERFORMANCES_BACKUP, Constants.DO_CAMPAIGN_PERFORMANCES, campaignId,
				goalId);
		statement.executeUpdate(query);
	}

	public void clear() throws SQLException, ParseException {
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		long currentTime = DateUtils.currentTime();
		
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		
		if (startTime <= currentTime && currentTime < endTime) {
			for (AdGroup adGroup : futureCampaignState.getAdGroups()) {
				String query = String.format("DELETE FROM %s WHERE adGroupId = %d;", Constants.DO_IMPRESSION_TARGETS, adGroup.getId());
				statement.executeUpdate(query);
			}
		}
		String startDate = DateUtils.getDatetime(startTime);
		String today = DateUtils.getToday();
		String query = String.format("SELECT id FROM %s WHERE campaignId = %d AND goalId = %d AND (date < '%s' OR date >= '%s');",
						Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, startDate, today);
		ResultSet result = statement.executeQuery(query);
		Set<Long> ids = new HashSet<Long>();
		while (result.next()) {
			long id = result.getLong("id");
			ids.add(id);
		}
		for (long id : ids) {
			query = String.format("DELETE FROM %s WHERE campaignRecordId = %d;", Constants.DO_SEGMENT_PERFORMANCES, id);
			statement.executeUpdate(query);
			query = String.format("DELETE FROM %s WHERE id = %d;", Constants.DO_CAMPAIGN_PERFORMANCES, id);
			deletedRecords += statement.executeUpdate(query);
		}
	}

	public void estimate() throws ParseException, IOException, SQLException {
		updatedRecords = 0;
		stateModel.reset();
		while (stateModel.hasNext()) {
			stateModel.estimate();
			stateModel.updateStatistics();
			if (stateModel.isCurrentCampaign()) {
				updatedRecords += stateModel.updateDoTables();
			}
		}
	}

	public boolean optimize() throws SQLException, ParseException {
		if (futureCampaignState.hasEnded()) {
			String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
			errors.add("Delivery not optimized: Campaign has ended");
			errors.log(campaignName);
			return false;
		} else if (stateModel.optimize()) {
			return true;
		} else {
			errors.add(stateModel.getErrors());
			return false;
		}
	}

	public boolean allocate() throws SQLException, ParseException {
		if (futureCampaignState.hasEnded()) {
			String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
			errors.add("Impressions not allocated: Campaign has ended");
			errors.log(campaignName);
			return false;
		} else {
			stateModel.allocateWithComments();
			futureCampaignState.writeCampaignState();
			errors.add(stateModel.getErrors());
			errors.add(futureCampaignState.getErrors());
			return true;
		}
	}
	
	public void write() throws SQLException, ParseException {
		futureCampaignState.writeImpressionTargets();
		deliveredRecords = futureCampaignState.getDeliveredRecords();
		errors.add(futureCampaignState.getErrors());
	}
	
	public double getKpiScore() {
		return stateModel.getKpiScore();
	}
	
	public boolean hasErrors() {
		return (!errors.isEmpty());
	}

	public String getReport(int activeGoalId) {
		String tag = (activeGoalId == goalId ? ">" : " ");
		return String
				.format("%sRecords for campaignId = %d, goalId = %d: deleted: %d, read: %2d, updated: %2d, delivered: %d, conversion factor: %s",
						tag, campaignId, goalId, deletedRecords, getReadCount(), updatedRecords, deliveredRecords,
						getDailyConversionFactor());
	}
}