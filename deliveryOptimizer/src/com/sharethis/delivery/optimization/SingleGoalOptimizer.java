package com.sharethis.delivery.optimization;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.AdGroup;
import com.sharethis.delivery.base.Campaign;
import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.common.CampaignParameters;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.record.DeliveryRecord;
import com.sharethis.delivery.record.PerformanceRecord;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class SingleGoalOptimizer extends Campaign {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private DeliveryRecord deliveryRecord;
	private List<PerformanceRecord> records;
	private Parameters globalParameters;
	private StateModel state;
	private StrategyType strategy;
	private Statistics weeklyTrend;

	private long startTime, endTime;

	private double budget, cpa, cpm;
	private double priorWeight;

	private double conversionExcess, impressionExcess;
	private int totalCumulativeConversions, totalCumulativeImpressions;
	private double totalCumulativeSpent;
	private double[] cumulativeSpent;
	private int[] cumulativeImpressions;
	private double targetConversions, targetImpressions;
	private double trend, delivery;
	private int deletedRecords, writtenRecords, deliveredRecords;

	private SingleGoalOptimizer() {
		trend = Double.MAX_VALUE;
		delivery = Double.MAX_VALUE;
		deletedRecords = writtenRecords = deliveredRecords = 0;
		weeklyTrend = new Statistics();
	}

	public SingleGoalOptimizer(long campaignId, int goalId, Parameters globalParameters) {
		this();
		initialize(campaignId, goalId);
		this.globalParameters = globalParameters;
	}
	
	public int getGoalId() {
		return goalId;
	}
		
	public int getReadCount() {
		return records.size();
	}
	
	public int getDeletedCount() {
		return deletedRecords;
	}
	
	public int getWrittenCount() {
		return writtenRecords;
	}
	
	public int getDeliveredCount() {
		return deliveredRecords;
	}
	
	public String getDailyConversionFactor() {
		return weeklyTrend.factorToString();
	}
	
	public boolean readCampaignData(String pvDate) throws ParseException, SQLException {
		if (!readParameters(campaignId, goalId))
			return false;
		if (!readSegments(campaignId, false, false))
			return false;
		if (!readPriceVolumeCurves(pvDate))
			return false;
		
		backupCampaignPerformanceData(campaignId, goalId);
		state = new StateModel(globalParameters, campaignParameters, priorWeight, segments, strategy);

		conversionExcess = impressionExcess = 0.0;
		cumulativeSpent = new double[segmentCount];
		totalCumulativeConversions = 0;
		cumulativeImpressions = new int[segmentCount];
		for (int i = 0; i < segmentCount; i++) {
			cumulativeImpressions[i] = 0;
			cumulativeSpent[i] = 0.0;
		}
		
		return true;
	}

	@Override
	public boolean readParameters(long campaignId, int goalId) throws ParseException, SQLException {
		campaignParameters = new CampaignParameters(campaignId, goalId, campaignSettingsTable);
		int status = campaignParameters.getInt(Constants.STATUS, 0);
		if (status == 0)
			return false;
		
		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		if (segmentCount != 2) {
			String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
			log.error(campaignName + " does not have 2 audience segments");
			return false;
		}
		startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		budget = campaignParameters.getDouble(Constants.BUDGET, 0.0);
		cpa = campaignParameters.getDouble(Constants.CPA, 0.0);
		cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		priorWeight = campaignParameters.getDouble(Constants.PRIOR_WEIGHT, 0.0);

		targetConversions = budget / cpa;
		targetImpressions = 1000.0 * budget / cpm;
		String strStrategy = campaignParameters.get(Constants.STRATEGY, StrategyType.getDefault().toString());
		strategy = StrategyType.fromString(strStrategy);
		
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		double intervals = (double)(endTime - startTime)/(deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
		campaignParameters.put(Constants.CONVERSIONS_PER_INTERVAL, String.format("%.1f", targetConversions/intervals));
		campaignParameters.put(Constants.IMPRESSIONS_PER_INTERVAL, String.format("%.1f", targetImpressions/intervals));

		return true;
	}

	private void backupCampaignPerformanceData(long campaignId, int goalId) throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("DELETE FROM %s WHERE campaignId = %d AND goalId = %d;", campaignPerformancesBackupTable, campaignId, goalId);
		statement.executeUpdate(query);
		query = String.format("INSERT INTO %s SELECT * FROM %s WHERE campaignId = %d AND goalId = %d;", campaignPerformancesBackupTable, campaignPerformancesTable, campaignId, goalId);
		statement.executeUpdate(query);
		connection.close();
		connection = null;
	}

	public void updateCampaignPerformanceRecords() throws IOException, SQLException, ParseException {
		String precedingCampaignIds = campaignParameters.get(Constants.PRECEDING_CAMPAIGN_IDS);
		List<PerformanceRecord> precedingRecords = readPrecedingRecords(precedingCampaignIds);
		for (PerformanceRecord record : precedingRecords) {
			state.updateState(record, false);
			updateStatistics(record);
		}

		records = readRecords(campaignId, goalId);
		for (PerformanceRecord record : records) {
			updatePredictedImpressions(record);
			updatePerformaceFOMs(record);
			updateStatusAndTrend(record);
			state.updateState(record, true);
			updateStatistics(record);
		}
		writeUpdatedCampaignPerformanceRecords();
	}
	
	private void writeUpdatedCampaignPerformanceRecords() throws IOException, SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		for (PerformanceRecord record : records) {
			writtenRecords += statement.executeUpdate(record.writeQuery(campaignPerformancesTable));
		}
		connection.close();
		connection = null;
	}
	
	private List<PerformanceRecord> readRecords(long campaignId, int goalId) throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();

		String query = String.format(
				"DELETE FROM %s WHERE campaignId = %d AND goalId = %d AND (imps_d_1 IS NULL OR imps_d_2 IS NULL);",
				campaignPerformancesTable, campaignId, goalId);
		deletedRecords = statement.executeUpdate(query);

		query = String.format("SELECT * FROM %s WHERE campaignId = %d AND goalId = %d ORDER BY date ASC;", campaignPerformancesTable, campaignId, goalId);
		ResultSet result = statement.executeQuery(query);

		List<PerformanceRecord> records = new ArrayList<PerformanceRecord>();
		while (result.next()) {
			PerformanceRecord record = new PerformanceRecord(result, segmentCount);
			records.add(record);
		}
		connection.close();
		connection = null;
		return records;
	}

	private List<PerformanceRecord> readPrecedingRecords(String strIds) throws SQLException, ParseException {
		List<PerformanceRecord> records = new ArrayList<PerformanceRecord>();
		if (strIds == null || strIds.trim().length() == 0)
			return records;

		List<Long> campaignIds = new ArrayList<Long>();
		String str = strIds.trim().replaceAll("\\s*,\\s*", ",");
		String[] ids = str.split("\\s+|,");
		for (int i = 0; i < ids.length; i++) {
			long id = Long.parseLong(ids[i].trim());
			campaignIds.add(id);
		}

		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();

		for (int i = 0; i < campaignIds.size(); i++) {
			long campaignId = campaignIds.get(i);
			String query = String
					.format("SELECT * FROM %s WHERE campaignId = %d AND goalId = %d AND imps_d_1 IS NOT NULL AND imps_d_2 IS NOT NULL ORDER BY date ASC;",
							campaignPerformancesTable, campaignId, goalId);
			ResultSet result = statement.executeQuery(query);
			while (result.next()) {
				PerformanceRecord record = new PerformanceRecord(result, segmentCount);
				records.add(record);
			}
		}

		connection.close();
		connection = null;

		return records;
	}

	private void updatePredictedImpressions(PerformanceRecord record) throws ParseException {
		double deliveryInterval = (record.getDeliveryInterval() > 0 ? record.getDeliveryInterval() : campaignParameters
				.getDouble(Constants.DELIVERY_INTERVAL, 0.0));
		long recordTime = record.getTime();
		double hoursLeft = (double) (endTime - recordTime) / (double) Constants.MILLISECONDS_PER_HOUR;

		double intervalsLeft = Math.max(1.0, hoursLeft / deliveryInterval);
		int n = (int) Math.round(intervalsLeft);
		double totalDelivery = 0.0;
		for (int i = 0; i < n; i++) {
			long time = recordTime + (long) (i * deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
			totalDelivery += strategy.getDeliveryFactor(time, 0);
		}
		double deliveryFraction = (totalDelivery > 0 ? 1.0 / totalDelivery : 0.0);
		
		double conversions = deliveryFraction * (targetConversions - totalCumulativeConversions);
		double impressions = deliveryFraction * (targetImpressions - totalCumulativeImpressions);
		double leaveOutImpressions = (double) record.getLeaveOutImpressions();
		if (impressions < leaveOutImpressions && leaveOutImpressions > 0) {
			impressions = 1000.0;
		} else {
			impressions -= leaveOutImpressions;
		}
		
		if (conversionExcess < 0) {
			double reversionIntervals = (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000);
			if (reversionIntervals < intervalsLeft) {
				conversions = campaignParameters.getDouble(Constants.CONVERSIONS_PER_INTERVAL, 0.0) - conversionExcess/reversionIntervals;
			}
			double impressionIntervalsLeft = (double) globalParameters.getInt(Constants.IMPRESSION_INTERVALS_LEFT, 0);
			if (intervalsLeft < impressionIntervalsLeft) {
				impressions = Math.max(impressions, campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0));
			}
		}

		if (impressionExcess < 0) {
			int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
			double endOfCampaignDeliveryIntervals = 24.0 * endOfCampaignDeliveryPeriodDays / deliveryInterval;
			if (intervalsLeft <= endOfCampaignDeliveryIntervals) {
				deliveryFraction = (totalDelivery > 0 ? 1.0 : 0.0);
			} else {
				deliveryFraction = (totalDelivery > 0 ? intervalsLeft / totalDelivery : 0.0);
			}
			
			double reversionIntervals = Math.min(intervalsLeft, (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000));
			impressions = deliveryFraction * campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0)
									- impressionExcess/reversionIntervals;
		}
		
		if (impressions > 0) {
			double maxEcpa = campaignParameters.getDouble(Constants.MAX_ECPA, Double.MAX_VALUE);
			double conversionFloor = 0.001 * cpm * impressions / maxEcpa;
			conversions = Math.max(conversions,  conversionFloor);
		}
		
		double conversionFactor = (DateUtils.isWeekend(recordTime) ? 1.0 : 1.0 + globalParameters.getDouble(Constants.KPI_GOAL_MARGIN, 0.0));
		state.optimize(conversionFactor * conversions, impressions, impressionExcess, recordTime);
		record.updatePredictions(state);
	}

	private void updatePerformaceFOMs(PerformanceRecord record) throws ParseException {
		totalCumulativeSpent = 0.0;
		totalCumulativeConversions += record.getTotalDeliveredConversions();
		totalCumulativeImpressions = 0;
		for (int i = 0; i < segmentCount; i++) {
			cumulativeSpent[i] += 0.001 * record.getEcpm(i) * record.getDeliveredImpressions(i);
			totalCumulativeSpent += cumulativeSpent[i];
			cumulativeImpressions[i] += record.getDeliveredImpressions(i);
			totalCumulativeImpressions += cumulativeImpressions[i];
		}

		long currentTime = record.getTime();
		long timeInterval = (long) (record.getDeliveryInterval() * Constants.MILLISECONDS_PER_HOUR);
		double deliveryFraction = (double) (currentTime + timeInterval - startTime) / (double) (endTime - startTime);
		conversionExcess = totalCumulativeConversions - deliveryFraction * targetConversions;
		record.setConversionExcess(conversionExcess);
		impressionExcess = totalCumulativeImpressions - deliveryFraction * targetImpressions;
		record.setImpressionExcess(impressionExcess);
		double budgetExcess = totalCumulativeSpent - deliveryFraction * budget;
		record.setBudgetExcess(budgetExcess);
		record.setCumulative(totalCumulativeSpent, totalCumulativeConversions, totalCumulativeImpressions);
		
		double deliveryFractionThreshold = (double) (endTime - startTime - 2.1*timeInterval) / (double) (endTime - startTime);
		double margin = (deliveryFraction > deliveryFractionThreshold ? 
				-budgetExcess / (deliveryFraction * budget) : 1.0 - totalCumulativeSpent / (0.001 * cpm * totalCumulativeImpressions));
		double ecpat = (totalCumulativeConversions == 0 ? 2000.0 : (0.001 * cpm * totalCumulativeImpressions) 
				/ totalCumulativeConversions); 
		double ecpmt = 1000.0 * totalCumulativeSpent / totalCumulativeImpressions;
		record.setMargin(100.0 * margin);
		record.setEcpa(ecpat);
		record.setEcpm(ecpmt);

		// System.out.printf("%4.2f\n", record.a[1] * record.ecpm[0] /
		// (record.a[0] * record.ecpm[1]));
		// System.out.println(record.toString());
	}

	private void updateStatusAndTrend(PerformanceRecord record) throws ParseException {
		if (record.getDeliveryInterval() == 0)
			return;
		double deliveryInterval = record.getDeliveryInterval() * (double) Constants.MILLISECONDS_PER_HOUR;
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double deliveryFraction = deliveryInterval / (double) (endTime - startTime);
		double dailyConversionGoal = deliveryFraction * record.getTotalConversionGoal();
		double currentTrend = (record.getTotalDeliveredConversions() - dailyConversionGoal) / dailyConversionGoal;
		trend = (trend > 1000000.0 ? currentTrend : 0.4 * trend + 0.6 * currentTrend);
		double currentDelivery = (double) record.getTotalDeliveredImpressions() / (double) record.getTotalImpressionTarget() - 1.0;
		delivery = (delivery > 1000000.0 ? currentDelivery : 0.4 * delivery + 0.6 * currentDelivery);
		record.setStatusAndTrend(trend, dailyConversionGoal, delivery);
	}
	
	private void updateStatistics(PerformanceRecord record) throws ParseException {
		if (record.getDeliveryInterval() == 0 || strategy.isWeekdayDelivery()) {
			return;
		}
		weeklyTrend.add(record);
		if (weeklyTrend.updateDailyConversionFactor()) {
			double[] factor = weeklyTrend.getDailyConversionFactor();
			strategy.setNonuniformDeliveryFactor(factor);
			state.setDailyConversionFactor(factor);
		}
	}
	
	private void setEndOfCampaignDeliveryFactor(long currentTime) {
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
		if (currentTime + endOfCampaignDeliveryPeriodDays * 24L * Constants.MILLISECONDS_PER_HOUR + 10L > endTime) {
			strategy.setEndOfCampaignDeliveryFactor();
//			log.info("state factor: " + state.strategy.deliveryFactorToString());
		}
	}
	
	public void predictImpressionTargets() throws ParseException, SQLException, IOException {
		double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		
		double dayLength = (double) (DateUtils.getToday(0) - DateUtils.getToday(-1)) / (double) Constants.MILLISECONDS_PER_HOUR;
		double numberOfIntervals = Math.round(dayLength / deliveryInterval);
		deliveryInterval = dayLength / numberOfIntervals;
		
		long recordTime = (records.isEmpty() ? startTime : records.get(records.size() - 1).getTime()
				+ (long) (deliveryInterval * Constants.MILLISECONDS_PER_HOUR));

		double hoursLeft = (double) (endTime - recordTime) / (double) Constants.MILLISECONDS_PER_HOUR;

		if (hoursLeft <= 0) {
			deleteImpressionTargets();
			return;
		}

		long currentTime = DateUtils.currentTime();
		long nextRecordTime = recordTime + (long) (deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
		boolean isCurrentRecord = (currentTime >= recordTime && currentTime < nextRecordTime);
		
		setEndOfCampaignDeliveryFactor(recordTime);
		
		double intervalsLeft = Math.max(1.0, hoursLeft / deliveryInterval);
		int n = (int) Math.round(intervalsLeft);
		double totalDelivery = 0.0;
		for (int i = 0; i < n; i++) {
			long time = recordTime + (long) (i * deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
			totalDelivery += strategy.getDeliveryFactor(time, 0);
		}
		double deliveryFraction = (totalDelivery > 0 ? 1.0 / totalDelivery : 0.0);

		double conversions = deliveryFraction * (targetConversions - totalCumulativeConversions);
		double impressions = deliveryFraction * (targetImpressions - totalCumulativeImpressions);
		double leaveOutImpressions = (double) campaignParameters.getInt(Constants.LEAVE_OUT_IMPRESSIONS, 0);
		if (impressions < leaveOutImpressions && leaveOutImpressions > 0) {
			impressions = 1000.0;
		} else {
			impressions -= leaveOutImpressions;
		}
		if (conversionExcess < 0) {
			double reversionIntervals = (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000);
			if (reversionIntervals < intervalsLeft) {
				conversions = campaignParameters.getDouble(Constants.CONVERSIONS_PER_INTERVAL, 0.0) - conversionExcess/reversionIntervals;
			}
			double impressionIntervalsLeft = (double) globalParameters.getInt(Constants.IMPRESSION_INTERVALS_LEFT, 0);
			if (intervalsLeft < impressionIntervalsLeft) {
				impressions = Math.max(impressions, campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0));
			}
		}

		if (impressionExcess < 0) {
			int endOfCampaignDeliveryPeriodDays = globalParameters.getInt(Constants.END_OF_CAMPAIGN_DELIVERY_PERIOD_DAYS, 3);
			double endOfCampaignDeliveryIntervals = 24.0 * endOfCampaignDeliveryPeriodDays / deliveryInterval;
			if (intervalsLeft <= endOfCampaignDeliveryIntervals) {
				deliveryFraction = (totalDelivery > 0 ? 1.0 : 0.0);
			} else {
				deliveryFraction = (totalDelivery > 0 ? intervalsLeft / totalDelivery : 0.0);
			}
			
			double reversionIntervals = Math.min(intervalsLeft, (double) campaignParameters.getInt(Constants.REVERSION_INTERVALS, 1000));
			impressions = deliveryFraction * campaignParameters.getDouble(Constants.IMPRESSIONS_PER_INTERVAL, 0.0)
									- impressionExcess/reversionIntervals;
		}

		boolean maxEcpaExceeded = false;
		if (impressions > 0) {
			double maxEcpa = campaignParameters.getDouble(Constants.MAX_ECPA, Double.MAX_VALUE);
			double conversionFloor = 0.001 * cpm * impressions / maxEcpa;
			if (conversions < conversionFloor) {
				maxEcpaExceeded = (conversions > 0);
				conversions = conversionFloor;
			}
		}
		
		double conversionFactor = (DateUtils.isWeekend(recordTime) ? 1.0 : 1.0 + globalParameters.getDouble(Constants.KPI_GOAL_MARGIN, 0.0));
		state.optimize(conversionFactor * conversions, impressions, impressionExcess, recordTime);
		if (isCurrentRecord) {
			if (strategy.isPaused()) {
				deliveryRecord = DeliveryRecord.getPausedRecord(recordTime, segments);
			} else {
				deliveryRecord = state.getDeliveryRecord();	
				deliveryRecord.validate(campaignParameters, globalParameters);
				deliveryRecord.addGoalMessages(targetConversions < totalCumulativeConversions, targetImpressions < totalCumulativeImpressions);
				if (maxEcpaExceeded) {
					deliveryRecord.updateDeliveryMessages("Max eCPA exceeded");
				}
			}
		} else {
			deliveryRecord = null;
		}
		PerformanceRecord predictedRecord = state.getPerformanceRecord();
		writeCampaignPerformanceRecord(predictedRecord);
	}
	
	public double getKpiScore() {
		if (records.isEmpty()) {
			return cpa;
		}
		double ecpa = records.get(records.size() - 1).getEcpa();
		return (ecpa - cpa)/cpa;
	}
	
	private void writeCampaignPerformanceRecord(PerformanceRecord record) throws IOException, SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		writtenRecords += statement.executeUpdate(record.writeQuery(campaignPerformancesTable));
		connection.close();
		connection = null;
	}

	private void deleteImpressionTargets() throws SQLException, ParseException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		for (Segment segment : segments) {
			for (AdGroup adGroup : segment.getAdGroups()) {
				String query = String.format("DELETE FROM %s WHERE adGroupId = %d;", Constants.DO_IMPRESSION_TARGETS, adGroup.getId());
				statement.executeUpdate(query);
			}
		}
		connection.close();
		connection = null;
	}

	public boolean writeImpressionTargets() throws SQLException, ParseException {
		if (deliveryRecord == null) {
			return false;
		} else {
			deliveredRecords += deliveryRecord.writeImpressionTargets(globalParameters, campaignParameters, campaignId, goalId);
			return true;
		}
	}

	public String getReport(int activeGoalId) {
		String tag = (activeGoalId == goalId ? ">" : " ");
		return String.format("%sRecords for campaignId = %d, goalId = %d: deleted: %d, read: %2d, written: %2d, delivered: %d, conversion factor: %s",
				tag, campaignId, goalId, deletedRecords, records.size(), writtenRecords, deliveredRecords, weeklyTrend.factorToString());
	}
}