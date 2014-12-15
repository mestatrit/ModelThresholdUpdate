package com.sharethis.delivery.record;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.optimization.StateModel;
import com.sharethis.delivery.util.DateUtils;

public class PerformanceRecord {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private long id;
	private long campaignId;
	private int goalId;
	private String campaignName;
	private long time;
	private double deliveryInterval;
	private double budget, cpa, cpm;
	private int leaveOutImpressions;
	private double cumSpent;
	private int totalCumulativeConversions, totalCumulativeImpressions;
	private double margin, trend, status, delivery;
	private double ecpat, ecpmt;
	private double[] ecpm, maxCpm;
	private double[] conversionRatio;
	private double conversionRatioTarget;
	private double totalDeliveredConversions, totalConversionTarget;
	private int totalImpressions;
	private double budgetExcess;
	private double conversionExcess;
	private double impressionExcess;
	private int[] impressionTarget;
	private int[] deliveredImpressions;
	private double singularity;
	private int segmentCount;
	private RecordType recordType;

	public PerformanceRecord(Parameters campaignParameters, StateModel state) {
		recordType = RecordType.ESTIMATE;
		campaignId = campaignParameters.getLong(Constants.CAMPAIGN_ID, 0L);
		campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
		goalId = campaignParameters.getInt(Constants.GOAL_ID, 0);
		budget = campaignParameters.getDouble(Constants.BUDGET, 0.0);
		cpa = campaignParameters.getDouble(Constants.CPA, 0.0);
		cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		leaveOutImpressions = campaignParameters.getInt(Constants.LEAVE_OUT_IMPRESSIONS, 0);
		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		time = state.getTime();

		deliveredImpressions = new int[segmentCount];
		for (int i = 0; i < segmentCount; i++) {
			deliveredImpressions[i] = 0;
		}
		ecpm = new double[segmentCount];
		conversionRatio = new double[segmentCount];
		
		totalConversionTarget = state.getTotalConversionTarget();
		impressionTarget = state.getImpressionTarget();
		conversionRatioTarget = state.getConversionRatioTarget();
		totalImpressions = getTotalImpressionTarget();
		maxCpm = state.getMaxCpm();
		totalDeliveredConversions = -1;
	}
	
	public void setMargin(double margin) {
		this.margin= margin;
	}
	
	public void setStatusAndTrend(double trend, double dailyConversionGoal, double delivery) {
		this.trend = trend;
		this.status = conversionExcess/dailyConversionGoal;
		this.delivery = delivery;
	}
	
	public void setEcpm(double ecpmt) {
		this.ecpmt = ecpmt;
	}
	
	public void setEcpa(double ecpat) {
		this.ecpat = ecpat;
	}
	
	public double getEcpa() {
		return ecpat;
	}
	
	public void setConversionExcess(double conversionExcess) {
		this.conversionExcess = conversionExcess;
	}
	
	public void setImpressionExcess(double impressionExcess) {
		this.impressionExcess = impressionExcess;
	}
	
	public void setBudgetExcess(double budgetExcess) {
		this.budgetExcess = budgetExcess;
	}
	
	public void setCumulative(double cumSpent, int totalCumulativeConversions, int totalCumulativeImpressions) {
		this.cumSpent = cumSpent;
		this.totalCumulativeConversions = totalCumulativeConversions;
		this.totalCumulativeImpressions = totalCumulativeImpressions;
	}

	public void updatePredictions(StateModel state) {
		totalConversionTarget = state.getTotalConversionTarget(this);
		conversionRatioTarget = state.getConversionRatioTarget();
		if (Constants.UPDATE_IMPRESSION_TARGET) {
			impressionTarget = state.getImpressionTarget();
		}
	}
	
	public void setEstimatedConversionRatios(double a1p, double a2p, double singularity) {
		conversionRatio[0] = a1p;
		conversionRatio[1] = a2p;
		this.singularity = singularity;
	}
	
	public String getTarget() {
		if (status > 0.33) {
			return "ahead";
		} else if (status < -0.33) {
			return "behind";
		} else {
			return "match";
		}
	}
	
	public String getTrend() {
		if (trend > 0.33) {
			return "up";
		} else if (trend < -0.33) {
			return "down";
		} else {
			return "stable";
		}
	}
	
	public String getDelivery() {
		if (delivery > 0.2) {
			return "ahead";
		} else if (delivery < -0.2) {
			return "behind";
		} else {
			return "match";
		}
	}
	
	public long getTime() {
		return time;
	}
	
	public int getMonth() {
		return DateUtils.getMonth(time);
	}
	
	public double getEcpm(int i) {
		return ecpm[i];
	}
	
	public double[] getMaxCpm() {
		return maxCpm;
	}
	
	public int getLeaveOutImpressions() {
		return leaveOutImpressions;
	}
	
	public int getTotalDeliveredImpressions() {
		int imps = 0;
		for (int i = 0; i < segmentCount; i++) {
			imps += deliveredImpressions[i];
		}
		return imps;
	}

	public int getDeliveredImpressions(int i) {
		return deliveredImpressions[i];
	}

	public double getSpent(int i) {
		return 0.001 * ecpm[i] * deliveredImpressions[i];
	}
	
	public int getTotalImpressionTarget() {
		int imps = 0;
		for (int i = 0; i < segmentCount; i++) {
			imps += impressionTarget[i];
		}
		return imps;
	}
	
	public double getTotalConversionGoal() {
		return budget/cpa;
	}
	
	public double getTotalDeliveredConversions() {
		return (double) totalDeliveredConversions;
	}
	
	public double getDeliveryInterval() {
		return deliveryInterval;
	}
	
	private double getRealizedConversionRatio() {
		// return (a[0]*deliveredImps[0] + a[1]*deliveredImps[1]) /
		// (deliveredImps[0] + deliveredImps[1]);
		return (totalImpressions == 0 ? 0.0 : totalDeliveredConversions / (double) totalImpressions);
	}
	
	public double[] getEstimatedConversionRatios() {
		return conversionRatio;
	}
	
	public boolean isPaused() {
		return (recordType == RecordType.PAUSED);
	}
	
	public String toString() {
		String datetime = DateUtils.getDatetime(time);
		String str = String.format("%s\t%.1f\t%4.1f\t%5.2f", datetime, deliveryInterval, margin, ecpat);
		for (int i = 0; i < segmentCount; i++) {
			str += String.format("\t%5.2f", ecpm[i]);
		}
		for (int i = 0; i < segmentCount; i++) {
			str += String.format("\t%7.1e", conversionRatio[i]);
		}
		str += String.format("\t%7.1e\t%7.1e\t%3d\t%7d\t%8.1f\t%5.0f\t%7.0f", getRealizedConversionRatio(), conversionRatioTarget,
				totalDeliveredConversions, totalImpressions, budgetExcess, conversionExcess, impressionExcess);
		for (int i = 0; i < segmentCount; i++) {
			str += String.format("\t%7d\t%7d", impressionTarget[i], deliveredImpressions[i]);
		}
		str += String.format("\t%6.1f", singularity);
		return str;
	}

	public String writeQuery(String campaignPerformancesTable) throws SQLException {
		String datetime = DateUtils.getDatetime(time);
		double ecpatMax = 1000.0 * cpa;
		if (recordType == RecordType.COMPLETE) {
			double roiRatio = Math.min(100.0, conversionRatio[1] * ecpm[0] / (conversionRatio[0] * ecpm[1]));
			return String.format("UPDATE %s SET campaignId=%d,campaignName='%s',goalId=%d,date='%s',deliveryInterval=%f,target='%s',trend='%s',delivery='%s', cumSpent=%f,cumConversions=%d,cumImpressions=%d,margin=%f,ecpa=%f,ecpm=%f,ecpm1=%f,ecpm2=%f,a1=%f,a2=%f,aeff=%f,agoal=%f,predictedConversions=%d,conversions=%d,impressions=%d,budgetExcess=%f,conversionExcess=%f,impressionExcess=%f,imps_p_1=%d,imps_d_1=%d,imps_p_2=%d,imps_d_2=%d,roiRatio=%f,singularity=%f,budget=%f,cpa=%f,cpm=%f,leaveOutImpressions=%d,maxCpm1=%f,maxCpm2=%f WHERE id=%d;",
							campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval, getTarget(), getTrend(), getDelivery(), round(cumSpent,2), totalCumulativeConversions, totalCumulativeImpressions,round(margin, 1),
							(ecpat>ecpatMax?null:round(ecpat, 2)), round(ecpmt, 2), ecpm[0], ecpm[1], round(conversionRatio[0], 3), round(conversionRatio[1], 3),
							round(getRealizedConversionRatio(), 3), round(conversionRatioTarget, 3), (int)totalConversionTarget, (int)totalDeliveredConversions, totalImpressions,
							round(budgetExcess, 1), conversionExcess, impressionExcess, impressionTarget[0],
							deliveredImpressions[0], impressionTarget[1], deliveredImpressions[1], round(roiRatio, 2), round(singularity, 1), budget, cpa, cpm, leaveOutImpressions, maxCpm[0], maxCpm[1], id);
		} else if (recordType == RecordType.ESTIMATE) {
			return String.format("INSERT INTO %s (campaignId,campaignName,goalId,date,deliveryInterval,agoal,predictedConversions,imps_p_1,imps_p_2,budget,cpa,cpm,leaveOutImpressions,maxCpm1,maxCpm2) VALUES (%d,'%s',%d,'%s',%f,%f,%d,%d,%d,%f,%f,%f,%d,%f,%f);", 
							campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval,
							round(conversionRatioTarget, 3), (int)totalConversionTarget, impressionTarget[0], impressionTarget[1], budget, cpa, cpm, leaveOutImpressions, maxCpm[0],maxCpm[1]);
		} else if (recordType == RecordType.PAUSED)  {
			return String.format("UPDATE %s SET campaignId=%d,campaignName='%s',goalId=%d,date='%s',deliveryInterval=%f, target='%s',trend='%s',delivery='%s',cumSpent=%f,cumConversions=%d,cumImpressions=%d,margin=%f,ecpa=%f,ecpm=%f,ecpm1=%f,ecpm2=%f ,agoal=%f,predictedConversions=%d,conversions=%d,impressions=%d,budgetExcess=%f,conversionExcess=%f,impressionExcess=%f,imps_p_1=%d,imps_d_1=%d,imps_p_2=%d,imps_d_2=%d,budget=%f,cpa=%f,cpm=%f,leaveOutImpressions=%d,maxCpm1=%f,maxCpm2=%f WHERE id=%d;", 
						campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval, 
						getTarget(), getTrend(), getDelivery(), round(cumSpent,2), totalCumulativeConversions, totalCumulativeImpressions, round(margin, 1), (ecpat>ecpatMax?null:round(ecpat, 2)), round(ecpmt, 2), ecpm[0], ecpm[1],
						round(conversionRatioTarget, 3), (int)totalConversionTarget, (int)totalDeliveredConversions,
							totalImpressions, round(budgetExcess, 1), conversionExcess, impressionExcess, impressionTarget[0],
							deliveredImpressions[0], impressionTarget[1], deliveredImpressions[1], budget, cpa, cpm, leaveOutImpressions, maxCpm[0],maxCpm[1], id);
		} else {
			throw new SQLException(String.format("Table %s not updated for %s: unknown record type: %s", campaignPerformancesTable, campaignName, recordType.toString()));
		}
	}

	public String writeSegmentQuery(int segmentId, String campaignPerformancesTable) throws SQLException {
		String datetime = DateUtils.getDatetime(time);
		if (recordType == RecordType.COMPLETE) {
			double roiRatio = Math.min(100.0, conversionRatio[1] * ecpm[0] / (conversionRatio[0] * ecpm[1]));
			return String.format("UPDATE %s SET campaignId=%d,campaignName='%s',goalId=%d,date='%s',deliveryInterval=%f,target='%s',trend='%s',delivery='%s', cumSpent=%f,cumConversions=%d,cumImpressions=%d,margin=%f,ecpa=%f,ecpm=%f,ecpm1=%f,ecpm2=%f,a1=%f,a2=%f,aeff=%f,agoal=%f,predictedConversions=%d,conversions=%d,impressions=%d,budgetExcess=%f,conversionExcess=%f,impressionExcess=%f,imps_p_1=%d,imps_d_1=%d,imps_p_2=%d,imps_d_2=%d,roiRatio=%f,singularity=%f,budget=%f,cpa=%f,cpm=%f,leaveOutImpressions=%d,maxCpm1=%f,maxCpm2=%f WHERE id=%d;",
							campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval, getTarget(), getTrend(), getDelivery(), round(cumSpent,2), totalCumulativeConversions, totalCumulativeImpressions, round(margin, 1),
							round(ecpat, 2), round(ecpmt, 2), ecpm[0], ecpm[1], round(conversionRatio[0], 3), round(conversionRatio[1], 3),
							round(getRealizedConversionRatio(), 3), round(conversionRatioTarget, 3), (int)totalConversionTarget, (int)totalDeliveredConversions, totalImpressions,
							round(budgetExcess, 1), conversionExcess, impressionExcess, impressionTarget[0],
							deliveredImpressions[0], impressionTarget[1], deliveredImpressions[1], round(roiRatio, 2), round(singularity, 1), budget, cpa, cpm, leaveOutImpressions, maxCpm[0], maxCpm[1], id);
		} else if (recordType == RecordType.ESTIMATE) {
			return String.format("INSERT INTO %s (campaignId,campaignName,goalId,date,deliveryInterval,agoal,predictedConversions,imps_p_1,imps_p_2,budget,cpa,cpm,leaveOutImpressions,maxCpm1,maxCpm2) VALUES (%d,'%s',%d,'%s',%f,%f,%d,%d,%d,%f,%f,%f,%d,%f,%f);", 
							campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval,
							round(conversionRatioTarget, 3), (int)totalConversionTarget, impressionTarget[0], impressionTarget[1], budget, cpa, cpm, leaveOutImpressions, maxCpm[0],maxCpm[1]);
		} else if (recordType == RecordType.PAUSED)  {
			return String.format("UPDATE %s SET campaignId=%d,campaignName='%s',goalId=%d,date='%s',deliveryInterval=%f, target='%s',trend='%s',delivery='%s',cumSpent=%f,cumConversions=%d,cumImpressions=%d,margin=%f,ecpa=%f,ecpm=%f,ecpm1=%f,ecpm2=%f ,agoal=%f,predictedConversions=%d,conversions=%d,impressions=%d,budgetExcess=%f,conversionExcess=%f,impressionExcess=%f,imps_p_1=%d,imps_d_1=%d,imps_p_2=%d,imps_d_2=%d,budget=%f,cpa=%f,cpm=%f,leaveOutImpressions=%d,maxCpm1=%f,maxCpm2=%f WHERE id=%d;", 
						campaignPerformancesTable, campaignId, campaignName, goalId, datetime, deliveryInterval, 
						getTarget(), getTrend(), getDelivery(), round(cumSpent,2), totalCumulativeConversions, totalCumulativeImpressions, round(margin, 1), round(ecpat, 2), round(ecpmt, 2), ecpm[0], ecpm[1],
						round(conversionRatioTarget, 3), (int)totalConversionTarget, (int)totalDeliveredConversions,
							totalImpressions, round(budgetExcess, 1), conversionExcess, impressionExcess, impressionTarget[0],
							deliveredImpressions[0], impressionTarget[1], deliveredImpressions[1], budget, cpa, cpm, leaveOutImpressions, maxCpm[0],maxCpm[1], id);
		} else {
			throw new SQLException(String.format("Table %s not updated for %s: unknown record type: %s", campaignPerformancesTable, campaignName, recordType.toString()));
		}
	}
	
	public PerformanceRecord(ResultSet result, int segments) throws ParseException, SQLException {
		this.segmentCount = segments;
		ecpm = new double[segments];
		maxCpm = new double[segments];
		conversionRatio = new double[segments];
		
		id = result.getLong("id");
		campaignId = result.getLong("campaignId");
		campaignName = result.getString("campaignName");
		goalId = result.getInt("goalId");
		time = DateUtils.parseTime(result, "date");
		budget = result.getDouble("budget");
		cpa = result.getDouble("cpa");
		cpm = result.getDouble("cpm");
		leaveOutImpressions = result.getInt(Constants.LEAVE_OUT_IMPRESSIONS);
		deliveryInterval = result.getDouble("deliveryInterval");
		totalConversionTarget = result.getInt("predictedConversions");
		cumSpent = result.getDouble("cumSpent");
		totalCumulativeConversions = result.getInt("cumConversions");
		totalCumulativeImpressions = result.getInt("cumImpressions");
		margin = result.getDouble("margin");
		ecpat = result.getDouble("ecpa");
		for (int i = 0; i < segments; i++) {
			ecpm[i] = result.getDouble(String.format("%s%d", "ecpm", i + 1));
			maxCpm[i] = result.getDouble(String.format("%s%d", "maxCpm", i + 1));
		}
		for (int i = 0; i < segments; i++) {
			conversionRatio[i] = result.getDouble(String.format("%s%d", "a", i + 1));
		}
//		realizedConversionRatio = result.getDouble("aeff"); re-calculated each time when record read
		conversionRatioTarget = result.getDouble("agoal");
		totalDeliveredConversions = result.getInt("conversions");
		totalImpressions = result.getInt("impressions");
		budgetExcess = result.getDouble("budgetExcess");
		conversionExcess = result.getInt("conversionExcess");
		impressionExcess = result.getInt("impressionExcess");
		impressionTarget = new int[segments];
		deliveredImpressions = new int[segments];
		for (int i = 0; i < segments; i++) {
			int predicted = result.getInt(String.format("%s%d", "imps_p_", i + 1));
			int delivered = result.getInt(String.format("%s%d", "imps_d_", i + 1));
			impressionTarget[i] = predicted;
			deliveredImpressions[i] = delivered;
		}
		singularity = result.getDouble("singularity");
		totalImpressions = getTotalDeliveredImpressions();
		recordType = (totalImpressions < 1000 ? RecordType.PAUSED : RecordType.COMPLETE);
	}

	private double round(double x, int n) {
		double ax = Math.abs(x);
		double e = n + (ax < 1 ? Math.floor(-Math.log10(Math.max(ax, 1.0e-8))) : 0);
		double d = Math.pow(10.0, e);
		x = Math.round(d * x) / d;
		return x;
	}
}
