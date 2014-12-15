package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

@SuppressWarnings("serial")
public class CampaignStates extends ArrayList<State> {
	private long campaignId;
	private int goalId;
	private int segmentCount;
	private Parameters campaignParameters;
	private int currentCampaignIndex;
	private Metric cumAuditMetric;
	private Metric cumDeliveryMetric;
	private double status, trend, delivery;
	private CampaignType campaignType;
	private Errors errors;

	public CampaignStates(Parameters campaignParameters) {
		super();
		this.campaignParameters = campaignParameters;
		campaignId = campaignParameters.getLong(Constants.CAMPAIGN_ID, 0);
		goalId = campaignParameters.getInt(Constants.GOAL_ID, 0);
		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		cumAuditMetric = new Metric();
		cumDeliveryMetric = new Metric();
		campaignType = CampaignType.valueOf(campaignParameters);
		trend = Double.NaN;
		delivery = Double.NaN;
	}

	public boolean readStates() throws ParseException, SQLException {
		String precedingCampaignIds = campaignParameters.get(Constants.PRECEDING_CAMPAIGN_IDS);
		List<Long> campaignIds = new ArrayList<Long>();
		if (precedingCampaignIds != null && precedingCampaignIds.trim().length() > 0) {
			String strIds = precedingCampaignIds.trim().replaceAll("\\s*,\\s*", ",");
			String[] ids = strIds.split("\\s+|,");
			for (int i = 0; i < ids.length; i++) {
				long id = Long.parseLong(ids[i].trim());
				campaignIds.add(id);
			}
		}
		campaignIds.add(campaignId);

		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);

		for (int i = 0; i < campaignIds.size(); i++) {
			long campaignId = campaignIds.get(i);
			if (i == campaignIds.size() - 1) {
				currentCampaignIndex = super.size();
			}
			StatusType status = new StatusType();
			status.setImpression();
			if (i < campaignIds.size() - 1) {
				status.setKpi();
			}
			String query = String
					.format("SELECT id, date, status FROM %s WHERE campaignId = %d AND goalId = %d AND status & %s > 0 ORDER BY date ASC;",
							Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, status.toString());
			ResultSet stateResult = statement.executeQuery(query);
			
			if (stateResult.next()) {
				do {
					State state = State.parseState(stateResult, campaignParameters);
					this.add(state);
				} while (stateResult.next());
			} else {
				continue;
			}
			
			for (State state : this) {
				long campaignRecordId = state.getId();
				query = String.format("SELECT * FROM %s WHERE campaignRecordId = %d ORDER BY segmentId ASC;",
						Constants.DO_SEGMENT_PERFORMANCES, campaignRecordId);
				ResultSet segmentResult = statement.executeQuery(query);
				if (!state.parseLogicalState(segmentResult)) {
					errors = state.getErrors();
					String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
					errors.log(campaignName);
					return false;
				}
			}
		}
		return true;
	}

	public Metric getCumDeliveryMetric() {
		return cumDeliveryMetric;
	}
	
	public Errors getErrors() {
		return errors;
	}

	public long getTargetTime() throws SQLException, ParseException {
		String query = String.format("SELECT date FROM %s WHERE campaignId = %d AND goalId = %d ORDER BY date DESC LIMIT 1;",
						Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long lastTime = DateUtils.parseTime(result, "date");
			long today = DateUtils.getToday(0);
			if (lastTime < today) {
				double deliveryInterval = campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
				double dayLength = DateUtils.getHoursPerDay(lastTime);
				double numberOfIntervals = Math.round(dayLength / deliveryInterval);
				deliveryInterval = dayLength / numberOfIntervals;
				lastTime += (long) (deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
			}
			return lastTime;
		} else {
			return campaignParameters.getLong(Constants.START_DATE, 0L);
		}
	}

	public Metric[] getCumulativeDeliveryMetrics(int day) {
		Metric[] cumulativeMetrics = new Metric[segmentCount];
		for (int p = 0; p < segmentCount; p++) {
			Metric cumMetric = new Metric();
			for (int i = currentCampaignIndex; i <= day; i++) {
				Metric metric = super.get(i).getDeliveredMetric(p);
				cumMetric.add(metric);
			}
			cumulativeMetrics[p] = cumMetric;
		}
		return cumulativeMetrics;
	}

	public Metric getTotalCumulativeDeliveryMetric(int day) {
		Metric totalCumulativeMetric = new Metric();
		if (day > super.size()) {
			return totalCumulativeMetric;
		}		
		for (int i = currentCampaignIndex; i < day; i++) {
			Metric metric = super.get(i).getTotalDeliveredMetric();
			totalCumulativeMetric.add(metric);
		}
		return totalCumulativeMetric;
	}
	
	public int getCurrentCampaignIndex() {
		return currentCampaignIndex;
	}
	
	public boolean isCurrentCampaign(int day) {
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		return (super.get(day).getTime() >= startTime);
	}
	
	private double getDeliveryFraction(int day) {
		long timeInterval = (long) (campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		long startTime = campaignParameters.getLong(Constants.START_DATE, 0L);
		long endTime = campaignParameters.getLong(Constants.END_DATE, 0L);
		double deliveryFraction = (double) (super.get(day).getTime() + timeInterval - startTime) / (double) (endTime - startTime);
		return deliveryFraction;
	}
	
	public String getTarget() {
		if (Double.isNaN(status)) {
			return "N/A";
		} else if (status > 0.05) {
			return "ahead";
		} else if (status < -0.05) {
			return "behind";
		} else {
			return "match";
		}
	}
	
	public String getTrend() {
		if (Double.isNaN(trend)) {
			return "N/A";
		} else if (trend > 0.1) {
			return "up";
		} else if (trend < -0.1) {
			return "down";
		} else {
			return "stable";
		}
	}
	
	public String getDelivery() {
		if (Double.isNaN(delivery)) {
			return "N/A";
		} else if (delivery > 0.2) {
			return "ahead";
		} else if (delivery < -0.2) {
			return "behind";
		} else {
			return "match";
		}
	}

	public int updateDoTables(int day) throws SQLException {
		State currentState = super.get(day);
		cumAuditMetric.add(currentState.getTotalAuditMetric());
		cumDeliveryMetric.add(currentState.getTotalDeliveredMetric());
		double fom = 1.0 / campaignType.getKpi();
		double eFom = campaignType.getFom(cumDeliveryMetric);
		status = (eFom - fom) / fom;
		
		Metric currentDeliveryMetric = currentState.getTotalDeliveredMetric();
		double currentImpressions = currentDeliveryMetric.getImpressions();
		double currentFom = campaignType.getFom(currentDeliveryMetric);
		
		double currentTrend = (currentFom - fom) / fom;
		if (!Double.isNaN(currentTrend)) {
			trend = (Double.isNaN(trend) ? currentTrend : 0.4 * trend + 0.6 * currentTrend);
		}
		
		double targetImpressions = currentState.getTotalTargetMetric().getImpressions();
		if (targetImpressions > 1000.0) {
			double currentDelivery = (currentImpressions - targetImpressions) / targetImpressions;
			delivery = (Double.isNaN(delivery) ? currentDelivery : 0.4 * delivery + 0.6 * currentDelivery);
		}
		
		return currentState.updateDoTables(cumAuditMetric, cumDeliveryMetric, getTarget(), getTrend(), getDelivery());
	}
}
