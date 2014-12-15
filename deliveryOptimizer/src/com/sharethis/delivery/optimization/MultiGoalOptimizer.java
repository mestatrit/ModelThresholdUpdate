package com.sharethis.delivery.optimization;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;


public class MultiGoalOptimizer {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private long campaignId;
	private List<Integer> goalIds;
	private Parameters globalParameters;
	private SingleGoalOptimizer worstOptimizer;
	private List<SingleGoalOptimizer> optimizers;

	public MultiGoalOptimizer(long campaignId, List<Integer> goalIds, Parameters globalParameters) throws IOException, ParseException, SQLException {
		this.campaignId = campaignId;
		this.goalIds = goalIds;
		this.globalParameters = globalParameters;
	}
	
	public void process(String pvDate) throws IOException, SQLException, ParseException {
		if (readCampaignData(pvDate)) {
			updateAndPredict();
			writeImpressionTargets();
			log.info(getReport());
		} else {
			log.error("Error reading optimizer data/parameters for campaign: " + campaignId);
		}
	}
	
	public boolean readCampaignData(String pvDate) throws IOException, ParseException, SQLException {
		optimizers = new ArrayList<SingleGoalOptimizer>();
		for (int goalId : goalIds) {
			SingleGoalOptimizer optimizer = new SingleGoalOptimizer(campaignId, goalId, globalParameters);
			if (optimizer.readCampaignData(pvDate)) {
				optimizers.add(optimizer);
			}
		}
		return (optimizers.size() > 0);
	}
	
	public void updateAndPredict() throws IOException, SQLException, ParseException {
		for (SingleGoalOptimizer optimizer : optimizers) {
			optimizer.updateCampaignPerformanceRecords();
			optimizer.predictImpressionTargets();
		}
	}
	
	public boolean writeImpressionTargets() throws SQLException, ParseException {
		double maxScore = -Double.MAX_VALUE;
		worstOptimizer = null;
		for (SingleGoalOptimizer optimizer : optimizers) {
			double score = optimizer.getKpiScore();
			if (score > maxScore) {
				maxScore = score;
				worstOptimizer = optimizer;
			}
		}
		return worstOptimizer.writeImpressionTargets();
	}
	
	public String getReport() {
		int k = worstOptimizer.getGoalId();
		StringBuilder report = new StringBuilder();
		report.append(optimizers.get(0).getReport(k));
		for (int i = 1; i < optimizers.size(); i++) {
			report.append("\n").append(optimizers.get(i).getReport(k));
		}
		return report.toString();
	}
}
