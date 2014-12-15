package com.sharethis.delivery.optimization;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.common.Constants;


public class MultiGoalOptimizer {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private long campaignId;
	private List<Integer> goalIds;
	private SingleGoalOptimizer worstOptimizer;
	private List<SingleGoalOptimizer> optimizers;

	public MultiGoalOptimizer(long campaignId, List<Integer> goalIds) throws IOException, ParseException, SQLException {
		this.campaignId = campaignId;
		this.goalIds = goalIds;
	}
	
	public void process(String pvDate) throws IOException, SQLException, ParseException {
		if (readCampaignData(pvDate)) {
			clear();
			estimate();
			if (optimize()) {
				allocate();
			}
			log.info(getReport());
		} else {
			log.error("Error reading optimizer data/parameters for campaign: " + campaignId);
		}
		reportErrors();
	}
	
	private boolean readCampaignData(String pvDate) throws IOException, ParseException, SQLException {
		optimizers = new ArrayList<SingleGoalOptimizer>();
		for (int goalId : goalIds) {
			SingleGoalOptimizer optimizer = new SingleGoalOptimizer(campaignId, goalId);
			optimizers.add(optimizer);
			if (!optimizer.readCampaignData(pvDate)) {
				return false;
			}
		}
		return (optimizers.size() > 0);
	}
	
	private void clear() throws SQLException, ParseException {
		for (SingleGoalOptimizer optimizer : optimizers) {
			optimizer.clear();
		}
	}
	
	private void estimate() throws IOException, SQLException, ParseException {
		for (SingleGoalOptimizer optimizer : optimizers) {
			optimizer.estimate();
		}
	}
	
	private boolean optimize() throws SQLException, ParseException {
		double maxScore = -Double.MAX_VALUE;
		worstOptimizer = null;
		for (SingleGoalOptimizer optimizer : optimizers) {
			if (!optimizer.optimize()) {
				return false;
			}
			double score = optimizer.getKpiScore();
			if (score > maxScore) {
				maxScore = score;
				worstOptimizer = optimizer;
			}
		}
		return true;
	}
	
	private boolean allocate() throws SQLException, ParseException {
		boolean success = true;
		for (SingleGoalOptimizer optimizer : optimizers) {
			success &= optimizer.allocate();
		}
		if (success) {
			worstOptimizer.write();
			return true;
		} else {
			return false;
		}
	}
	
	private void reportErrors() throws SQLException {
		for (SingleGoalOptimizer optimizer : optimizers) {
			if (optimizer.hasErrors()) {
				Errors errors = optimizer.getErrors();
				errors.updateErrorTable();
			}
		}
	}
	
	private String getReport() {
		int goalId = (worstOptimizer != null ? worstOptimizer.getGoalId() : -1);
		StringBuilder report = new StringBuilder();
		report.append(optimizers.get(0).getReport(goalId));
		for (int i = 1; i < optimizers.size(); i++) {
			report.append("\n").append(optimizers.get(i).getReport(goalId));
		}
		return report.toString();
	}
}
