package com.sharethis.delivery.job;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public abstract class Runner {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	protected String[] args;
	protected Options options;
	protected CommandLine line;
	protected Parameters propertyParameters;
	private List<Long> adGroupIds = null;
	private List<String> adGroupNames = null;
	private boolean readAllAdGroups;

	protected abstract int run();

	protected void initialize(String[] args) {
		DateUtils.initialize();
		this.args = args;
		options = new Options();
		options.addOption(Constants.LOG4J_PROPERTIES, true, "log4j properties file");
		options.addOption(Constants.DELIVERY_OPTIMIZER_PROPERTIES, true, "delivery optimizer properties file");
	}

	protected void parseProperties() throws DOException {
		try {
			CommandLineParser parser = new BasicParser();
			line = parser.parse(options, args);
			if (line.hasOption(Constants.LOG4J_PROPERTIES)) {
				PropertyConfigurator.configure(line.getOptionValue(Constants.LOG4J_PROPERTIES));
			}
			propertyParameters = new Parameters(line.getOptionValue(Constants.DELIVERY_OPTIMIZER_PROPERTIES));
		} catch (Exception e) {
			throw new DOException(e);
		}
	}

	private void getAdGroupIdsAndNames(boolean readAllAdGroups) throws SQLException {
		this.readAllAdGroups = readAllAdGroups;
		int yield = (readAllAdGroups ? -1 : 0);
		String today = DateUtils.getDatetime();
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT DISTINCT t1.adGroupId, t1.adGroupName FROM rtbDelivery.adGroupMappings t1, rtbDelivery.campaignSettings t2 WHERE t2.status = 1 AND t2.campaignId = t1.campaignId AND t1.yield > %d AND t2.startDate <= '%s' AND t2.endDate >= '%s' ORDER BY t1.adGroupName;",
						yield, today, today);
		ResultSet result = statement.executeQuery(query);
		adGroupIds = new ArrayList<Long>();
		adGroupNames = new ArrayList<String>();
		while (result.next()) {
			adGroupIds.add(result.getLong("adGroupId"));
			adGroupNames.add(result.getString("adGroupName"));
		}
		connection.close();
		connection = null;
	}

	protected List<Long> getAdGroupIds(boolean readAllAdGroups) throws SQLException {
		if (adGroupIds == null || this.readAllAdGroups != readAllAdGroups) {
			getAdGroupIdsAndNames(readAllAdGroups);
		}
		return adGroupIds;
	}

	protected List<String> getAdGroupNames(boolean readAllAdGroups) throws SQLException {
		if (adGroupNames == null || this.readAllAdGroups != readAllAdGroups) {
			getAdGroupIdsAndNames(readAllAdGroups);
		}
		return adGroupNames;
	}

	protected List<Long> getCampaignIds() throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT DISTINCT campaignId, startDate, endDate FROM %s WHERE status=1 ORDER BY startDate, campaignId;",
						Constants.DO_CAMPAIGN_SETTINGS);
		ResultSet result = statement.executeQuery(query);
		List<Long> ids = new ArrayList<Long>();
		while (result.next()) {
			long endTime = DateUtils.parseTime(result, Constants.END_DATE);
			long currentTime = DateUtils.currentTime();
			if (currentTime < endTime + Constants.DAYS_UPDATE_AFTER_END_DATE * 24 * Constants.MILLISECONDS_PER_HOUR) {
				ids.add(result.getLong("campaignId"));
			}
		}
		connection.close();
		connection = null;

		if (ids.size() > 0) {
			StringBuilder builder = new StringBuilder();
			builder.append("Campaign Ids: ");
			for (long id : ids) {
				builder.append(id + " ");
			}
			log.info(builder.toString());
		}

		return ids;
	}

	protected List<Integer> getGoalIds(long campaignId) throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT goalId FROM %s WHERE campaignId = %d ORDER BY goalId;",
				Constants.DO_CAMPAIGN_SETTINGS, campaignId);
		ResultSet result = statement.executeQuery(query);
		List<Integer> ids = new ArrayList<Integer>();
		while (result.next()) {
			ids.add(result.getInt("goalId"));
		}
		connection.close();
		connection = null;

		if (ids.size() > 0) {
			StringBuilder builder = new StringBuilder();
			builder.append("Goal Ids: ");
			for (long id : ids) {
				builder.append(id + " ");
			}
			log.info("");
			log.info(builder.toString());
		}

		return ids;
	}

	protected void updateAdminDate(String stage) throws SQLException {
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String jobName = "DeliveryOptimizer:" + stage;
		String datetime = DateUtils.getDatetime();
		String query = String.format(
				"INSERT INTO %s (name,value) VALUES ('%s','%s') ON DUPLICATE KEY UPDATE value ='%s';",
				Constants.DO_ADMIN_DATE, jobName, datetime, datetime);
		int count = statement.executeUpdate(query);
		if (count != 1 && count != 2) {
			log.error(String.format("%s inserted into %s %d times", stage, Constants.DO_ADMIN_DATE, count));
		}
		connection.close();
		connection = null;
	}
}
