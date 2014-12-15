package com.sharethis.delivery.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	protected String[] args;
	protected Options options;
	protected CommandLine line;
	protected Parameters propertyParameters;
	private Map<Long, String> adGroupIds;
	protected String deliveryOptimizerPath;
//	private boolean readAllAdGroups;

	protected abstract int run();

	protected void initialize(String[] args) {
		DateUtils.initialize();
		this.args = args;
		options = new Options();
		options.addOption(Constants.LOG4J_PROPERTIES, true, "log4j properties file");
		options.addOption(Constants.DELIVERY_OPTIMIZER_PROPERTIES, true, "delivery optimizer properties file");
		options.addOption(Constants.DELIVERY_OPTIMIZER_PATH, true, "delivery optimizer path");
	}

	protected void parseProperties() throws DOException {
		try {
			CommandLineParser parser = new BasicParser();
			line = parser.parse(options, args);
			if (line.hasOption(Constants.LOG4J_PROPERTIES)) {
				PropertyConfigurator.configure(line.getOptionValue(Constants.LOG4J_PROPERTIES));
			}
			propertyParameters = new Parameters(line.getOptionValue(Constants.DELIVERY_OPTIMIZER_PROPERTIES));
			DbUtils.setUrls(propertyParameters);
			deliveryOptimizerPath = line.getOptionValue(Constants.DELIVERY_OPTIMIZER_PATH);
			Constants.DO_ERROR_FILE = deliveryOptimizerPath + "/" + propertyParameters.get(Constants.DO_ERROR_FILE);
		} catch (Exception e) {
			throw new DOException(e);
		}
	}

	protected void cleanErrorsTable() throws SQLException, ParseException {
		String today = DateUtils.getToday();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		
		String query = String.format("INSERT INTO %s SELECT * FROM %s WHERE time < '%s';",
				Constants.DO_ERRORS_BACKUP, Constants.DO_ERRORS, today);
		int backupCount = statement.executeUpdate(query);
		
		query = String.format("DELETE FROM %s WHERE time < '%s';", Constants.DO_ERRORS, today);
		int deleteCount = statement.executeUpdate(query);
		log.info(Constants.DO_ERRORS + " table: delete = "+ deleteCount + ", backup = " + backupCount);
	}

	protected Map<Long, String> getAdGroupIds(boolean readAllAdGroups) throws SQLException {
		return getAdGroupIds(readAllAdGroups, "");
	}
	
	protected Map<Long, String> getAdGroupIds(boolean readAllAdGroups, String userFilter) throws SQLException {
//		this.readAllAdGroups = readAllAdGroups;
		int yield = (readAllAdGroups ? -1 : 0);
		String today = DateUtils.getDatetime();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String
				.format("SELECT DISTINCT t1.adGroupId, t1.adGroupName FROM %s t1, %s t2 WHERE t2.status = 1 AND t2.campaignId = t1.campaignId AND t1.yield > %d AND t2.startDate <= '%s' AND t2.endDate >= '%s' %s ORDER BY t1.adGroupName;",
						Constants.DO_AD_GROUP_MAPPINGS, Constants.DO_CAMPAIGN_SETTINGS, yield, today, today, userFilter);
		ResultSet result = statement.executeQuery(query);
		adGroupIds = new LinkedHashMap<Long, String>();
		while (result.next()) {
			adGroupIds.put(result.getLong("adGroupId"), result.getString("adGroupName"));
		}
		return adGroupIds;
	}

	protected List<Long> getCampaignIds() throws SQLException {
		String today = DateUtils.getDatetime();
		String past  = DateUtils.getDatetime(-365);
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String
				.format("SELECT DISTINCT campaignId, startDate, endDate FROM %s WHERE status=1 AND startDate <= '%s' AND endDate > '%s' ORDER BY startDate, campaignId;",
						Constants.DO_CAMPAIGN_SETTINGS, today, past);
		ResultSet result = statement.executeQuery(query);
		List<Long> ids = new ArrayList<Long>();
		while (result.next()) {
			long endTime = DateUtils.parseTime(result, Constants.END_DATE);
			long currentTime = DateUtils.currentTime();
			if (currentTime < endTime + Constants.DAYS_UPDATE_AFTER_END_DATE * 24 * Constants.MILLISECONDS_PER_HOUR) {
				ids.add(result.getLong("campaignId"));
			}
		}

		if (ids.size() > 0) {
			StringBuilder builder = new StringBuilder();
			builder.append("Campaign Ids:");
			int n = 0;
			for (long id : ids) {
				if (n % 10 == 0) {
					builder.append("\n     ");
				}
				builder.append(id + " ");
				n++;
			}
			log.info(builder.toString());
		}

		return ids;
	}

	protected List<Integer> getGoalIds(long campaignId) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT campaignName, goalId FROM %s WHERE campaignId = %d ORDER BY goalId;",
				Constants.DO_CAMPAIGN_SETTINGS, campaignId);
		ResultSet result = statement.executeQuery(query);
		List<Integer> ids = new ArrayList<Integer>();
		String campaignName = null;
		while (result.next()) {
			campaignName = result.getString("campaignName");
			ids.add(result.getInt("goalId"));
		}

		if (ids.size() > 0) {
			StringBuilder builder = new StringBuilder();
			builder.append(campaignName).append(": Goal Ids: ");
			for (long id : ids) {
				builder.append(id + " ");
			}
			log.info("");
			log.info(builder.toString());
		}

		return ids;
	}

	protected void updateAdminDate(String stage) throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String jobName = "DeliveryOptimizer:" + stage;
		String datetime = DateUtils.getDatetime();
		String query = String.format(
				"INSERT INTO %s (name,value) VALUES ('%s','%s') ON DUPLICATE KEY UPDATE value ='%s';",
				Constants.DO_ADMIN_DATE, jobName, datetime, datetime);
		int count = statement.executeUpdate(query);
		if (count != 1 && count != 2) {
			log.error(String.format("%s inserted into %s %d times", stage, Constants.DO_ADMIN_DATE, count));
		}
	}
	protected int writeToFile(String fileName, String msg) {
		File file = new File(fileName);
		if (msg.length() == 0) {
			file.delete();
		} else {
			try {
			Writer output = new BufferedWriter(new FileWriter(file));
			output.write(msg);
			output.close();
			} catch (IOException e) {
				log.error("WriteToFile Exception:", e);
				return 1;
			}
		}
		return 0;
	}
	
	protected int writeToFile(String fileName, Exception e) {
		return writeToFile(fileName, e.toString());
	}
}
