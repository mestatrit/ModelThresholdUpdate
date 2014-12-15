package com.sharethis.delivery.base;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Errors {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private String task;
	private List<String> ids;
	private List<String> names;
	private List<String> causes;

	public Errors() {
		ids = new ArrayList<String>();
		names = new ArrayList<String>();
		causes = new ArrayList<String>();
	}

	public void add(Errors errors) {
		for (int i = 0; i < errors.causes.size(); i++) {
			ids.add(errors.ids.get(i));
			names.add(errors.names.get(i));
			causes.add(errors.causes.get(i));
		}
	}

	public void add(String cause) {
		ids.add(null);
		names.add(null);
		causes.add(cause);
	}

	public void add(long id, String name, String cause) {
		this.add(String.format("%d", id), name, cause);
	}

	public void add(String id, String name, String cause) {
		ids.add(id);
		names.add(name);
		causes.add(cause);
	}

	public void setIds(Parameters campaignParameters) {
		String singleCampaignId = campaignParameters.get(Constants.CAMPAIGN_ID) + "."
				+ campaignParameters.get(Constants.GOAL_ID);
		String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
		for (int i = 0; i < causes.size(); i++) {
			if (ids.get(i) == null) {
				ids.set(i, singleCampaignId);
				names.set(i, campaignName);
			}
		}
	}

	public void setTask(String task) {
		this.task = task;
	}

	public void log() {
		log.error(causes.get(causes.size() - 1));
	}

	public void log(String campaignName) {
		log.error(campaignName + ": " + causes.get(causes.size() - 1));
	}

	public boolean isEmpty() {
		return causes.isEmpty();
	}

	public void updateErrorTable() throws SQLException {
		String time = DateUtils.getDatetime();
		for (int i = 0; i < causes.size(); i++) {
			String query = String.format("INSERT INTO %s (time,task,id,name,cause) VALUES ('%s','%s','%s','%s','%s');",
					Constants.DO_ERRORS, time, task, ids.get(i), names.get(i), causes.get(i));
			Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
			statement.executeUpdate(query);
		}
	}

	public static void cleanErrorsTable() throws SQLException, ParseException {
		String today = DateUtils.getToday();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("DELETE FROM %s WHERE time < '%s';", Constants.DO_ERRORS, today);
		int count = statement.executeUpdate(query);
		log.info(Constants.DO_ERRORS + " table: " + count + " rows deleted");
	}
}
