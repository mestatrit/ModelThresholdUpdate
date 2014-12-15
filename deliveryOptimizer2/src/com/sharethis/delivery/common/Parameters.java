package com.sharethis.delivery.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.StrategyType;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Parameters {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private Map<String, String> parameters;

	public Parameters() {
		parameters = new HashMap<String, String>();
	}

	public Parameters(String fname) throws IOException {
		this();
		BufferedReader reader = new BufferedReader(new FileReader(fname));
		String line = null;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.length() > 0 && line.charAt(0) != '#') {
				put(line);
			}
		}
		reader.close();
	}
	
	public void read(String url, String table) throws SQLException {
		parameters.clear();
		Statement statement = DbUtils.getStatement(url);
		String query = String.format("SELECT parameter, value FROM %s;", table);
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			String parameter = result.getString("parameter").trim().toLowerCase().replaceAll(" ", "_");
			String value = result.getString("value").trim();
			put(parameter, value);
		}
	}
	
	public void put(String line) {
		String[] values = line.split("=", 2);
		if (values.length == 2) {
			put(values[0].trim(), values[1].trim());
		} else {
			log.error("Parameters.put: Cannot parse parameter: " + line);
		}
	}

	public void put(String key, String value) {
		parameters.put(key, value);
	}

	public String get(String key) {
		return parameters.get(key);
	}

	public String get(String key, String defaultValue) {
		if (parameters.containsKey(key)) {
			return parameters.get(key);
		} else {
			log.warn("Parameter not defined: set to default value: " + key + "=" + defaultValue);
			return defaultValue;
		}
	}

	public int getInt(String key, int defaultValue) {
		if (parameters.containsKey(key)) {
			return Integer.parseInt(parameters.get(key));
		} else {
			log.warn("Parameter not defined: set to default value: " + key + "=" + defaultValue);
			return defaultValue;
		}
	}

	public long getLong(String key, long defaultValue) {
		if (parameters.containsKey(key)) {
			return Long.parseLong(parameters.get(key));
		} else {
			log.warn("Parameter not defined: set to default value: " + key + "=" + defaultValue);
			return defaultValue;
		}
	}

	public double getDouble(String key, double defaultValue) {
		if (parameters.containsKey(key)) {
			return Double.parseDouble(parameters.get(key));
		} else {
			log.warn("Parameter not defined: set to default value: " + key + "=" + defaultValue);
			return defaultValue;
		}
	}

	public boolean getBoolean(String key, boolean defaultValue) {
		if (parameters.containsKey(key)) {
			return Boolean.parseBoolean(parameters.get(key));
		} else {
			log.warn("Parameter not defined: set to default value: " + key + "=" + defaultValue);
			return defaultValue;
		}
	}
	
	public StrategyType getStrategy() {
		String strStrategy = this.get(Constants.STRATEGY, (new StrategyType()).getType());
		StrategyType strategy = StrategyType.parseStrategy(strStrategy);
		double holidayDeliveryFactor = this.getDouble(Constants.HOLIDAY_DELIVERY_FACTOR, 1.0);
		strategy.setHolidayDeliveryFactor(holidayDeliveryFactor);
		return strategy;
	}
	
	public double getKpi() {
		return getDouble(Constants.KPI, 0.0);
	}
	
	public long getDeliveryIntervalEndTime(long startTime) {
		double deliveryInterval = this.getDouble(Constants.DELIVERY_INTERVAL, 0.0);
		double dayLength = DateUtils.getHoursPerDay(startTime);
		double numberOfIntervals = Math.round(dayLength / deliveryInterval);
		deliveryInterval = dayLength / numberOfIntervals;
		long endTime = startTime + (long) Math.round(deliveryInterval * Constants.MILLISECONDS_PER_HOUR);
		return endTime;
	}
	
	public void printLog() {
		for (String key : parameters.keySet()) {
			if (key.equalsIgnoreCase(Constants.DB_PASSWORD)) {
				log.info(key + "= --");
			} else {
				log.info(key + "=" + parameters.get(key));
			}
		}
	}

	public int size() {
		return parameters.size();
	}
}
