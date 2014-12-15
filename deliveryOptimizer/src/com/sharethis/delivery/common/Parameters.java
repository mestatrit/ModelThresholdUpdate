package com.sharethis.delivery.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.util.DbUtils;

public class Parameters {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
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
	
	public Parameters(String url, String user, String password, String table) throws SQLException {
		this();
		Connection connection = DbUtils.getConnection(url, user, password);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT parameter, value FROM %s;", table);
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			String parameter = result.getString("parameter").trim().toLowerCase().replaceAll(" ", "_");
			String value = result.getString("value").trim();
			put(parameter, value);
		}
		connection.close();
		connection = null;
	}
	
	public void put(String line) {
		String[] values = line.split("=", 2);
		if (values.length == 2) {
			put(values[0].trim(), values[1].trim());
//			log.info("Parameters.put: Added parameter: " + values[0] + "=" + values[1]);
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

	public void print() {
		for (String key : parameters.keySet()) {
			System.out.println(key + "=" + parameters.get(key));
		}
	}

	public int size() {
		return parameters.size();
	}
}
