package com.sharethis.delivery.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;

public class DbUtils {
	private static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	private static final Map<String, String> urls = new HashMap<String, String>();
	private static final Map<String, Connection> connections = new HashMap<String, Connection>();
	private static final Map<String, Statement> statements = new HashMap<String, Statement>();
	private static String user, password;

	public static Connection getConnection(String url, String user, String password) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			log.error("Database Connection Exception (DbUtils):", e);
			System.exit(1);
		} catch (ClassNotFoundException e) {
			log.error("Mysql Driver Class Exception (DbUtils):", e);
			System.exit(1);
		}
		return null;
	}

	public static void setUrls(Parameters propertyParameters) {
		urls.put(Constants.URL_DELIVERY_OPTIMIZER, propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER));
		urls.put(Constants.URL_RTB_STATISTICS, propertyParameters.get(Constants.URL_RTB_STATISTICS));
		urls.put(Constants.URL_RTB_CAMPAIGN_STATISTICS, propertyParameters.get(Constants.URL_RTB_CAMPAIGN_STATISTICS));
		urls.put(Constants.URL_RTB, propertyParameters.get(Constants.URL_RTB));
		urls.put(Constants.URL_ADPLATFORM, propertyParameters.get(Constants.URL_ADPLATFORM));
		urls.put(Constants.URL_LOOKALIKE, propertyParameters.get(Constants.URL_LOOKALIKE));
		urls.put(Constants.URL_LOOKALIKE, propertyParameters.get(Constants.URL_LOOKALIKE));
		user = propertyParameters.get(Constants.DB_USER);
		password = propertyParameters.get(Constants.DB_PASSWORD);
	}

	public static void setUrl(String urlName, String url, String usr, String pwd) {
		urls.put(urlName, url);
		user = usr;
		password = pwd;
	}
	
	public static void open(String... urlNames) throws SQLException {
		for (String urlName : urlNames) {
			if (!connections.containsKey(urlName) || connections.get(urlName) == null) {
				String url = urls.get(urlName);
				Connection connection = getConnection(url, user, password);
				Statement statement = connection.createStatement();
				connections.put(urlName, connection);
				statements.put(urlName, statement);
			}
		}
	}

	public static void close() {
		for (String urlName : connections.keySet()) {
			Connection connection = connections.get(urlName);
			Statement statement = statements.get(urlName);
			try {
				connection.close();
				statement.close();
			} catch (SQLException e) {
				log.error("Database Connection Exception (DbUtils.close) for url: " + urlName, e);
			}
			connection = null;
		}
		connections.clear();
		statements.clear();
	}

	public static Statement getStatement(String urlName) throws SQLException {
		if (!statements.containsKey(urlName)) {
			open(urlName);
		}
		return statements.get(urlName);
	}

	public static Statement getNewStatement(String urlName) throws SQLException {
		return connections.get(urlName).createStatement();
	}
}
