package com.sharethis.delivery.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;

public class DbUtils {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);

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
}
