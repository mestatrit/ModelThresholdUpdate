package com.sharethis.adoptimization.adopt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class JdbcOperations {
	private static final Logger sLogger = Logger.getLogger(JdbcOperations.class);
	public static final String URL_KEY = "jdbc.url";
	public static final String DRIVER_KEY = "jdbc.driver";
	public static final String LOGIN_KEY = "db.user";
	public static final String PASSWORD_KEY = "db.password";
	
	protected String url;
	protected String driver;
	protected String login;
	protected String password;
	
	public JdbcOperations(Configuration conf, String instanceName){
		String postFix = "";
		if(StringUtils.isNotBlank(instanceName) && !"default".equals(instanceName)){
			postFix ="."+instanceName;
		}
		url = conf.get(URL_KEY + postFix);
		driver = conf.get(DRIVER_KEY + postFix);
		login = conf.get(LOGIN_KEY + postFix);
		password = conf.get(PASSWORD_KEY + postFix);
		sLogger.info(toString());
	}
	
	public JdbcOperations(){
		url = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery2?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
		driver = "com.mysql.jdbc.Driver";
		login = "sharethis";
		password = "sharethis";		
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @return the driver
	 */
	public String getDriver() {
		return driver;
	}

	/**
	 * @return the login
	 */
	public String getLogin() {
		return login;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	
	public Connection getConnection() throws SQLException{
		try{
			Class.forName(driver);
		}catch(ClassNotFoundException e){
			throw new RuntimeException("Unable to load class: "+driver,e);
		}
		return DriverManager.getConnection(url, login, password);
	}
	
	public String toString(){
		Object[] values = {
			"Url: " +url, "Login: " + login,"Pass: " +password, "Driver: " + driver
		};
		return StringUtils.join(values, ",");
	}	
}
