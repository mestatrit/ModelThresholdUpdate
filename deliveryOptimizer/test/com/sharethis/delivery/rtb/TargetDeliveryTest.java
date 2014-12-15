package com.sharethis.delivery.rtb;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.rtb.TargetDelivery;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class TargetDeliveryTest {

	private String urlRead, urlWrite;
	private int impressions1, impressions2;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void initialize() throws FileNotFoundException, IOException {
		try {
			PropertyConfigurator.configure("res/log4j.properties");
			Parameters propertyParameters = new Parameters("res/junit.properties");
			urlRead = propertyParameters.get(Constants.URL_JUNIT);
			urlWrite = propertyParameters.get(Constants.URL_JUNIT);
			Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
			Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
			setTable();
			TargetDelivery deliveryTargets = new TargetDelivery(urlRead, urlWrite);
			deliveryTargets.readTargets();
			deliveryTargets.writeTargets();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
	}

	private void setTable() {
		Random generator = new Random();
		impressions1 = generator.nextInt(20000);
		impressions2 = generator.nextInt(20000);
		String date = DateUtils.getDatetime();
		try {
			Connection connection = DbUtils.getConnection(urlRead, Constants.DB_USER, Constants.DB_PASSWORD);
			Statement statement = connection.createStatement();
			String query = String.format("UPDATE %s SET currentImpressionTarget = %d, nextImpressionTarget = %d, date = '%s' WHERE adGroupId = 0;", Constants.DO_IMPRESSION_TARGETS, impressions1, impressions2, date);
			statement.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testImpressionCap() throws Exception {
		String dayOfWeek = DateUtils.getDayOfWeek();
		String nextDayOfWeek = DateUtils.getDayOfWeek(1);
		Connection connection = DbUtils.getConnection(urlWrite, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(String.format("SELECT impr%s, impr%s FROM %s WHERE adGroupId = 0;", dayOfWeek, nextDayOfWeek, Constants.RTB_AD_GROUP_DELIVERY));
		int impressionsToday = 0;
		int impressionsTomorrow = 0;
		if (result.next()) {
			impressionsToday = result.getInt(1);
			impressionsTomorrow = result.getInt(2);
		}
		connection.close();
		connection = null;
		assertTrue(impressionsToday == impressions1);
		assertTrue(impressionsTomorrow == impressions2);
	}
}
