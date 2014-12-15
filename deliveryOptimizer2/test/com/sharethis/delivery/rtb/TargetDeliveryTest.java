package com.sharethis.delivery.rtb;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
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

	private int impressions1, impressions2;
	private double maxCpm;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void initialize() throws FileNotFoundException, IOException {
		try {
			PropertyConfigurator.configure("res/log4j.properties");
			Parameters propertyParameters = new Parameters("res/junit.properties");
			String url = propertyParameters.get(Constants.URL_JUNIT);
			String user = propertyParameters.get(Constants.DB_USER);
			String password = propertyParameters.get(Constants.DB_PASSWORD);
			DbUtils.setUrl(Constants.URL_JUNIT, url, user, password);
			setTable();
			TargetDelivery deliveryTargets = new TargetDelivery(Constants.URL_JUNIT, Constants.URL_JUNIT, Constants.URL_JUNIT);
			deliveryTargets.readTargets();
			deliveryTargets.writeTargets();
			deliveryTargets.writeBids();
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
		maxCpm = 0.01 * generator.nextInt(10000);
		String date = DateUtils.getDatetime();
		try {
			Statement statement = DbUtils.getStatement(Constants.URL_JUNIT);
			String query = String.format("UPDATE %s SET currentImpressionTarget = %d, nextImpressionTarget = %d, currentMaxCpm=%.2f, date = '%s' WHERE adGroupId = 0;", Constants.DO_IMPRESSION_TARGETS, impressions1, impressions2, maxCpm, date);
			statement.executeUpdate(query);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testImpressionCap() throws Exception {
		String dayOfWeek = DateUtils.getDayOfWeek();
		String nextDayOfWeek = DateUtils.getDayOfWeek(1);
		Statement statement = DbUtils.getStatement(Constants.URL_JUNIT);
		ResultSet result = statement.executeQuery(String.format("SELECT impr%s, impr%s FROM %s WHERE adGroupId = 0;", dayOfWeek, nextDayOfWeek, Constants.RTB_AD_GROUP_DELIVERY));
		int impressionsToday = 0;
		int impressionsTomorrow = 0;
		if (result.next()) {
			impressionsToday = result.getInt(1);
			impressionsTomorrow = result.getInt(2);
		}
		System.out.println(impressionsToday + "  " + impressions1);
		assertTrue(impressionsToday == impressions1);
		assertTrue(impressionsTomorrow == impressions2);
	}
	
	@Test
	public void testMaxCpm() throws Exception {
		Statement statement = DbUtils.getStatement(Constants.URL_JUNIT);
		ResultSet result = statement.executeQuery(String.format("SELECT maxPrice FROM %s WHERE networkAdgId = 0;", Constants.ADPLATFORM_DB_AD_GROUP));
		long maxPrice = 0;
		if (result.next()) {
			maxPrice = result.getLong(1);
		}
		System.out.println(1.0e-6 * maxPrice + "  " + maxCpm);
		assertTrue(Math.round(1.0e-4 * maxPrice) == Math.round(100.0 * maxCpm));
	}
}
