package com.sharethis.delivery.base;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.sharethis.adoptimization.adopt.PriceVolumeModel;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class PriceVolumeCurve {
	private static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private PriceVolumeModel priceVolumeModel;
	private int maxImpressions;
	private int deliveredImpressions, deliveredClicks;
	private double deliveredEcpm;
	private double maxCpm;
	private double lowCpm, highCpm;
	private long time;
	private boolean isValidCurve, isDefaultCurve;
	private int totalInventory;


	public PriceVolumeCurve(int totalInventory, double yield, double lowCpm, double highCpm) {
		this.isDefaultCurve = true;
		this.isValidCurve = true;
		this.maxImpressions = this.totalInventory = totalInventory;
		this.lowCpm = lowCpm;
		this.highCpm = highCpm;
		this.maxCpm = lowCpm;
	}
	
	public PriceVolumeCurve(double yield, double lowCpm, double highCpm) {
		isDefaultCurve = true;
		isValidCurve = true;
		maxImpressions = totalInventory = 1000;
		this.lowCpm = lowCpm;
		this.highCpm = highCpm;
		this.maxCpm = lowCpm;
	}
	
	public PriceVolumeCurve(Connection connection, ResultSet result, double yield, double lowCpm, double highCpm)
			throws SQLException, ParseException {
		maxImpressions = 0;
		this.lowCpm = lowCpm;
		this.highCpm = highCpm;
		isDefaultCurve = false;
		if (result.next()) {
			long adGroupId = result.getLong("adGroupId");
			String adGroupName = result.getString("adGroupName");
			deliveredEcpm = result.getDouble("ecpmDelivery");
			deliveredImpressions = result.getInt("impressionDelivery");
			deliveredClicks = result.getInt("clickDelivery");
			maxCpm = result.getDouble("maxCpm");
			time = DateUtils.parseTime(result, "updateDate");

			totalInventory = result.getInt("totalInventory");
			int averageTotalInventory = getMovingAverageTotalInventory(connection, adGroupId, totalInventory, time);

			if (averageTotalInventory >= Constants.PV_MIN_VALID_TOTAL_INVENTORY) {
				double delta = 100.0 * ((double) averageTotalInventory / (double) (totalInventory + 1) - 1.0);
				log.info(String.format("Using moving average total inventory: %,d (%,d   %4.1f%%   %s)",
						averageTotalInventory, totalInventory, delta, adGroupName));
				totalInventory = averageTotalInventory;
			}

			try {
				priceVolumeModel = new PriceVolumeModel(adGroupId);
			} catch (Exception e) {
				String msg = String.format("Error reading price-volume model for ad group id: %d", adGroupId);
				throw new SQLException(msg, e);
			}

			isValidCurve = (totalInventory >= Constants.PV_MIN_VALID_TOTAL_INVENTORY)
					&& (priceVolumeModel.getBeta1() > 0) && (priceVolumeModel.getRSquare() >= Constants.PV_MIN_R2);

			if (isValidCurve) {
				int achievableTotalInventory = (int) Math.round(0.8 * yield * totalInventory);
				maxImpressions = Math.min((int) (yield * getVolume(highCpm)), achievableTotalInventory);
			} else {
				log.warn("Invalid price-volume curve for ad group: " + adGroupName);
			}
		} else {
			isValidCurve = false;
			log.error("Price-volume curve missing");
		}
	}

	public PriceVolumeCurve(double deliveredEcpm, int deliveredImpressions, int deliveredClicks, double maxCpm, long time) {
		this.deliveredEcpm = deliveredEcpm;
		this.deliveredImpressions = deliveredImpressions;
		this.deliveredClicks = deliveredClicks;
		this.maxCpm = maxCpm;
		this.maxImpressions = 0;
		this.time = time;
		this.isValidCurve = false;
		this.isDefaultCurve = false;
	}

	private int getAverageTotalInventory(Connection connection, long adGroupId) throws SQLException {
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT AVG(t.totalInventory) as inventory FROM (SELECT totalInventory FROM %s WHERE adGroupId = %d AND totalInventory >= %d ORDER BY updateDate DESC LIMIT 4) t",
						Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupId, Constants.PV_MIN_VALID_TOTAL_INVENTORY);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			int averageTotalInventory = result.getInt("inventory");
			return averageTotalInventory;
		} else {
			return 0;
		}
	}

	private int getMovingAverageTotalInventory(Connection connection, long adGroupId, int currentTotalInventory, long time) throws SQLException {
		String startDate = DateUtils.getDatetime(time - 7L * 24L * Constants.MILLISECONDS_PER_HOUR);
		String endDate = DateUtils.getDatetime(time - Constants.MILLISECONDS_PER_HOUR);
		Statement statement = connection.createStatement();
		String query = String
				.format("SELECT totalInventory FROM %s WHERE adGroupId = %d AND totalInventory >= %d AND updateDate >= '%s' AND updateDate < '%s' ORDER BY updateDate ASC",
						Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupId, Constants.PV_MIN_VALID_TOTAL_INVENTORY,
						startDate, endDate);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			double a = Constants.PV_MOVING_AVERAGE_SMOOTHING_FACTOR;
			double averageTotalInventory = (double) result.getInt("totalInventory");
			while (result.next()) {
				averageTotalInventory = a * (double) result.getInt("totalInventory") + (1.0 - a)
						* averageTotalInventory;
			}
			averageTotalInventory = a * currentTotalInventory + (1.0 - a) * averageTotalInventory;
			return (int) Math.round(averageTotalInventory);
		} else {
			return 0;
		}
	}

	public double getVolume(double price) {
		if (isDefaultCurve) {
			return totalInventory;
		} else if (isValidCurve) {
			return totalInventory * priceVolumeModel.getWinRate(price);
		} else {
			return 0.0;
		}

	}

	public double getPrice(int volume) {
		if (isDefaultCurve) {
			return highCpm;
		} else if (isValidCurve) {
			double maxVolume = Constants.PV_CPM_IMPRESSION_FACTOR * (double) volume;
			if (maxVolume >= totalInventory) {
				return highCpm;
			}
			double winRate = maxVolume / (double) totalInventory;
			double price = priceVolumeModel.getWinningPrice(winRate);
			return Math.max(Math.min(0.01 * Math.round(100.0 * price), highCpm), lowCpm);
		} else {
			return lowCpm;
		}
	}

	public double getPredictedEcpm(int impressionTarget) {
		if (isDefaultCurve) {
			return lowCpm;
		} else if (isValidCurve) {
			double dV = 1000.0;
			int n = (int) Math.round(impressionTarget / dV);
			if (n == 0) {
				n = 1;
				dV = (double) impressionTarget;
				
			} else {
				dV = (double) impressionTarget / (double) n;
			}
			double ecpm = 0.0;
			double dx = dV / (double) totalInventory;
			for (int i = 0; i < n; i++) {
				double winRate = (i + 0.5) * dx;
				ecpm += priceVolumeModel.getWinningPrice(winRate);
			}
			ecpm *= (dV / (double) impressionTarget);
			return ecpm;
		} else {
			return lowCpm;
		}
	}
	
	public int getMaxImpressions() {
		return maxImpressions;
	}

	public int getDeliveredImpressions() {
		return deliveredImpressions;
	}

	public int getDeliveredClicks() {
		return deliveredClicks;
	}

	public double getDeliveredEcpm() {
		return deliveredEcpm;
	}

	public double getMaxCpm(int volume) {
		return (isValidCurve ? getPrice(volume) : maxCpm);
	}

	public long getTime() {
		return time;
	}

	public boolean isValid() {
		return isValidCurve;
	}

	public static void backupPriceVolumeCurves() throws SQLException {
		String yesterday = DateUtils.getDate("yyyy-MM-dd 00:00:00", -1);
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String
				.format("INSERT INTO %s"
						+ " (adGroupId, adGroupName, ecpmDelivery, impressionDelivery, clickDelivery, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate)"
						+ " SELECT t2.adGroupId, t2.adGroupName, t2.ecpmDelivery, t2.impressionDelivery, t2.clickDelivery, t2.totalInventory, t2.maxCpm, t2.impressionMaxCpm, t2.beta0, t2.beta1, t2.r_square, t2.updateDate"
						+ " FROM %s t2 LEFT JOIN %s t1 ON t1.adGroupId = t2.adGroupId AND t1.updateDate = t2.updateDate WHERE t1.adGroupId IS NULL AND t1.updateDate IS NULL AND t2.updateDate >= '%s';",
						Constants.DO_PRICE_VOLUME_CURVES_BACKUP, Constants.DO_PRICE_VOLUME_CURVES,
						Constants.DO_PRICE_VOLUME_CURVES_BACKUP, yesterday);
		int backupRecords = statement.executeUpdate(query);
		log.info(String.format("PV records added to %s table: %d\n", Constants.DO_PRICE_VOLUME_CURVES_BACKUP,
				backupRecords));

		connection.close();
		connection = null;
	}
}
