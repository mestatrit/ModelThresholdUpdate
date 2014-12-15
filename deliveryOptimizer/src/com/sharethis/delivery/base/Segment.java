package com.sharethis.delivery.base;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;
import com.sharethis.delivery.util.Sort;

public class Segment {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private List<AdGroup> adGroups;
	private long time;
	
	public Segment() {
		adGroups = new ArrayList<AdGroup>();
	}
	
	public boolean add(long adGroupId, String adGroupName, int priority, double yield, double lowCpm, double highCpm) {
		for (AdGroup adGroup : adGroups) {
			if (adGroup.equals(adGroupId)) {
				return false;
			}
		}
		adGroups.add(new AdGroup(adGroupId, adGroupName, priority, yield, lowCpm, highCpm));
		return true;
	}

	public boolean readAdGroups(ResultSet result) throws SQLException, ParseException {
		while (result.next()) {
			String adGroupName = result.getString("adGroupName");
			long adGroupId = result.getLong("adGroupId");
			int priority = result.getInt("priority");
			double yield = result.getDouble("yield");
			double lowCpm = result.getDouble("lowCpm");
			double highCpm = result.getDouble("highCpm");
			if (!this.add(adGroupId, adGroupName, priority, yield, lowCpm, highCpm)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean addPriceVolumeCurve(String adxAdGroupName, int deliveredImpressions, int deliveredClicks, double deliveredEcpm, double maxCpm, long time) {
		for (AdGroup adGroup : adGroups) {
			if (adGroup.equals(adxAdGroupName)) {
				adGroup.setCurve(new PriceVolumeCurve(deliveredEcpm, deliveredImpressions, deliveredClicks, maxCpm, time));
				return true;
			}
		}
		return false;
	}
	
	public boolean addPriceVolumeCurves(Map<Long, PriceVolumeCurve> priceVolumeCurves) {
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			if (priceVolumeCurves.containsKey(adGroupId)) {
				PriceVolumeCurve priceVolumeCurve = priceVolumeCurves.get(adGroupId);
				long pvTime = priceVolumeCurve.getTime();
				if (time <= pvTime && pvTime < time + 24L * Constants.MILLISECONDS_PER_HOUR) {
					adGroup.setCurve(priceVolumeCurve);
					log.info(String.format("Impressions: %,d, Clicks: %d, eCPM: %5.2f, Ad group: %s", priceVolumeCurve.getDeliveredImpressions(), priceVolumeCurve.getDeliveredClicks(), priceVolumeCurve.getDeliveredEcpm(), adGroup.getName()));

				} else {
					log.error(String.format("AdWords time (%s) and segment time (%s) do not match", DateUtils.getDatetime(pvTime), DateUtils.getDatetime(time)));
					return false;
				}
			} else {
				log.error(String.format("AdGroupId = %d missing from AdWords stats set", adGroupId));
				return false;
			}
		}
		return true;
	}
	
	public boolean readPriceVolumeCurves(long startTime, long endTime) throws SQLException, ParseException {
		String start = DateUtils.getDatetime(startTime);
		String end = DateUtils.getDatetime(endTime);
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			String query = "SELECT adGroupId, adGroupName, ecpmDelivery, impressionDelivery, clickDelivery, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES
							+ " WHERE adGroupId = " + adGroupId + " AND updateDate >= '" + start + "' AND updateDate < '" + end + "' ORDER BY updateDate DESC";
			ResultSet result = statement.executeQuery(query);
			PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(connection, result, adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());
			if (!priceVolumeCurve.isValid() || priceVolumeCurve.getTime() < startTime || priceVolumeCurve.getTime() >= endTime) {
				log.error(String.format("Error in %s for adGroupId = %d: price-volume curve missing or has invalid updateDate", Constants.DO_PRICE_VOLUME_CURVES, adGroupId));
				query = "SELECT adGroupId, adGroupName, ecpmDelivery, impressionDelivery, clickDelivery, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES_BACKUP
						+ " WHERE adGroupId = " + adGroupId + " AND totalInventory >= " + Constants.PV_MIN_VALID_TOTAL_INVENTORY + " ORDER BY updateDate DESC LIMIT 1";
				result = statement.executeQuery(query);
				priceVolumeCurve = new PriceVolumeCurve(connection, result, adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());
				if (!priceVolumeCurve.isValid()) {
					log.error(String.format("Error in %s for adGroupId = %d: no valid price-volume curve available", Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupId));
					priceVolumeCurve = new PriceVolumeCurve(0, adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());;
				} else {
					log.info(String.format("Using date from %s for adGroupId = %d: %s", Constants.DO_PRICE_VOLUME_CURVES_BACKUP, adGroupId, DateUtils.getDatetime(priceVolumeCurve.getTime())));
				}
			}
			adGroup.setCurve(priceVolumeCurve);
		}
		connection.close();
		connection = null;
		return true;
	}
	
	public boolean setDefaultPriceVolumeCurves() throws SQLException, ParseException {
		for (AdGroup adGroup : adGroups) {
			PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());
			adGroup.setCurve(priceVolumeCurve);
		}
		return true;
	}
	
	public boolean readDeliveredImpressions(long startTime, long endTime) throws SQLException, ParseException {
		String start = DateUtils.getDatetime(startTime);
		String end = DateUtils.getDatetime(endTime);
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		for (AdGroup adGroup : adGroups) {
			long adGroupId = adGroup.getId();
			String query = "SELECT adGroupId, adGroupName, ecpmDelivery, impressionDelivery, clickDelivery, totalInventory, maxCpm, impressionMaxCpm, beta0, beta1, r_square, updateDate FROM " + Constants.DO_PRICE_VOLUME_CURVES
							+ " WHERE adGroupId = " + adGroupId + " AND updateDate >= '" + start + "' AND updateDate < '" + end + "' ORDER BY updateDate DESC";
			ResultSet result = statement.executeQuery(query);
			PriceVolumeCurve priceVolumeCurve = new PriceVolumeCurve(connection, result, adGroup.getYield(), adGroup.getLowCpm(), adGroup.getHighCpm());
			if (!priceVolumeCurve.isValid() || priceVolumeCurve.getTime() < startTime || priceVolumeCurve.getTime() >= endTime) {
				log.error(String.format("Error in %s for adGroupId = %d: price-volume curve missing or has invalid updateDate", Constants.DO_PRICE_VOLUME_CURVES, adGroupId));
				return false;
			}
			adGroup.setCurve(priceVolumeCurve);
		}
		connection.close();
		connection = null;
		return true;
	}
	
	public boolean hasAdGroup(String adGroupName) {
		for (AdGroup adGroup : adGroups) {
			if (adGroup.equals(adGroupName)) {
				return true;
			}
		}
		return false;
	}
	
	public List<AdGroup> getAdGroups() {
		return adGroups;
	}
	
	public List<Long> getAdGroupIds() {
		List<Long> adGroupIds = new ArrayList<Long>();
		for (AdGroup adGroup : adGroups) {
			adGroupIds.add(adGroup.getId());
		}
		return adGroupIds;
	}
	
	public int size() {
		return adGroups.size();
	}

	public int getMaxImpressions() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getMaxImpressions();
		}
		return imps;
	}
	
	public int getDeliveredImpressions() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getDeliveredImpressions();
		}
		return imps;
	}
	
	public double getDeliveredEcpm() {
		double ecpm = 0.0;
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			ecpm += adGroup.getDeliveredEcpm() * adGroup.getDeliveredImpressions();
			imps += adGroup.getDeliveredImpressions();
		}
		return (imps == 0 ? 0.0 : ecpm / (double) imps);
	}

	public double getPredictedEcpm(double impressionTarget) {
		int[] targets = this.getImpressionTargets((int)impressionTarget);
		double ecpm = 0.0;
		int imps = 0;
		for (int i = 0; i < adGroups.size(); i++) {
			AdGroup adGroup = adGroups.get(i);
			ecpm += adGroup.getPredictedEcpm(targets[i]) * targets[i];
			imps += targets[i];
		}
		return (imps == 0 ? 0.0 : ecpm / (double) imps);
	}
	
	public double getTotalMaxCpm() {
		double maxCpm = 0.0;
		for (AdGroup adGroup : adGroups) {
			maxCpm = Math.max(maxCpm, adGroup.getMaxCpm());
		}
		return maxCpm;
	}
	
	public int getTotalImpressionTarget() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getImpressionTarget();
		}
		return imps;
	}
	
	public int getTotalNextImpressionTarget() {
		int imps = 0;
		for (AdGroup adGroup : adGroups) {
			imps += adGroup.getNextImpressionTarget();
		}
		return imps;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public long getTime() {
		return time;
	}
	
	public void setImpressionTargets(int impressionTarget, int nextImpressionTarget) {
		int[] targets = this.getImpressionTargets(impressionTarget);
		int[] nextTargets = this.getImpressionTargets(nextImpressionTarget);
		for (int i = 0; i < adGroups.size(); i++) {
			adGroups.get(i).setImpressionTargets(targets[i], nextTargets[i]);
		}
	}
	
	private int[] getImpressionTargets(int totalImpressions) {
		int n = adGroups.size();
		int[] targets = new int[n];
		
		if (totalImpressions == 0) {
			return targets;
		}
		
		if (n == 1) {
			targets[0] = totalImpressions;
			return targets;
		}
		
		int[] maxImp = new int[n];
		double imps = 0.0;
		for (int i = 0; i < n; i++) {
			maxImp[i] = adGroups.get(i).getMaxImpressions();
			imps += (double) maxImp[i];
		}
		
		double totalImps = (double) totalImpressions;
		if (totalImps >= imps) {
			for (int i = 0; i < n; i++) {
				targets[i] = (int) Math.round(totalImps * (double) maxImp[i] / imps);
			}
		} else {
			int[] index = new int[n];
			int cumPriority = 0;
			for (int i = 0; i < n; i++) {
				int priority = adGroups.get(i).getPriority();
				cumPriority += priority;
				index[priority] = i;
			}
			if (cumPriority > 0) {
				for (int j = 0; j < n; j++) {
					int i = index[j];
					if (maxImp[i] >= totalImpressions) {
						targets[i] = totalImpressions;
						break;
					} else {
						targets[i] = maxImp[i];
						totalImpressions -= maxImp[i];
					}
				}
			} else {
				Arrays.sort(maxImp);
				Map<Integer, Integer> capacity = new HashMap<Integer, Integer>();
				for (int i = 0; i < n; i++) {
					capacity.put(i, maxImp[i]);
				}
				List<Integer> order = Sort.byValue(capacity, "asc");
				int m = 0;
				int i = 0;
				while (m < n) {
					int impsPerAdGroup = totalImpressions / (n - m);
					i = order.get(m);
					targets[i] = Math.min(impsPerAdGroup, maxImp[i]);
					totalImpressions -= targets[i];
					m++;
				}
				if (totalImpressions > 0) {
					targets[i] += totalImpressions;
				}
			}
		}
		return targets;
	}
}
