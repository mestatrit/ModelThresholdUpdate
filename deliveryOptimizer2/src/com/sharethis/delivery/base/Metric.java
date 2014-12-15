package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;

public class Metric {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	protected double impressions;
	protected double clicks;
	protected double conversions;
	protected double cost;
	protected long time;
	
	public Metric() {
		this(0.0, 0.0, 0.0, 0.0);
	}

	public Metric(double impressions, double clicks, double conversions, double cost) {
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.cost = cost;
		this.time = 0L;
	}

	public Metric(double impressions) {
		this(impressions, 0.0, 0.0, 0.0);
	}

	public Metric (Metric m) {
		impressions = m.impressions;
		clicks = m.clicks;
		conversions = m.conversions;
		cost = m.cost;
		time = m.time;
	}

	public static Metric parseDeliveryAdGroupMetric(ResultSet result, double clickAttribution) throws SQLException {
		double impressions = (double) result.getInt("impressions");
		double costCountCorrection = (double) result.getInt("costCountCorrection");
		double clicks = (double) result.getInt("clicks");
		double cost = result.getDouble("cost");
		if (impressions > costCountCorrection) {
			cost *= impressions / (impressions - costCountCorrection);
		} else {
			cost = 0.0;
		}
		return new Metric(impressions, clickAttribution * clicks, 0.0, cost);
	}
	
	public void add(Metric m) {
		impressions += m.impressions;
		clicks += m.clicks;
		conversions += m.conversions;
		cost += m.cost;
	}
	
	public static Metric substract(Metric m1, Metric m2, double factor) {
		double imps = m1.impressions - factor * m2.impressions;
		double clks = m1.clicks - factor * m2.clicks;
 		return new Metric(imps, clks, m1.conversions - factor * m2.conversions, m1.cost - factor * m2.cost);
	}
	
	public static Metric add(Metric[] m1, Metric m2) {
		Metric metric = new Metric(m2);
		for (Metric m : m1) {
			metric.add(m);
		}
		return metric;
	}
	
	public void setCost(double cost) {
		this.cost = cost;
	}
	
	public void setTime(long time) {
		this.time = time;
	}
	
	public long getTime() {
		return time;
	}
		
	public double getImpressions() {
		return impressions;
	}
	
	public double getClicks() {
		return clicks;
	}
	
	public double getConversions() {
		return conversions;
	}

	public double getCost() {
		return cost;
	}
	
	public double getClickRatio() {
		return (impressions == 0 ? 0.0 : clicks / impressions);
	}
	
	public double getConversionRatio() {
		return (impressions == 0 ? 0.0 : conversions / impressions);
	}
		
	public double getEcpm() {
		return (impressions == 0 ? 0.0 : 1000.0 * cost / impressions);
	}
	
	public double getEcpa() {
		return (conversions == 0 ? Double.NaN : cost / conversions);
	}
	
	public double getEcpc() {
		return (clicks == 0 ? Double.NaN : cost / clicks);
	}
	
	public double getMargin(Parameters campaignParameters) {
		double cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
		double targetImpressions = campaignParameters.getDouble(Constants.TARGET_IMPRESSIONS, 0.0);
		double marginImpressions = Math.min(impressions, targetImpressions);
		return 100.0 * (impressions == 0 ? 1.0: 1.0 - cost / (0.001 * cpm * marginImpressions));
	}
}
