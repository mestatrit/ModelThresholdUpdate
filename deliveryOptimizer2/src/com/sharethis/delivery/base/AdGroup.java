package com.sharethis.delivery.base;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class AdGroup {
	private long id;
	private String name;
	private int priority;
	private double yield;
	private double lowCpm, highCpm;
	private PriceVolumeCurve priceVolumeCurve;
	private Metric targetMetric, nextTargetMetric;
	private Metric deliveryMetric;
	private Metric auditMetric;
	private String comment;
	
	public AdGroup(long id, String name, int priority, double yield, double lowCpm, double highCpm) {
		this.id = id;
		this.name = name;
		this.priority = priority;
		this.yield = yield;
		this.lowCpm = lowCpm;
		this.highCpm = highCpm;
	}
	
	public static AdGroup parseAdGroup(ResultSet result) throws SQLException {
		long adGroupId = result.getLong("adGroupId");
		String adGroupName = result.getString("adGroupName");
		int priority = result.getInt("priority");
		double yield = result.getDouble("yield");
		double lowCpm = result.getDouble("lowCpm");
		double highCpm = result.getDouble("highCpm");
		return new AdGroup(adGroupId, adGroupName, priority, yield, lowCpm, highCpm);
	}
	
	
	public boolean readCurrentDelivery(long time) throws SQLException {
		String date1 = DateUtils.getDatetimeUtc(time);
		String date2 = DateUtils.getDatetimeUtc(time + 24L * Constants.MILLISECONDS_PER_HOUR - 10L);
		Statement statement = DbUtils.getStatement(Constants.URL_RTB_STATISTICS);
		String query = String
				.format("SELECT delivery, createDate FROM %s WHERE adGroupId = %d AND createDate > '%s' AND createDate < '%s' ORDER BY createDate DESC LIMIT 1;",
						Constants.RTB_STATISTICS_DB_DELIVERY_REPORT, id, date1, date2);
		ResultSet result = statement.executeQuery(query);
		if (result.next()) {
			long createTime = DateUtils.parseDateTime(result, "createDate");
			deliveryMetric = new Metric(result.getInt("delivery"));
			return (createTime > DateUtils.currentTime() - 1L * Constants.MILLISECONDS_PER_HOUR);
		} else {
			deliveryMetric = new Metric();
			return false;
		}
	}
	
	public void setPriceVolumeCurve(PriceVolumeCurve priceVolumeCurve) {
		this.priceVolumeCurve = priceVolumeCurve;
	}
	
	public void setDeliveryMetric(Metric deliveryMetric) {
		this.deliveryMetric = deliveryMetric;
	}
	
	public void setTargetMetric(Metric targetMetric) {
		this.targetMetric = targetMetric;
	}
	
	public void setNextTargetMetric(Metric nextTargetMetric) {
		this.nextTargetMetric = nextTargetMetric;
	}
	
	public void setAuditMetric(Metric auditMetric) {
		this.auditMetric = auditMetric;
	}
	
	public void resetImpressionTarget() {
		targetMetric = new Metric();
	}
	public long getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public int getPriority() {
		return priority;
	}
	
	public double getYield() {
		return yield;
	}
	
	public double getHighCpm() {
		return highCpm;
	}
	
	public double getLowCpm() {
		return lowCpm;
	}

	public Metric getDeliveryMetric() {
		return deliveryMetric;
	}
	
	public Metric getTargetMetric() {
		return targetMetric;
	}
	
	public Metric getNextTargetMetric() {
		return nextTargetMetric;
	}
	
	public Metric getAuditMetric() {
		return auditMetric;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}

	public PriceVolumeCurve getPriceVolumeCurve() {
	return priceVolumeCurve;
	}

	public double getMaxCpm() {
		return priceVolumeCurve.getMaxCpm(targetMetric);
	}
	
	public double getTargetCost() {
		double impressions = targetMetric.getImpressions();
		return 0.001 * impressions * priceVolumeCurve.getPredictedEcpm(impressions);
	}
	
	public String getComment() {
		return comment;
	}

	public int getAtf(int adGroupAtfFactor) {
		return (adGroupAtfFactor * targetMetric.getImpressions() < priceVolumeCurve.getMaxImpressions() ? 1 : 0) ;
	}
	
	public boolean equals(AdGroup adGroup) {
		return (id == adGroup.id);
	}
}
