package com.sharethis.delivery.base;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.util.DateUtils;


public class AdGroup {
	protected static final Logger log = Logger.getLogger(Constants.LOGGER_NAME);
	private long id;
	private String name;
	private int priority;
	private double yield;
	private double lowCpm, highCpm;
	private PriceVolumeCurve curve;
	private int impressionTarget;
	private int nextImpressionTarget;
	
	public AdGroup(long id, String name, int priority, double yield, double lowCpm, double highCpm, PriceVolumeCurve curve) {
		this.id = id;
		this.name = name;
		this.priority = priority;
		this.yield = yield;
		this.lowCpm = lowCpm;
		this.highCpm = highCpm;
		this.curve = curve;
		this.impressionTarget = 0;
		this.nextImpressionTarget = 0;
	}
	
	public AdGroup(long id, String name, int priority, double yield, double lowCpm, double highCpm) {
		this(id, name, priority, yield, lowCpm, highCpm, new PriceVolumeCurve(0.0, 0, 0, 0.0, DateUtils.currentTime()));
	}
	
	public void setCurve(PriceVolumeCurve curve) {
		this.curve = curve;
	}
	
	public void setImpressionTargets(int impressionTarget, int nextImpressionTarget) {
		this.impressionTarget = impressionTarget;
		this.nextImpressionTarget = nextImpressionTarget;
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
		
	public int getDeliveredImpressions() {
		return curve.getDeliveredImpressions();
	}
	
	public double getDeliveredEcpm() {
		return curve.getDeliveredEcpm();
	}

	public double getPredictedEcpm(int impressionTarget) {
		return curve.getPredictedEcpm(impressionTarget);
	}
	
	public int getMaxImpressions() {
		return curve.getMaxImpressions();
	}
	
	public double getMaxCpm() {
		return curve.getMaxCpm(impressionTarget);
	}
	
	public long getTime() {
		return curve.getTime();
	}

	public int getImpressionTarget() {
		return impressionTarget;
	}	
	
	public int getNextImpressionTarget() {
		return nextImpressionTarget;
	}
	
	public int getAtf(int adGroupAtfFactor) {
		return (adGroupAtfFactor * impressionTarget < getMaxImpressions() ? 1 : 0) ;
	}
	
	public boolean equals(AdGroup adGroup) {
		return (id == adGroup.id);
	}
	
	public boolean equals(long adGroupId) {
		return (id == adGroupId);
	}
	
	public boolean equals(String adGroupName) {
		return name.equals(adGroupName);
	}
}
