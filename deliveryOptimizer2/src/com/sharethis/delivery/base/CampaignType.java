package com.sharethis.delivery.base;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;

public enum CampaignType {
	CPA, CPC, ROAS, UNKNOWN;
	
	private double kpi;
	private double cpm;
	
	public static CampaignType valueOf(Parameters campaignParameters) {
		String type = campaignParameters.get(Constants.CAMPAIGN_TYPE, "");
		for (CampaignType campaignType : values()) {
			if (type.equalsIgnoreCase(campaignType.toString())) {
				campaignType.setKpi(campaignParameters);
				return campaignType;
			}
		}
		return UNKNOWN;
	}
	
	private void setKpi(Parameters campaignParameters) {
		kpi = campaignParameters.getDouble(Constants.KPI, 0.0);
		cpm = campaignParameters.getDouble(Constants.CPM, 0.0);
	}
	
	public double getKpi() {
		return kpi;
	}
	
	public double getActions(Metric metric) {
		return (this.isCpc() ? metric.getClicks() : metric.getConversions());
	}
	
	public double getFom(Metric metric) {
		return (metric.getImpressions() > 1000.0 ? getActions(metric) / (0.001 * cpm * metric.getImpressions()) : Double.NaN);
	}
	
	public double getEkpi(Metric metric) {
		double actions = getActions(metric);
		return (actions > 0 ? (0.001 * cpm * metric.getImpressions()) / actions : Double.NaN);
	}
	
	public double getImpliedKpiRatio() {
		return 0.001 * cpm / kpi;
	}
	
	public boolean isValid() {
		return this != UNKNOWN;
	}
	
	public boolean isCpc() {
		return this == CPC;
	}
	
	public boolean isRoas() {
		return this == ROAS;
	}
}
