package com.sharethis.delivery.optimization;

import org.apache.log4j.Logger;

import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;

public abstract class Optimizer {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	protected long campaignId;
	protected int goalId;
	protected int status;
	protected int segmentCount;
	protected Parameters campaignParameters;
	
	protected void initialize(long campaignId, int goalId) {
		this.campaignId = campaignId;
		this.goalId = goalId;
	}
	
	public boolean isActive() {
		return (status > 0);
	}
}
