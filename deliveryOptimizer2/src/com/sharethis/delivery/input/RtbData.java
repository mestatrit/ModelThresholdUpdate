package com.sharethis.delivery.input;

import java.sql.SQLException;
import java.text.ParseException;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Segment;
import com.sharethis.delivery.base.State;

@SuppressWarnings("serial")
public class RtbData extends Data {

	public RtbData(long campaignId, int goalId) {
		super(campaignId, goalId);
	}
	
	public boolean readDeliveredMetrics() throws DOException, ParseException, SQLException {
//		long deliveryInterval = (long) Math.round(campaignParameters.getDouble(Constants.DELIVERY_INTERVAL, 0.0) * Constants.MILLISECONDS_PER_HOUR);
		for (State state : this) {
			long startTime = state.getTime();
			long endTime = campaignParameters.getDeliveryIntervalEndTime(startTime);
			for (Segment segment : state.getSegments()) {
				if (!segment.readDeliveryImpressions(startTime, endTime)) {
					return false;
				}
			}
			state.setImpressionStatus();
		}
		return true;
	}

	public void writeAdGroupSettings() {
	}
}
