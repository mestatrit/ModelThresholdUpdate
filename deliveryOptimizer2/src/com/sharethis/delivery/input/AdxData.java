package com.sharethis.delivery.input;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Map;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Metric;
import com.sharethis.delivery.base.State;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

@SuppressWarnings("serial")
public class AdxData extends Data {
	private Parameters propertyParameters;
	private Map<Long, Double> maxCpm;
	private Map<Long, String> adGroupName;

	public AdxData(Parameters propertyParameters, long campaignId, int goalId) {
		super(campaignId, goalId);
		this.propertyParameters = propertyParameters;
	}

	public boolean readDeliveredMetrics() throws DOException, ParseException {
		for (State state : this) {
			Map<Long, String> adGroupIds = state.getAdGroupIds();
			if (adGroupIds.isEmpty()) {
				errors.add("State has no ad groups");
				return false;
			}
			AdWordsData adWordsData = new AdWordsData(adGroupIds);
			Map<Long, Metric> deliveredMetrics = adWordsData.readDeliveryMetrics(state.getTime(), propertyParameters);
			maxCpm = adWordsData.getMaxCpm();
			adGroupName = adWordsData.getAdGroupName();
			if (!state.addDeliveryMetrics(deliveredMetrics)) {
				errors = state.getErrors();
				return false;
			}
		}
		return true;
	}

	public void writeAdGroupSettings() throws IOException, ParseException, SQLException {
		if (updateSegmentCount != segmentCount) {
			log.info(String.format("Records in %s with campaignId = %d, goalId = %d not updated",
					Constants.DO_AD_GROUP_SETTINGS, campaignId, goalId));
			return;
		}
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String datetime = DateUtils.getDatetime();

			for (long adGroupId : maxCpm.keySet()) {
				String query = String.format("SELECT * FROM %s WHERE adGroupId = %d;", Constants.DO_AD_GROUP_SETTINGS,
						adGroupId);
				ResultSet result = statement.executeQuery(query);
				if (result.next()) {
					long id = result.getLong("id");
					query = String.format("UPDATE %s SET maxCpm = %.2f, updateDate='%s' WHERE id = %d;",
							Constants.DO_AD_GROUP_SETTINGS, maxCpm.get(adGroupId), datetime, id);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %d updated with maxCpm = %.2f",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, maxCpm.get(adGroupId)));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d updated %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
					}
				} else {
					query = String
							.format("INSERT INTO %s (adGroupId,adGroupName,maxCpm,updateDate,createDate) VALUES (%d,'%s',%f,'%s','%s');",
									Constants.DO_AD_GROUP_SETTINGS, adGroupId, adGroupName.get(adGroupId),
									maxCpm.get(adGroupId), datetime, datetime);
					int count = statement.executeUpdate(query);
					if (count == 1) {
						log.info(String.format("Record in %s with adGroupId = %d inserted with maxCpm = %.2f",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, maxCpm.get(adGroupId)));
					} else {
						log.error(String.format("Record in %s with adGroupId = %d inserted %d times",
								Constants.DO_AD_GROUP_SETTINGS, adGroupId, count));
					}
				}
			}
		}
}
