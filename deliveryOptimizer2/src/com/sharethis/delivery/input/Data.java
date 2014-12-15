package com.sharethis.delivery.input;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.delivery.base.CampaignType;
import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.Errors;
import com.sharethis.delivery.base.State;
import com.sharethis.delivery.base.StatusType;
import com.sharethis.delivery.common.CampaignParameters;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

@SuppressWarnings("serial")
public abstract class Data extends ArrayList<State> {
	protected static final Logger log = Logger.getLogger(Constants.DO_LOGGER_NAME);
	protected Parameters campaignParameters;
	protected long campaignId;
	protected int goalId;
	protected int segmentCount;
	protected int updateSegmentCount;
	protected Map<Long, Long> times;
	protected Errors errors;
	
	public Data(long campaignId, int goalId) {
		super();
		this.campaignId = campaignId;
		this.goalId = goalId;
		this.updateSegmentCount = 0;
		times = new HashMap<Long, Long>();
		errors = new Errors();
	}

	public abstract boolean readDeliveredMetrics() throws IOException, ParseException, SQLException, DOException;

	public abstract void writeAdGroupSettings() throws IOException, ParseException, SQLException;

	public boolean read() throws ParseException, DOException, SQLException, IOException {
		if (!readCampaignParameters())
			return false;
		if (!hasInputData())
			return false;
		if (!readTime())
			return false;
		if (!readStates())
			return false;
		if (!readDeliveredMetrics()) {
			return false;
		}
		return true;
	}
	
	protected boolean hasInputData() throws ParseException {
		long midnight = DateUtils.getToday(0);
		long now = DateUtils.currentTime();
		if (now - midnight < 4L * Constants.MILLISECONDS_PER_HOUR) {
			errors.add("input data not ready for transfer");
			errors.log();
			return false;
		} else {
			return true;
		}
	}

	protected boolean readCampaignParameters() throws ParseException, SQLException {
		campaignParameters = new CampaignParameters(campaignId, goalId);
		String campaignName = campaignParameters.get(Constants.CAMPAIGN_NAME);
		int status = campaignParameters.getInt(Constants.STATUS, 0);
		if (status == 0) {
			errors.add("Paused");
			errors.log(campaignName);			
			return false;
		}

		segmentCount = campaignParameters.getInt(Constants.SEGMENTS, 0);
		if (segmentCount < 1) {
			errors.add("Unsuported number of audience segments: " + segmentCount);
			errors.log(campaignName);
			return false;
		}
		return true;
	}

	protected boolean readStates() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT * FROM %s WHERE campaignId = %d ORDER BY segment ASC;",
				Constants.DO_AD_GROUP_MAPPINGS, campaignId);
		ResultSet result = statement.executeQuery(query);
		for (long id : times.keySet()) {
			result.beforeFirst();
			long time = times.get(id);
			State state = new State(id, time, segmentCount, campaignParameters);
			if (!state.parsePhysicalState(result)) {
				errors = state.getErrors();
				return false;
			}
			this.add(state);
		}
		return true;
	}

	protected boolean readTime() throws SQLException, ParseException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		StatusType status = new StatusType();
		status.setImpression();
		String query = String
				.format("SELECT id, date FROM %s WHERE campaignId = %d AND goalId = %d AND status & %s >= 0 ORDER BY DATE ASC;",
						Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, status.toString());
		ResultSet result = statement.executeQuery(query);
		while (result.next()) {
			long id = result.getLong("id");
			long time = DateUtils.parseTime(result, "date");
			long midnight = DateUtils.getToday(0);
			if (time < midnight) {
				times.put(id, time);
			}
		} 
		if (times.isEmpty()) {
			errors.add("New impression data not needed");
			errors.log();
			return false;
		} else {
			return true;
		}
	}

	public void writeDeliveredMetrics() throws IOException, ParseException, SQLException {
		for (State state : this) {
			state.writeDeliveryMetrics();
		}
	}

	public void writePredictedToDeliveredKpis() throws SQLException {
		CampaignType campaignType = CampaignType.valueOf(campaignParameters);
		if (campaignType.isCpc()) {
			return;
		}
		StatusType status = new StatusType();
		status.setKpi();
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String
				.format("UPDATE %s t1 INNER JOIN %s t2 ON t1.campaignRecordId = t2.id SET t1.deliveryConversions = t1.targetConversions "
				 	  + "WHERE t2.campaignId = %d AND t2.goalId = %d AND t2.status & %s = 0;",
						Constants.DO_SEGMENT_PERFORMANCES, Constants.DO_CAMPAIGN_PERFORMANCES, campaignId, goalId, status.toString());
		int count = statement.executeUpdate(query);
		log.info(String.format("Number of delivery records updated by predicted kpis: %d", count));
	}

	public boolean isActive() {
		return (campaignParameters.getInt(Constants.STATUS, 0) > 0);
	}

	public Errors getError() {
		return errors;
	}
	
	public void reportErrors() throws SQLException {
		if (!errors.isEmpty() && isActive()) {
			errors.setTask("Transfer");
			errors.setIds(campaignParameters);
			errors.updateErrorTable();
		}
	}
}
