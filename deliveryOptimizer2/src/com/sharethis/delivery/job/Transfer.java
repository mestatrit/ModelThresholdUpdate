package com.sharethis.delivery.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.List;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdxData;
import com.sharethis.delivery.input.Data;
import com.sharethis.delivery.input.AuditData;
import com.sharethis.delivery.input.RtbData;
import com.sharethis.delivery.util.DateUtils;
import com.sharethis.delivery.util.DbUtils;

public class Transfer extends Runner {
	private String inputData;
	private String inputCase;
	private String pvDate;
	private String missingDates;

	private Transfer(String[] args) {
		initialize(args);
		pvDate = null;
		missingDates = "";
	}

	private void parseTransferProperties() throws DOException {
		options.addOption(Constants.INPUT_DATA_PROPERTY, true, "source of delivery optimizer input data");
		options.addOption(Constants.INPUT_PRICE_VOLUME_PROPERTY, true, "name of priceVolumeCurves table");
		options.addOption(Constants.INPUT_PRICE_VOLUME_DATE_PROPERTY, true, "date of priceVolumeCurves");
		options.addOption(Constants.INPUT_CASE_PROPERTY, true, "case of delivery optimizer input data (imp/kpi/all)");
		parseProperties();
		Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE = deliveryOptimizerPath + "/" + propertyParameters.get(Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE);
		inputData = line.getOptionValue(Constants.INPUT_DATA_PROPERTY);
		String pvTable = line.getOptionValue(Constants.INPUT_PRICE_VOLUME_PROPERTY);
		if (pvTable != null && !pvTable.trim().equals(Constants.DEFAULT)) {
			Constants.DO_PRICE_VOLUME_CURVES = pvTable;
			pvDate = line.getOptionValue(Constants.INPUT_PRICE_VOLUME_DATE_PROPERTY);
			if (pvDate != null && !pvDate.trim().equals(Constants.DEFAULT)) {
				pvDate = pvDate.substring(0, 4) + "-" + pvDate.substring(4, 6) + "-" + pvDate.substring(6, 8)
						+ " 00:00:00";
			} else {
				pvDate = null;
			}
		}
		inputCase = line.getOptionValue(Constants.INPUT_CASE_PROPERTY);
	}

	private boolean hasPriceVolumeData() throws ParseException, SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("SELECT value FROM admin_date WHERE name = \"PriceVolume\";");
		ResultSet result = statement.executeQuery(query);
		long time = 0L;
		if (result.next()) {
			time = DateUtils.parseTime(result, "value");
		}

		long yesterday = DateUtils.getToday(-1);
		return (time >= yesterday);
	}

	private int importImpressions() {
		try {
//			if (inputData.equals("rtb") && pvDate == null && !hasPriceVolumeData()) {
//				log.error("Price-volume task has not completed in time");
//				return 1;
//			}
			List<Long> campaignIds = getCampaignIds();
			for (long campaignId : campaignIds) {
				List<Integer> goalIds = getGoalIds(campaignId);
				for (int goalId : goalIds) {
					Data data = (inputData.equals("rtb") ? new RtbData(campaignId, goalId) : 
						new AdxData(propertyParameters, campaignId, goalId));
					if (data.read()) {
						data.writeDeliveredMetrics();
//						data.writeAdGroupSettings();
					}
					data.reportErrors();
				}
			}
			updateAdminDate("Transfer:Imp");
		} catch (Exception e) {
			log.error("Transfer Exception (importImpressions):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
		return 0;
	}
	
	private int importKpis() {
		try {
			List<Long> campaignIds = getCampaignIds();
			for (long campaignId : campaignIds) {
				List<Integer> goalIds = getGoalIds(campaignId);
				for (int goalId : goalIds) {
					AuditData data = new AuditData(campaignId, goalId);
					if (data.read()) {
						data.writeDeliveredMetrics();
						missingDates += data.getMissingDates();
					} else {
						log.info("No kpi data read");
					}
					data.writePredictedToDeliveredKpis();
					data.reportErrors();
				}
			}
			writeToFile(Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE, missingDates);
			updateAdminDate("Transfer:Kpi");
		} catch (Exception e) {
			log.error("Transfer Exception (importKpis):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
		return 0;
	}
	
	protected int run() {
		try {
			parseTransferProperties();
			cleanErrorsTable();
		} catch (Exception e) {
			log.error("Transfer Parse/Errors Exception (run):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
		if (!propertyParameters.getBoolean(Constants.RUN_TRANSFER, false)) {
			log.info("Transfer stage skipped");
			return 0;
		}
		if (inputCase == null || !Constants.INPUT_TYPES.contains(inputCase)) {
			log.error("Unknown case argument: " + inputCase);
			return 1;
		}
		if (inputData == null || !Constants.INPUT_DATA_SOURCES.contains(inputData)) {
			log.error("Unknown input data argument: " + inputData);
			return 1;
		}
		log.info("Transfer case: " + inputCase + ", transfer input data: " + inputData);

		if (inputCase.equals("imp") || inputCase.equals("all")) {
			log.info("");
			log.info("Transfer impressions:");
			if (importImpressions() > 0) {
				return 1;
			}
		}
		if (inputCase.equals("kpi") || inputCase.equals("all")) {
			log.info("");
			log.info("Transfer kpis:");
			return importKpis();
		} else {
			return 0;
		}
	}

	public static void main(String[] args) {
		Transfer transfer = new Transfer(args);
		System.exit(transfer.run());
	}
}
