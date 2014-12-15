package com.sharethis.delivery.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.List;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdxData;
import com.sharethis.delivery.input.Data;
import com.sharethis.delivery.input.KpiData;
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
		Constants.URL_DELIVERY_OPTIMIZER = propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER);
		Constants.URL_CAMPAIGN = propertyParameters.get(Constants.URL_CAMPAIGN);
		Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
		Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
		Constants.ADX_FILE = propertyParameters.get(Constants.ADX_FILE);
		Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE = propertyParameters.get(Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE);
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
		Connection connection = DbUtils.getConnection(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER,
				Constants.DB_PASSWORD);
		Statement statement = connection.createStatement();
		String query = String.format("SELECT value FROM rtbDelivery.admin_date WHERE name = \"PriceVolume\";");
		ResultSet result = statement.executeQuery(query);
		long time = 0L;
		if (result.next()) {
			time = DateUtils.parseTime(result, "value");
		}
		connection.close();
		connection = null;

		long yesterday = DateUtils.getToday(-1);
		return (time >= yesterday);
	}

	private int importImpressions() {
		try {
			if (inputData.equals("rtb") && pvDate == null && !hasPriceVolumeData()) {
				log.error("Price-volume task has not completed in time");
				return 1;
			}
			List<Long> campaignIds = getCampaignIds();
			for (long campaignId : campaignIds) {
				List<Integer> goalIds = getGoalIds(campaignId);
				for (int goalId : goalIds) {
					Data data = (inputData.equals("rtb") ? new RtbData(campaignId, goalId) : new AdxData(
							propertyParameters, campaignId, goalId));
					if (data.read()) {
						data.writeCampaignPerformances();
						data.writeAdGroupSettings();
					} else if (data.isActive()) {
						log.error("Impression import: Error reading adx data/parameters for campaign: " + campaignId + ", goal id: "
								+ goalId);
					}
				}
			}
			updateAdminDate("Transfer:Imp");
		} catch (Exception e) {
			log.error("Transfer Exception (importImpressions):", e);
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
					KpiData data = new KpiData(campaignId, goalId);
					if (data.read()) {
						data.writeCampaignPerformances();
						missingDates += data.getMissingDates();
					} else if (data.isActive()) {
						log.error("KPI import: Error reading adx data/parameters for campaign: " + campaignId + ", goal id: "
								+ goalId);
					}
				}
			}
			writeErrorFile(missingDates);
			updateAdminDate("Transfer:Kpi");
		} catch (Exception e) {
			log.error("Transfer Exception (importKpis):", e);
			return 1;
		}
		return 0;
	}
	
	private void writeErrorFile(String msg) throws IOException {
		File file = new File(Constants.DO_TRANSFER_MISSING_KPI_DATES_FILE);
		if (msg.length() == 0) {
			file.delete();
		} else {
			Writer output = new BufferedWriter(new FileWriter(file));
			output.write(msg);
			output.close();
		}
	}
	
	protected int run() {
			try {
				parseTransferProperties();
			} catch (DOException e) {
				log.error("Transfer Parse Exception (run):", e);
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
				if (importImpressions() > 0) {
					return 1;
				}
			}
			return (inputCase.equals("kpi") || inputCase.equals("all") ? importKpis() : 0);
	}

	public static void main(String[] args) {
		Transfer transfer = new Transfer(args);
		System.exit(transfer.run());
	}
}
