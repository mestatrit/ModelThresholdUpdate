package com.sharethis.delivery.job;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.List;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.PriceVolumeCurve;
import com.sharethis.delivery.base.StrategyType;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.optimization.MultiGoalOptimizer;
import com.sharethis.delivery.util.DbUtils;

public class Optimize extends Runner {
	private String pvDate;

	private Optimize(String[] args) {
		initialize(args);
		pvDate = null;
	}

	private void parseOptimizeProperties() throws DOException, SQLException, ParseException {
		options.addOption(Constants.INPUT_PRICE_VOLUME_PROPERTY, true, "name of priceVolumeCurves table");
		options.addOption(Constants.INPUT_PRICE_VOLUME_DATE_PROPERTY, true, "date of priceVolumeCurves");
		parseProperties();
		Constants.globalParameters.read(Constants.URL_DELIVERY_OPTIMIZER, Constants.DO_GLOBAL_PARAMETERS);
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
		StrategyType.setHolidays();
	}
	
	private void truncateImpressionTargets() throws SQLException {
		Statement statement = DbUtils.getStatement(Constants.URL_DELIVERY_OPTIMIZER);
		String query = String.format("TRUNCATE TABLE %s;", Constants.DO_IMPRESSION_TARGETS);
		statement.executeUpdate(query);
		log.info(Constants.DO_IMPRESSION_TARGETS + " table truncated");
	}

	protected int run() {
		try {
			parseOptimizeProperties();
			if (!propertyParameters.getBoolean(Constants.RUN_OPTIMIZE, false)) {
				log.info("Optimize stage skipped");
				return 0;
			}
			PriceVolumeCurve.backupPriceVolumeCurves();
			truncateImpressionTargets();
			cleanErrorsTable();
			List<Long> campaignIds = getCampaignIds();
			for (long campaignId : campaignIds) {
				List<Integer> goalIds = getGoalIds(campaignId);
				MultiGoalOptimizer optimizer = new MultiGoalOptimizer(campaignId, goalIds);
				optimizer.process(pvDate);
			}
			updateAdminDate("Optimize");
		} catch (Exception e) {
			DbUtils.close();
			log.error("Optimize Exception (run):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
		DbUtils.close();
		return 0;
	}

	public static void main(String[] args) {
		Optimize optimize = new Optimize(args);
		System.exit(optimize.run());
	}
}
