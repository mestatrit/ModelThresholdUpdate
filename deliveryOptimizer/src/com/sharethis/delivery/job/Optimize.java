package com.sharethis.delivery.job;

import java.sql.SQLException;
import java.util.List;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.base.PriceVolumeCurve;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.common.Parameters;
import com.sharethis.delivery.optimization.MultiGoalOptimizer;
import com.sharethis.delivery.rtb.TargetDelivery;

public class Optimize extends Runner {
	private Parameters globalParameters;
	private String pvDate;

	private Optimize(String[] args) {
		initialize(args);
		pvDate = null;
	}

	private void parseOptimizeProperties() throws DOException, SQLException {
		options.addOption(Constants.INPUT_PRICE_VOLUME_PROPERTY, true, "name of priceVolumeCurves table");
		options.addOption(Constants.INPUT_PRICE_VOLUME_DATE_PROPERTY, true, "date of priceVolumeCurves");
		parseProperties();
		Constants.URL_DELIVERY_OPTIMIZER = propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER);
		Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
		Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
		globalParameters = new Parameters(Constants.URL_DELIVERY_OPTIMIZER, Constants.DB_USER, Constants.DB_PASSWORD,
				Constants.DO_GLOBAL_PARAMETERS);
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
	}

	protected int run() {
		try {
			parseOptimizeProperties();
			
			if (!propertyParameters.getBoolean(Constants.RUN_OPTIMIZE, false)) {
				log.info("Optimize stage skipped");
				return 0;
			}
			TargetDelivery.truncateImpressionTargets();
			PriceVolumeCurve.backupPriceVolumeCurves();
			List<Long> campaignIds = getCampaignIds();
			for (long campaignId : campaignIds) {
				List<Integer> goalIds = getGoalIds(campaignId);
				MultiGoalOptimizer optimizer = new MultiGoalOptimizer(campaignId, goalIds, globalParameters);
				optimizer.process(pvDate);
			}
			updateAdminDate("Optimize");
		} catch (Exception e) {
			log.error("Optimize Exception (run):", e);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		Optimize optimize = new Optimize(args);
		System.exit(optimize.run());
	}
}
