package com.sharethis.delivery.job;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.rtb.TargetDelivery;

public class Deliver extends Runner {
	private String urlRead, urlWriteRtb, urlWriteAdPlatform;
	private boolean outputAdx;
	private boolean outputRtb;
	
	private Deliver(String[] args) {
		initialize(args);
	}

	private void parseDeliverProperties() throws DOException {
		options.addOption(Constants.OUTPUT_DELIVER_ADX_PROPERTY, true, "deliver max cpm to adx?");
		options.addOption(Constants.OUTPUT_DELIVER_RTB_PROPERTY, true, "deliver atf to rtb?");
		parseProperties();
		urlRead =  Constants.URL_DELIVERY_OPTIMIZER;
		urlWriteRtb = Constants.URL_RTB;
		urlWriteAdPlatform = Constants.URL_ADPLATFORM;
		outputAdx = Boolean.parseBoolean(line.getOptionValue(Constants.OUTPUT_DELIVER_ADX_PROPERTY));
		outputRtb = Boolean.parseBoolean(line.getOptionValue(Constants.OUTPUT_DELIVER_RTB_PROPERTY));
	}

	protected int run() {
		try {
			parseDeliverProperties();
			if (!propertyParameters.getBoolean(Constants.RUN_DELIVER, false)) {
				log.info("Deliver stage skipped");
				return 0;
			}
			TargetDelivery targetDelivery = new TargetDelivery(urlRead, urlWriteRtb, urlWriteAdPlatform);
			targetDelivery.readTargets();
			targetDelivery.writeTargets();
			targetDelivery.writeBids();
			if (outputAdx) {
					targetDelivery.writeAdx(propertyParameters);
			} else {
				log.info("Adx not updated");
			}
			if (outputRtb) {
				targetDelivery.writeRtb();
			} else {
				log.info("Rtb ad group properties not updated");
			}
			targetDelivery.reportErrors();
			updateAdminDate("Deliver");
			return (targetDelivery.size() > 1 ? 0 : 1);
		} catch (Exception e) {
			log.error("Deliver Exception (run):", e);
			writeToFile(Constants.DO_ERROR_FILE, e);
			return 1;
		}
	}

	public static void main(String[] args) {
		Deliver deliver = new Deliver(args);
		System.exit(deliver.run());
	}
}
