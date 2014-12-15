package com.sharethis.delivery.job;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.rtb.TargetDelivery;

public class Deliver extends Runner {
	private String urlRead, urlWrite;
	private boolean outputAdx;
	private boolean outputRtb;
	
	private Deliver(String[] args) {
		initialize(args);
	}

	private void parseDeliverProperties() throws DOException {
		options.addOption(Constants.OUTPUT_DELIVER_ADX_PROPERTY, true, "deliver max cpm to adx?");
		options.addOption(Constants.OUTPUT_DELIVER_RTB_PROPERTY, true, "deliver atf to rtb?");
		parseProperties();
		urlRead =  propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER);
		urlWrite = propertyParameters.get(Constants.URL_RTB);
		Constants.URL_DELIVERY_OPTIMIZER = urlRead;
		Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
		Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
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
			TargetDelivery deliveryTargets = new TargetDelivery(urlRead, urlWrite);
			if (deliveryTargets.readTargets()) {
				deliveryTargets.writeTargets();
				if (outputAdx) {
					deliveryTargets.writeAdx(propertyParameters);
				} else {
					log.info("Adx not updated");
				}
				if (outputRtb) {
					deliveryTargets.writeRtb();
				} else {
					log.info("Rtb ad group properties not updated");
				}
			} else {
				return 1;
			}
			updateAdminDate("Deliver");
		} catch (Exception e) {
			log.error("Deliver Exception (run):", e);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		Deliver deliver = new Deliver(args);
		System.exit(deliver.run());
	}
}
