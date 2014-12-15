package com.sharethis.delivery.job;

import java.util.List;

import com.sharethis.delivery.base.DOException;
import com.sharethis.delivery.common.Constants;
import com.sharethis.delivery.input.AdWordsData;

public class Update extends Runner {

	private Update(String[] args) {
		initialize(args);
	}

	private void parseUpdateProperties() throws DOException {
		parseProperties();
		Constants.URL_DELIVERY_OPTIMIZER = propertyParameters.get(Constants.URL_DELIVERY_OPTIMIZER);
		Constants.DB_USER = propertyParameters.get(Constants.DB_USER);
		Constants.DB_PASSWORD = propertyParameters.get(Constants.DB_PASSWORD);
	}

	protected int run() {
		try {
			parseUpdateProperties();

			if (!propertyParameters.getBoolean(Constants.RUN_UPDATE, false)) {
				log.info("Update stage skipped");
				return 0;
			}
			boolean readAllAgGroups = false;
			List<Long> adGroupIds = getAdGroupIds(readAllAgGroups);
			AdWordsData adWordsData = new AdWordsData(adGroupIds);
			if (adWordsData.readAdGroupSettings(propertyParameters)) {
				adWordsData.writeAdGroupSettings();
			} else {
				log.error("Error reading max cpm values from AdWords");
				return 1;
			}
			updateAdminDate("Update");
		} catch (Exception e) {
			log.error("Update Exception (run):", e);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		Update update = new Update(args);
		System.exit(update.run());
	}
}
