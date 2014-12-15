package com.sharethis.adoptimization.pricevolume;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.pricevolume.GeneratingPV;


public class GeneratingPVTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(GeneratingPVTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		
		if(config.get("ProcessingDate")==null){
			// If ProcessDate is null, then using the date of yesterday as the processing date.
			Date date = DateUtils.addDays(new Date(), -1);
			config.set("ProcessingDate", sdf.format(date));
		}

		sLogger.info("\nStarting generating the pv task ...");
		boolean dailyTaskFlag = config.getBoolean("DailyTaskFlag", true);
		GeneratingPV gPV = new GeneratingPV();
		if(dailyTaskFlag)
			gPV.generatingPVDailyAgg(config);
		int numOfDays = Integer.parseInt(config.get("NumOfDays"));
		if(numOfDays>1)
			gPV.generatingPVAgg(config);
		sLogger.info("Ended generating the pv task.\n");
		return 0;	
	}			
}
