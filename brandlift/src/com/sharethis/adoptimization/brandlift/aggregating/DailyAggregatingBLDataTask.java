package com.sharethis.adoptimization.brandlift.aggregating;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

import java.text.SimpleDateFormat;
import java.util.Date;


public class DailyAggregatingBLDataTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DailyAggregatingBLDataTask.class);
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
		//Performing the list of all tasks at a hour level
		String pDateStr = config.get("ProcessingDate");
		DailyAggregatingBLData dailyData = new DailyAggregatingBLData();
		sLogger.info("\nStarting the generation of daily data on " + pDateStr + " ...");
		dailyData.aggregatingBLDataDaily(config);
		sLogger.info("The generation of daily data on " + pDateStr + " is done.\n");
		return 0;		
	}		
}
