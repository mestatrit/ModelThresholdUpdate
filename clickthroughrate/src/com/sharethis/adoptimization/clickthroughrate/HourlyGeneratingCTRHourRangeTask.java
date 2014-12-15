package com.sharethis.adoptimization.clickthroughrate;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

import java.text.SimpleDateFormat;
import java.util.Date;


public class HourlyGeneratingCTRHourRangeTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingCTRHourRangeTask.class);
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
		int startHour = config.getInt("StartHour", 0);
		int endHour = config.getInt("EndHour", 1);
		int aggFlag = config.getInt("AggFlagHourly", 1);
		HourlyGeneratingCTRList hList = new HourlyGeneratingCTRList();
		sLogger.info("AggFlagHouyly = " + aggFlag);
		for(int i_hour=startHour; i_hour<endHour; i_hour++){				
			//Performing the list of all tasks at a hour level
			switch(aggFlag){
			case 0:
				hList.hourlyCTRAggFromHourlyData(config, i_hour);
				break;
			case 1:
				hList.hourlyCTRAggFromHourlyBase(config, i_hour);
				break;
			}
		}
		return 0;		
	}		
}
