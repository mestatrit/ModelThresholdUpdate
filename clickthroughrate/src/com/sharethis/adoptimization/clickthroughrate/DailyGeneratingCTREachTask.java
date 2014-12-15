package com.sharethis.adoptimization.clickthroughrate;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class DailyGeneratingCTREachTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingCTREachTask.class);
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public int run(String[] args) throws Exception {
		sLogger.info("Getting the configuration ...");
		Configuration config = ConfigurationUtil.setConf(args);
		sLogger.info("Getting the configuration is done.");	
		String pDateStr = config.get("ProcessingDate");
		if(pDateStr==null){
			Date pDate = new Date();
			pDateStr = sdf.format(pDate);
			config.set("ProcessingDate", pDateStr);
		}
		int aggFlag = config.getInt("AggFlagDaily", 99);
		DailyGeneratingCTRList dailyCTRList = new DailyGeneratingCTRList();
		sLogger.info("AggFlagDaily = " + aggFlag);
		switch(aggFlag){
		case -4:
			dailyCTRList.uploadingDailyCTR(config, pDateStr);
			break;
		case -3:
			dailyCTRList.dailyCTRMapFromDailyData(config, pDateStr);
			break;
		case -2:
			dailyCTRList.dailyCTRDayFromDailyBase(config, pDateStr);
			break;
		case 1004:
			dailyCTRList.dailyCTRPeriodRollingEach(config, pDateStr);
			break;
		case 1003:
			dailyCTRList.dailyCTR4WeeksRollingEach(config, pDateStr);
			break;
		case 1002:
			dailyCTRList.dailyCTRWeekRollingEach(config, pDateStr);
			break;
		case 1001:
			dailyCTRList.dailyCTRWeekDayRollingEach(config, pDateStr);
			break;
		case 1999:
			dailyCTRList.dailyCTRAggRollingEach(config, pDateStr);
			break;
		}
		return 0;		
	}				
}
