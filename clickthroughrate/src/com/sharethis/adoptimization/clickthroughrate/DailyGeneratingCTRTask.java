package com.sharethis.adoptimization.clickthroughrate;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class DailyGeneratingCTRTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingCTRTask.class);
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
		case -1:
			dailyCTRList.dailyCTRWeekDayFromDailyBase(config, pDateStr);
			break;
		case 0:
			dailyCTRList.dailyCTRDayAllFromDailyBase(config, pDateStr);
			break;
		case 1:
			dailyCTRList.dailyCTRWeekFromDailyBase(config, pDateStr);
			break;
		case 2:
			dailyCTRList.dailyCTR4WeeksFromDailyBase(config, pDateStr);
			break;
		case 3:
			dailyCTRList.dailyCTRPeriodFromDailyBase(config, pDateStr);
			break;
		case 4:
			dailyCTRList.dailyCTRAggFromDailyDataEx(config, pDateStr);
			break;
		case 99:
			dailyCTRList.dailyCTRAggFromDailyBase(config, pDateStr);
			break;
		case 1004:
			dailyCTRList.dailyCTRPeriodFromDailyBaseRolling(config, pDateStr);
			break;
		case 1003:
			dailyCTRList.dailyCTR4WeeksFromDailyBaseRolling(config, pDateStr);
			break;
		case 1002:
			dailyCTRList.dailyCTRWeekFromDailyBaseRolling(config, pDateStr);
			break;
		case 1001:
			dailyCTRList.dailyCTRWeekDayFromDailyBaseRolling(config, pDateStr);
			break;
		case 1999:
			dailyCTRList.dailyCTRAggFromDailyBaseRolling(config, pDateStr);
			break;
		}
		return 0;		
	}				
}
