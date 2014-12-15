package com.sharethis.adoptimization.inventory;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class PeriodicalInventoryTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PeriodicalInventoryTask.class);
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
		PeriodicalInventoryList dailyList = new PeriodicalInventoryList();
		int aggFlagInv = config.getInt("AggFlagDailyInv", 99);
		sLogger.info("AggFlagDailyInv = " + aggFlagInv);
		switch(aggFlagInv){
			case 0:
				dailyList.dailyAggregatingDayInventoryData(config, pDateStr);
				break;
			case 1:
				dailyList.dailyAggregatingWeekdayInventoryData(config, pDateStr);
				break;
			case 2:
				dailyList.dailyAggregatingWeeklyInventoryData(config, pDateStr);
				break;
			case 3:
				dailyList.dailyAggregating4WeekInventoryData(config, pDateStr);
				break;
			case 4:
				dailyList.dailyAggregatingPeriodInventoryData(config, pDateStr);
				break;
			case 99:
				dailyList.dailyAggregatingInventoryData(config, pDateStr);
				break;
		}
		return 0;		
	}				
}
