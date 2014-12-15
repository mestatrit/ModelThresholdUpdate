package com.sharethis.adoptimization.inventory;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;

public class PeriodicalInventoryList
{
	private static final Logger sLogger = Logger.getLogger(PeriodicalInventoryList.class);	
	
	public void dailyAggregatingDayInventoryData(Configuration config, String pDateStr) throws Exception {		
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily inventory data task on " + pDateStr + " starts ...");
		DailyInventoryGeneration dailyNB = new DailyInventoryGeneration();
		String basePipeName = config.get("BasePipeNameDaily");
		String baseKeyFields = config.get("BaseKeyFieldsDaily");
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily inventory data on " + pDateStr + " ...");
			dailyNB.generatingInventoryDaily(config, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("Generating the daily inventory data on " + pDateStr + " is done.");
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTask");
		sLogger.info("The daily inventory data task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyAggregatingWeekdayInventoryData(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe weekday inventory data task on " + pDateStr + " starts ...");
		
		PeriodicalInventoryGeneration aggNB = new PeriodicalInventoryGeneration();
		int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesListWeekday");
		String keyFieldsList = config.get("KeyFieldsListWeekday");
		if(numOfWeeksWD>0&&pipeNamesList!=null){
			int interval = 7;
			String infile_postfix = "_daily";
			String outfile_postfix = "_weekday";
			sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
			aggNB.generatingInventory(config, numOfWeeksWD, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTaskWeekday");
		sLogger.info("The weekday inventory data task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyAggregatingWeeklyInventoryData(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe weekly inventory data task on " + pDateStr + " starts ...");
		PeriodicalInventoryGeneration aggNB = new PeriodicalInventoryGeneration();
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesListWeekly");
		String keyFieldsList = config.get("KeyFieldsListWeekly");
		int numOfDays = config.getInt("NumOfDays", 7);
		if(numOfDays>0&&pipeNamesList!=null){
			int interval = 1;
			String infile_postfix = "_daily";
			String outfile_postfix = "_weekly";
			sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
			aggNB.generatingInventory(config, numOfDays, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTaskWeek");
		sLogger.info("The weekly inventory data task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyAggregating4WeekInventoryData(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe 4 week inventory data task on " + pDateStr + " starts ...");
		
		PeriodicalInventoryGeneration aggNB = new PeriodicalInventoryGeneration();
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesList4Weeks");
		String keyFieldsList = config.get("KeyFieldsList4Weeks");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		if(numOfWeeks>0&&pipeNamesList!=null){
			int interval = 7;
			String infile_postfix = "_weekly";
			String outfile_postfix = "_" + numOfWeeks + "_weeks";
			sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
			aggNB.generatingInventory(config, numOfWeeks, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
		}		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTask4Week");
		sLogger.info("The 4 week inventory data task on " + pDateStr + " is done.\n");	
	}
	
	
	public void dailyAggregatingPeriodInventoryData(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe periodical inventory data task on " + pDateStr + " starts ...");		
		PeriodicalInventoryGeneration aggNB = new PeriodicalInventoryGeneration();
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesListAgg");
		String keyFieldsList = config.get("KeyFieldsListAgg");
		int numOfPeriods = config.getInt("NumOfPeriods", 3);
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		if(numOfPeriods>0&&pipeNamesList!=null){
			int interval = 28;
			String infile_postfix = "_" + numOfWeeks + "_weeks";
			String outfile_postfix = "_agg";
			sLogger.info("Generating the aggregation of 4 week data on " + pDateStr + " over " + numOfPeriods + " periods ...");
			aggNB.generatingInventory(config, numOfPeriods, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of 4 week data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
		}		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTaskPeriod");
		sLogger.info("The periodical inventory data task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyAggregatingInventoryData(Configuration config, String pDateStr) throws Exception {		
		PriceVolumeUtils adoptUtils = new PriceVolumeUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily overall task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", 3);
		dailyAggregatingDayInventoryData(config, pDateStr);
		
		if(dayAggLevel>=0){
			dailyAggregatingWeekdayInventoryData(config, pDateStr);
			dailyAggregatingWeeklyInventoryData(config, pDateStr);
		}

		if(dayAggLevel>1){
			dailyAggregating4WeekInventoryData(config, pDateStr);
		}
		
		if(dayAggLevel>2){
			dailyAggregatingPeriodInventoryData(config, pDateStr);
		}		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "DailyTaskOverall");
		sLogger.info("The daily overall data task on " + pDateStr + " is done.\n");	
	}	
}
