package com.sharethis.adoptimization.clickthroughrate;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.clickthroughrate.upload.UploadingCTRData;
import com.sharethis.adoptimization.common.AdoptimizationUtils;


public class DailyGeneratingCTRList
{
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingCTRList.class);
	
	public void dailyCTRAggFromHourlyData(Configuration config, String pDateStr) throws Exception {
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		UploadingCTRData uploadingData = new UploadingCTRData();
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		DailyGeneratingCTR dailyCTR = new DailyGeneratingCTR();
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily ctr on " + pDateStr + " ...");
			dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("Generating the daily ctr on " + pDateStr + " is done.");
		}
		
		if(dayAggLevel>=0){		
			pipeNamesList = config.get("PipeNamesListWeekday");
			keyFieldsList = config.get("KeyFieldsListWeekday");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " ...");
				infile_postfix = "_hour_weekday";
				outfile_postfix = "_weekday";
				dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekday data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>0){
			pipeNamesList = config.get("PipeNamesListWeekly");
			keyFieldsList = config.get("KeyFieldsListWeekly");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " ...");
				infile_postfix = "_hour_weekly";
				outfile_postfix = "_weekly";
				dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekly data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>1){
			pipeNamesList = config.get("PipeNamesList4Weeks");
			keyFieldsList = config.get("KeyFieldsList4Weeks");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of multi-week data on " + pDateStr + " ...");
				infile_postfix = "_hour_4_weeks";
				outfile_postfix = "_4_weeks";
				dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of multi-week data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>2){
			pipeNamesList = config.get("PipeNamesListAgg");
			keyFieldsList = config.get("KeyFieldsListAgg");
			if(pipeNamesList!=null){	
				sLogger.info("Generating the aggregation of periodically data on " + pDateStr + " ...");
				infile_postfix = "_hour_agg";
				outfile_postfix = "_agg";
				dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of periodically data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevelMap>=0){
			String mapPipeNames = config.get("MapPipeNames");
			String mapKeys = config.get("MapKeys");
			if(mapPipeNames!=null){
				infile_postfix = "_hourly";
				outfile_postfix = "_daily";
				sLogger.info("\nGenerating the daily Mappinps on " + pDateStr + " ...");
				MappingsDailyGeneration mapDaily = new MappingsDailyGeneration();
				mapDaily.dailyGeneratingMappings(config, mapPipeNames, mapKeys, infile_postfix, outfile_postfix);
				sLogger.info("Generating the daily mappings on " + pDateStr + " is done.");
			}
		}
		
		uploadingData.uploadingData(config, pDateStr, dayAggLevel, dayAggLevelMap);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");		
	}

	public void dailyCTRAggFromHourlyBase(Configuration config, String pDateStr) throws Exception {
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		UploadingCTRData uploadingData = new UploadingCTRData();
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		DailyGeneratingCTR dailyCTR = new DailyGeneratingCTR();
		String basePipeName = config.get("BasePipeNameDaily");
		String baseKeyFields = config.get("BaseKeyFieldsDaily");
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily ctr on " + pDateStr + " ...");
			dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("Generating the daily ctr on " + pDateStr + " is done.");
		}

		if(dayAggLevel>=0){		
			basePipeName = config.get("BasePipeNameWeekday");
			baseKeyFields = config.get("BaseKeyFieldsWeekday");
			pipeNamesList = config.get("PipeNamesListWeekday");
			keyFieldsList = config.get("KeyFieldsListWeekday");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " ...");
				infile_postfix = "_hour_weekday";
				outfile_postfix = "_weekday";
				dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekday data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>0){
			basePipeName = config.get("BasePipeNameWeekly");
			baseKeyFields = config.get("BaseKeyFieldsWeekly");
			pipeNamesList = config.get("PipeNamesListWeekly");
			keyFieldsList = config.get("KeyFieldsListWeekly");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " ...");
				infile_postfix = "_hour_weekly";
				outfile_postfix = "_weekly";
				dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekly data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>1){
			basePipeName = config.get("BasePipeName4Weeks");
			baseKeyFields = config.get("BaseKeyFields4Weeks");
			pipeNamesList = config.get("PipeNamesList4Weeks");
			keyFieldsList = config.get("KeyFieldsList4Weeks");
			if(pipeNamesList!=null){
				sLogger.info("Generating the aggregation of multi-week data on " + pDateStr + " ...");
				infile_postfix = "_hour_4_weeks";
				outfile_postfix = "_4_weeks";
				dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of multi-week data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevel>2){
			basePipeName = config.get("BasePipeNameAgg");
			baseKeyFields = config.get("BaseKeyFieldsAgg");
			pipeNamesList = config.get("PipeNamesListAgg");
			keyFieldsList = config.get("KeyFieldsListAgg");
			if(pipeNamesList!=null){	
				sLogger.info("Generating the aggregation of periodically data on " + pDateStr + " ...");
				infile_postfix = "_hour_agg";
				outfile_postfix = "_agg";
				dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of periodically data on " + pDateStr + " is done.");			
			}
		}
		
		if(dayAggLevelMap>=0){
			String mapPipeNames = config.get("MapPipeNames");
			String mapKeys = config.get("MapKeys");
			if(mapPipeNames!=null){
				infile_postfix = "_hourly";
				outfile_postfix = "_daily";
				sLogger.info("\nGenerating the daily Mappinps on " + pDateStr + " ...");
				MappingsDailyGeneration mapDaily = new MappingsDailyGeneration();
				mapDaily.dailyGeneratingMappings(config, mapPipeNames, mapKeys, infile_postfix, outfile_postfix);
				sLogger.info("Generating the daily mappings on " + pDateStr + " is done.");
			}
		}
		
		uploadingData.uploadingData(config, pDateStr, dayAggLevel, dayAggLevelMap);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");		
	}
		
	public void dailyCTRAggFromDailyData(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		UploadingCTRData uploadingData = new UploadingCTRData();
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		DailyGeneratingCTR dailyCTR = new DailyGeneratingCTR();
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily ctr on " + pDateStr + " ...");
			dailyCTR.dailyGeneratingCTREach(config, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("Generating the daily ctr on " + pDateStr + " is done.");
		}
				
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		int interval = 1;
		if(dayAggLevel>=0){
			infile_postfix = null;
			outfile_postfix = null;
			int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
			pipeNamesList = config.get("PipeNamesListWeekday");
			keyFieldsList = config.get("KeyFieldsListWeekday");
			if(numOfWeeksWD>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = "_daily";
				outfile_postfix = "_weekday";
				sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeksWD, interval, hour, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
			}
		}

		if(dayAggLevel>0){
			pipeNamesList = config.get("PipeNamesListWeekly");
			keyFieldsList = config.get("KeyFieldsListWeekly");
			int numOfDays = config.getInt("NumOfDays", 7);
			if(numOfDays>0&&pipeNamesList!=null){
				interval = 1;
				infile_postfix = "_daily";
				outfile_postfix = "_weekly";
				sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
				dPV.generatingCTRPeriodsEach(config, numOfDays, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
			}
		}

		if(dayAggLevel>1){
			pipeNamesList = config.get("PipeNamesList4Weeks");
			keyFieldsList = config.get("KeyFieldsList4Weeks");
			int numOfWeeks = config.getInt("NumOfWeeks", 4);
			if(numOfWeeks>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_" + numOfWeeks + "_weeks";
				sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeks, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
			}
		}
		
		if(dayAggLevel>2){
			pipeNamesList = config.get("PipeNamesListAgg");
			keyFieldsList = config.get("KeyFieldsListAgg");
			int numOfPeriods = config.getInt("NumOfPeriods", 3);
			if(numOfPeriods>0&&pipeNamesList!=null){
				interval = 28;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_agg";
				sLogger.info("Generating the aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
				dPV.generatingCTRPeriodsEach(config, numOfPeriods, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
			}
		}		

		if(dayAggLevelMap>=0){
			String mapPipeNames = config.get("MapPipeNames");
			String mapKeys = config.get("MapKeys");
			if(mapPipeNames!=null){
				infile_postfix = "_hourly";
				outfile_postfix = "_daily";
				sLogger.info("\nGenerating the daily Mappinps on " + pDateStr + " ...");
				MappingsDailyGeneration mapDaily = new MappingsDailyGeneration();
				mapDaily.dailyGeneratingMappings(config, mapPipeNames, mapKeys, infile_postfix, outfile_postfix);
				sLogger.info("Generating the daily mappings on " + pDateStr + " is done.");
			}
		}

		uploadingData.uploadingData(config, pDateStr, dayAggLevel, dayAggLevelMap);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");	
	}	
	
	public void dailyCTRDayAllFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr aggregation on " + pDateStr + " starts ...");

		dailyCTRDayFromDailyBase(config, pDateStr);
		dailyCTRMapFromDailyData(config, pDateStr);		
		dailyCTRWeekDayFromDailyBase(config, pDateStr);			
		uploadingDailyCTR(config, pDateStr);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the daily aggregation");
		sLogger.info("The daily ctr aggregation on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRDayFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr aggregation on " + pDateStr + " starts ...");
		DailyGeneratingCTR dailyCTR = new DailyGeneratingCTR();
		String basePipeName = config.get("BasePipeNameDaily");
		String baseKeyFields = config.get("BaseKeyFieldsDaily");
		String pipeNamesList = config.get("PipeNamesListDaily");
		String keyFieldsList = config.get("KeyFieldsListDaily");
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";
		int perfFlag = config.getInt("PerfFlag", 0);
		if(pipeNamesList!=null){
			sLogger.info("\nGenerating the daily ctr on " + pDateStr + " ...");
			if(perfFlag==0)
				dailyCTR.dailyGeneratingCTR(config, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
			else
				dailyCTR.dailyGeneratingCTRAll(config, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);

			sLogger.info("Generating the daily ctr on " + pDateStr + " is done.");
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the daily aggregation");
		sLogger.info("The daily ctr aggregation on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRWeekDayFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe weekday ctr aggregation on " + pDateStr + " starts ...");		
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
		String basePipeName = config.get("BasePipeNameWeekday");
		String baseKeyFields = config.get("BaseKeyFieldsWeekday");
		String pipeNamesList = config.get("PipeNamesListWeekday");
		String keyFieldsList = config.get("KeyFieldsListWeekday");
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfWeeksWD>0&&pipeNamesList!=null){
			int interval = 7;
			String infile_postfix = "_daily";
			String outfile_postfix = "_weekday";
			sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
			if(perfFlag==0)
				dPV.generatingCTRPeriods(config, numOfWeeksWD, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			else
				dPV.generatingCTRPeriodsAll(config, numOfWeeksWD, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekday aggregation");
		sLogger.info("The weekday ctr aggregation on " + pDateStr + " is done.\n");	
	}

	public void uploadingDailyCTR(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		UploadingCTRData uploadingData = new UploadingCTRData();
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nUploading the daily ctr on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		
		uploadingData.uploadingData(config, pDateStr, dayAggLevel, dayAggLevelMap);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "uploading the daily ctr");
		sLogger.info("Uploading daily ctr on " + pDateStr + " is done.\n");	
	}
	
	public void dailyCTRWeekFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		long time_s0 = System.currentTimeMillis();
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		sLogger.info("\nThe weekly ctr aggregation task on " + pDateStr + " starts ...");
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesListWeekly");
		String keyFieldsList = config.get("KeyFieldsListWeekly");
		String infile_postfix = "_daily";
		String outfile_postfix = "_weekly";
		int interval = 1;
		int numOfDays = config.getInt("NumOfDays", 7);
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfDays>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
			if(perfFlag==0)
				dPV.generatingCTRPeriods(config, numOfDays, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			else
				dPV.generatingCTRPeriodsAll(config, numOfDays, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekly aggregation");
		sLogger.info("The weekly ctr aggregation task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyCTR4WeeksFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		sLogger.info("\nThe 4 week ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String basePipeName = config.get("BasePipeName4Weeks");
		String baseKeyFields = config.get("BaseKeyFields4Weeks");
		String pipeNamesList = config.get("PipeNamesList4Weeks");
		String keyFieldsList = config.get("KeyFieldsList4Weeks");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		int interval = 7;
		String infile_postfix = "_weekly";
		String outfile_postfix = "_" + numOfWeeks + "_weeks";
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfWeeks>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
			if(perfFlag==0)
				dPV.generatingCTRPeriods(config, numOfWeeks, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			else
				dPV.generatingCTRPeriodsAll(config, numOfWeeks, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the 4 week aggregation");
		sLogger.info("The 4 week ctr aggregation task on " + pDateStr + " is done.\n");	
	}
	
	public void dailyCTRPeriodFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		sLogger.info("\nThe periodical ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String basePipeName = config.get("BasePipeNameAgg");
		String baseKeyFields = config.get("BaseKeyFieldsAgg");
		String pipeNamesList = config.get("PipeNamesListAgg");
		String keyFieldsList = config.get("KeyFieldsListAgg");
		int numOfPeriods = config.getInt("NumOfPeriods", 3);
		int interval = 28;
		String infile_postfix = "_4_weeks";
		String outfile_postfix = "_agg";
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfPeriods>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
			if(perfFlag==0)
				dPV.generatingCTRPeriods(config, numOfPeriods, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			else
				dPV.generatingCTRPeriodsAll(config, numOfPeriods, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
		}				
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the periodical aggregation");
		sLogger.info("The peridical ctr aggregation task on " + pDateStr + " is done.\n");	
	}	
	
	public void dailyCTRMapFromDailyData(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		UploadingCTRData uploadingData = new UploadingCTRData();
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		String infile_postfix = "_hourly";
		String outfile_postfix = "_daily";				

		if(dayAggLevelMap>=0){
			String mapPipeNames = config.get("MapPipeNames");
			String mapKeys = config.get("MapKeys");
			if(mapPipeNames!=null){
				sLogger.info("\nGenerating the daily Mappinps on " + pDateStr + " ...");
				MappingsDailyGeneration mapDaily = new MappingsDailyGeneration();
				mapDaily.dailyGeneratingMappings(config, mapPipeNames, mapKeys, infile_postfix, outfile_postfix);
				sLogger.info("Generating the daily mappings on " + pDateStr + " is done.");
			}
		}

		uploadingData.uploadingMapData(config, pDateStr, dayAggLevelMap);
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");	
	}	

	
	public void dailyCTRAggFromDailyDataEx(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe extra daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		String pipeNamesList = null;
		String keyFieldsList = null;
		String infile_postfix = null;
		String outfile_postfix = null;
		int interval = 1;
				
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		int numOfWeeks = config.getInt("NumOfWeeks", 4);

		if(dayAggLevel>1){
			pipeNamesList = config.get("PipeNamesList4WeeksEx");
			keyFieldsList = config.get("KeyFieldsList4WeeksEx");
			if(numOfWeeks>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = "_weekly";
				outfile_postfix = "_" + numOfWeeks + "_weeks";
				sLogger.info("Generating the extra aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeks, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The extra aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
			}
		}
		
		if(dayAggLevel>2){
			pipeNamesList = config.get("PipeNamesListAggEx");
			keyFieldsList = config.get("KeyFieldsListAggEx");
			int numOfPeriods = config.getInt("NumOfPeriods", 3);
			if(numOfPeriods>0&&pipeNamesList!=null){
				interval = 28;
				infile_postfix = "_" + numOfWeeks + "_weeks";
				outfile_postfix = "_agg";
				sLogger.info("Generating the extra aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
				dPV.generatingCTRPeriodsEach(config, numOfPeriods, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The extra aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
			}
		}		

		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole extra daily list");
		sLogger.info("The extra daily ctr task on " + pDateStr + " is done.\n");	
	}		
	
	public void dailyCTRAggFromDailyBase(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		dailyCTRDayFromDailyBase(config, pDateStr);
		if(dayAggLevel>=0)
			dailyCTRWeekFromDailyBase(config, pDateStr);	
		
		if(dayAggLevel>0)
			dailyCTRWeekDayFromDailyBase(config, pDateStr);	

		if(dayAggLevel>1)
			dailyCTR4WeeksFromDailyBase(config, pDateStr);			
		
		if(dayAggLevel>2)
			dailyCTRPeriodFromDailyBase(config, pDateStr);			
		
		if(dayAggLevelMap>=0)
			dailyCTRMapFromDailyData(config, pDateStr);
		
		uploadingDailyCTR(config, pDateStr);
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");	
	}	

	public void dailyCTRPeriodFromDailyBaseRolling(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe periodical ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String basePipeName = config.get("BasePipeNameAgg");
		String baseKeyFields = config.get("BaseKeyFieldsAgg");
		String pipeNamesList = config.get("PipeNamesListAgg");
		String keyFieldsList = config.get("KeyFieldsListAgg");
		int numOfPeriods = config.getInt("NumOfPeriods", 3);
		int interval1 = 1;
		int interval2 = 84;
		String infile_postfix = "_daily";
		String outfile_postfix = "_agg";
		if(numOfPeriods>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
			dPV.generatingCTRRolling(config, interval1, interval2, hour, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, 
					infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
		}				
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the periodical aggregation");
		sLogger.info("The peridical ctr aggregation task on " + pDateStr + " is done.\n");	
	}	

	public void dailyCTR4WeeksFromDailyBaseRolling(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe 4 week ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String basePipeName = config.get("BasePipeName4Weeks");
		String baseKeyFields = config.get("BaseKeyFields4Weeks");
		String pipeNamesList = config.get("PipeNamesList4Weeks");
		String keyFieldsList = config.get("KeyFieldsList4Weeks");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		int interval1 = 1;
		int interval2 = 28;
		String infile_postfix = "_daily";
		String outfile_postfix = "_" + numOfWeeks + "_weeks";
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfWeeks>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
			dPV.generatingCTRRolling(config, interval1, interval2, hour, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, 
					infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the 4 week aggregation");
		sLogger.info("The 4 week ctr aggregation task on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRWeekFromDailyBaseRolling(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		long time_s0 = System.currentTimeMillis();
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe weekly ctr aggregation task on " + pDateStr + " starts ...");
		String basePipeName = config.get("BasePipeNameWeekly");
		String baseKeyFields = config.get("BaseKeyFieldsWeekly");
		String pipeNamesList = config.get("PipeNamesListWeekly");
		String keyFieldsList = config.get("KeyFieldsListWeekly");
		String infile_postfix = "_daily";
		String outfile_postfix = "_weekly";
		int interval1 = 1;
		int interval2 = 7;
		int numOfDays = config.getInt("NumOfDays", 7);
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfDays>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
			dPV.generatingCTRRolling(config, interval1, interval2, hour, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, 
					infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekly aggregation");
		sLogger.info("The weekly ctr aggregation task on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRWeekDayFromDailyBaseRolling(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe weekday ctr aggregation on " + pDateStr + " starts ...");		
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
		String basePipeName = config.get("BasePipeNameWeekday");
		String baseKeyFields = config.get("BaseKeyFieldsWeekday");
		String pipeNamesList = config.get("PipeNamesListWeekday");
		String keyFieldsList = config.get("KeyFieldsListWeekday");
		int perfFlag = config.getInt("PerfFlag", 0);
		if(numOfWeeksWD>0&&pipeNamesList!=null){
			int interval1 = 7;
			int interval2 = 28;
			String infile_postfix = "_daily";
			String outfile_postfix = "_weekday";
			sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
			dPV.generatingCTRRolling(config, interval1, interval2, hour, basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, 
					infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekday aggregation");
		sLogger.info("The weekday ctr aggregation on " + pDateStr + " is done.\n");	
	}
	
	public void dailyCTRAggFromDailyBaseRolling(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		dailyCTRDayFromDailyBase(config, pDateStr);
		if(dayAggLevel>=0)
			dailyCTRWeekFromDailyBaseRolling(config, pDateStr);	
		
		if(dayAggLevel>0)
			dailyCTRWeekDayFromDailyBaseRolling(config, pDateStr);	

		if(dayAggLevel>1)
			dailyCTR4WeeksFromDailyBaseRolling(config, pDateStr);			
		
		if(dayAggLevel>2)
			dailyCTRPeriodFromDailyBaseRolling(config, pDateStr);			
		
		if(dayAggLevelMap>=0){
			dailyCTRMapFromDailyData(config, pDateStr);
		}
		
		uploadingDailyCTR(config, pDateStr);
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");	
	}	
	
	public void dailyCTRPeriodRollingEach(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe periodical ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String pipeNamesList = config.get("PipeNamesListAgg");
		String keyFieldsList = config.get("KeyFieldsListAgg");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		int numOfPeriods = config.getInt("NumOfPeriods", 3);
		int interval1 = 1;
		int interval2 = 7*numOfPeriods*numOfWeeks;
		String infile_postfix = "_daily";
		String outfile_postfix = "_agg";
		if(numOfPeriods>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
			dPV.generatingCTRPeriodsEach(config, interval1, interval2, hour, pipeNamesList, keyFieldsList, infile_postfix, 
					outfile_postfix);
			sLogger.info("The aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
		}				
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the periodical aggregation");
		sLogger.info("The peridical ctr aggregation task on " + pDateStr + " is done.\n");	
	}	

	public void dailyCTR4WeeksRollingEach(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe 4 week ctr aggregation task on " + pDateStr + " starts ...");
		long time_s0 = System.currentTimeMillis();
		String pipeNamesList = config.get("PipeNamesList4Weeks");
		String keyFieldsList = config.get("KeyFieldsList4Weeks");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		int interval1 = 1;
		int interval2 = 7*numOfWeeks;
		String infile_postfix = "_daily";
		String outfile_postfix = "_" + numOfWeeks + "_weeks";
		if(numOfWeeks>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
			dPV.generatingCTRPeriodsEach(config, interval1, interval2, hour, pipeNamesList, keyFieldsList, infile_postfix, 
					outfile_postfix);
			sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the 4 week aggregation");
		sLogger.info("The 4 week ctr aggregation task on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRWeekRollingEach(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		long time_s0 = System.currentTimeMillis();
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		sLogger.info("\nThe weekly ctr aggregation task on " + pDateStr + " starts ...");
		String pipeNamesList = config.get("PipeNamesListWeekly");
		String keyFieldsList = config.get("KeyFieldsListWeekly");
		String infile_postfix = "_daily";
		String outfile_postfix = "_weekly";
		int interval1 = 1;
		int interval2 = 7;
		int numOfDays = config.getInt("NumOfDays", 7);
		if(numOfDays>0&&pipeNamesList!=null){
			sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
			dPV.generatingCTRPeriodsEach(config, interval1, interval2, hour, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekly aggregation");
		sLogger.info("The weekly ctr aggregation task on " + pDateStr + " is done.\n");	
	}

	public void dailyCTRWeekDayRollingEach(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe weekday ctr aggregation on " + pDateStr + " starts ...");		
		HourlyAggregationCTRRolling dPV = new HourlyAggregationCTRRolling();
		int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
		String pipeNamesList = config.get("PipeNamesListWeekday");
		String keyFieldsList = config.get("KeyFieldsListWeekday");
		if(numOfWeeksWD>0&&pipeNamesList!=null){
			int interval1 = 7;
			int interval2 = 7*(numOfWeeksWD+1);
			String infile_postfix = "_daily";
			String outfile_postfix = "_weekday";
			sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
			dPV.generatingCTRPeriodsEach(config, interval1, interval2, hour, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
		}
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the weekday aggregation");
		sLogger.info("The weekday ctr aggregation on " + pDateStr + " is done.\n");	
	}
	
	public void dailyCTRAggRollingEach(Configuration config, String pDateStr) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		sLogger.info("\nThe daily ctr task on " + pDateStr + " starts ...");
		int dayAggLevel = config.getInt("DayAggLevel", -1);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", -1);
		dailyCTRDayFromDailyBase(config, pDateStr);
		if(dayAggLevel>=0)
			dailyCTRWeekRollingEach(config, pDateStr);	
		
		if(dayAggLevel>0)
			dailyCTRWeekDayRollingEach(config, pDateStr);	

		if(dayAggLevel>1)
			dailyCTR4WeeksRollingEach(config, pDateStr);			
		
		if(dayAggLevel>2)
			dailyCTRPeriodRollingEach(config, pDateStr);			
		
		if(dayAggLevelMap>=0){
			dailyCTRMapFromDailyData(config, pDateStr);
		}
		
		uploadingDailyCTR(config, pDateStr);
		
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole daily list");
		sLogger.info("The daily ctr task on " + pDateStr + " is done.\n");	
	}	
}
