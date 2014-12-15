package com.sharethis.adoptimization.clickthroughrate;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.CTRConstants;


public class HourlyGeneratingCTRList
{
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingCTRList.class);
	
	public void hourlyCTRAggFromHourlyData(Configuration config, int hour) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		String pDateStr = config.get("ProcessingDate");
		String hourFolder = CTRConstants.hourFolders[hour];
		sLogger.info("\nStarting the hourly ctr task of hour " + hourFolder + " ...");
		HourlyGeneratingCTR hgCTR = new HourlyGeneratingCTR();
		sLogger.info("\nStarting the comutation of hourly CTR at hour " + hourFolder + " on " + pDateStr + " ...");
		hgCTR.generatingCTRHourly(config, hour);
		sLogger.info("The computation of hourly CTR at hour " + hourFolder + " on " + pDateStr + " is done.\n");
				
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		int interval = 1;
		String infile_postfix = null;
		String outfile_postfix = null;
		String pipeNamesList = null;
		String keyFieldsList = null;
		int hourAggLevel = config.getInt("HourAggLevel", -1);
		if(hourAggLevel>=0){
			int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
			interval = 1;
			pipeNamesList = config.get("PipeNamesListWeekday");
			keyFieldsList = config.get("KeyFieldsListWeekday");
			if(numOfWeeksWD>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = "_hourly";
				outfile_postfix = "_hour_weekday";
				sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeksWD, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
			}
		}
		if(hourAggLevel>0){
			pipeNamesList = config.get("PipeNamesListWeekly");
			keyFieldsList = config.get("KeyFieldsListWeekly");
			int numOfDays = config.getInt("NumOfDays", 7);
			if(numOfDays>0&&pipeNamesList!=null){
				interval = 1;
				infile_postfix = "_hourly";
				outfile_postfix = "_hour_weekly";
				sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
				dPV.generatingCTRPeriodsEach(config, numOfDays, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
			}
		}
		if(hourAggLevel>1){
			pipeNamesList = config.get("PipeNamesList4Weeks");
			keyFieldsList = config.get("KeyFieldsList4Weeks");
			int numOfWeeks = config.getInt("NumOfWeeks", 4);
			if(numOfWeeks>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_" + numOfWeeks + "_weeks";
				sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeks, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
			}
		}
		if(hourAggLevel>2){
			pipeNamesList = config.get("PipeNamesListAgg");
			keyFieldsList = config.get("KeyFieldsListAgg");
			int numOfPeriods = config.getInt("NumOfPeriods", 3);
			if(numOfPeriods>0&&pipeNamesList!=null){
				interval = 28;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_agg";
				sLogger.info("Generating the aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods ...");
				dPV.generatingCTRPeriodsEach(config, numOfPeriods, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
			}
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole hourly list");
		sLogger.info("The hourly ctr task at hour " + hourFolder + " is done.\n");	
	}
	
	public void hourlyCTRAggFromHourlyBase(Configuration config, int hour) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		String pDateStr = config.get("ProcessingDate");
		String hourFolder = CTRConstants.hourFolders[hour];
		sLogger.info("\nStarting the hourly ctr task of hour " + hourFolder + " ...");
		HourlyGeneratingCTR hgCTR = new HourlyGeneratingCTR();
		sLogger.info("\nStarting the comutation of hourly CTR at hour " + hourFolder + " on " + pDateStr + " ...");
		hgCTR.generatingCTRHourlyNew(config, hour);
		sLogger.info("The computation of hourly CTR at hour " + hourFolder + " on " + pDateStr + " is done.\n");
				
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		String basePipeName = null;
		String baseKeyFields = null;
		String infile_postfix = null;
		String outfile_postfix = null;
		String pipeNamesList = null;
		String keyFieldsList = null;
		int interval = 1;
		int hourAggLevel = config.getInt("HourAggLevel", -1);
		if(hourAggLevel>=0){
			int numOfWeeksWD = config.getInt("NumOfWeeksWD", 4);
			interval = 1;
			basePipeName = config.get("BasePipeNameWeekday");
			baseKeyFields = config.get("BaseKeyFieldsWeekday");
			pipeNamesList = config.get("PipeNamesListWeekday");
			keyFieldsList = config.get("KeyFieldsListWeekday");
			if(numOfWeeksWD>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = "_hourly";
				outfile_postfix = "_hour_weekday";
				sLogger.info("Generating the aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks ...");
				dPV.generatingCTRPeriods(config, numOfWeeksWD, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekday data on " + pDateStr + " over " + numOfWeeksWD + " weeks is done.");			
			}
		}
		if(hourAggLevel>0){
			basePipeName = config.get("BasePipeNameWeekly");
			baseKeyFields = config.get("BaseKeyFieldsWeekly");
			pipeNamesList = config.get("PipeNamesListWeekly");
			keyFieldsList = config.get("KeyFieldsListWeekly");
			int numOfDays = config.getInt("NumOfDays", 7);
			if(numOfDays>0&&pipeNamesList!=null){
				interval = 1;
				infile_postfix = "_hourly";
				outfile_postfix = "_hour_weekly";
				sLogger.info("Generating the aggregation of daily data on " + pDateStr + " over " + numOfDays + " days ...");
				dPV.generatingCTRPeriods(config, numOfDays, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of daily data on " + pDateStr + " over " + numOfDays + " days is done.");			
			}
		}
		if(hourAggLevel>1){
			basePipeName = config.get("BasePipeName4Weeks");
			baseKeyFields = config.get("BaseKeyFields4Weeks");
			pipeNamesList = config.get("PipeNamesList4Weeks");
			keyFieldsList = config.get("KeyFieldsList4Weeks");
			int numOfWeeks = config.getInt("NumOfWeeks", 4);
			if(numOfWeeks>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_" + numOfWeeks + "_weeks";
				sLogger.info("Generating the aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
				dPV.generatingCTRPeriods(config, numOfWeeks, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
			}
		}
		if(hourAggLevel>2){
			basePipeName = config.get("BasePipeNameAgg");
			baseKeyFields = config.get("BaseKeyFieldsAgg");
			pipeNamesList = config.get("PipeNamesListAgg");
			keyFieldsList = config.get("KeyFieldsListAgg");
			int numOfPeriods = config.getInt("NumOfPeriods", 3);
			if(numOfPeriods>0&&pipeNamesList!=null){
				interval = 28;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_agg";
				sLogger.info("Generating the aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods ...");
				dPV.generatingCTRPeriods(config, numOfPeriods, interval, hour, basePipeName, baseKeyFields, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
			}
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole hourly list");
		sLogger.info("The hourly ctr task at hour " + hourFolder + " is done.\n");	
	}	
	
	public void hourlyCTRAggFromHourlyDataEx(Configuration config, int hour) throws Exception {		
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		long time_s0 = System.currentTimeMillis();
		String pDateStr = config.get("ProcessingDate");
		String hourFolder = CTRConstants.hourFolders[hour];
		sLogger.info("\nStarting the extra hourly ctr task of hour " + hourFolder + " ...");
				
		HourlyAggregationCTR dPV = new HourlyAggregationCTR();
		int interval = 1;
		String infile_postfix = null;
		String outfile_postfix = null;
		String pipeNamesList = null;
		String keyFieldsList = null;
		int hourAggLevel = config.getInt("HourAggLevel", -1);
		if(hourAggLevel>1){
			pipeNamesList = config.get("PipeNamesList4WeeksEx");
			keyFieldsList = config.get("KeyFieldsList4WeeksEx");
			int numOfWeeks = config.getInt("NumOfWeeks", 4);
			if(numOfWeeks>0&&pipeNamesList!=null){
				interval = 7;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_" + numOfWeeks + "_weeks";
				sLogger.info("Generating the extra aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks ...");
				dPV.generatingCTRPeriodsEach(config, numOfWeeks, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The extra aggregation of weekly data on " + pDateStr + " over " + numOfWeeks + " weeks is done.");	
			}
		}
		if(hourAggLevel>2){
			pipeNamesList = config.get("PipeNamesListAggEx");
			keyFieldsList = config.get("KeyFieldsListAggEx");
			int numOfPeriods = config.getInt("NumOfPeriods", 3);
			if(numOfPeriods>0&&pipeNamesList!=null){
				interval = 28;
				infile_postfix = outfile_postfix;
				outfile_postfix = "_hour_agg";
				sLogger.info("Generating the extra aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods ...");
				dPV.generatingCTRPeriodsEach(config, numOfPeriods, interval, hour, pipeNamesList, 
						keyFieldsList, infile_postfix, outfile_postfix);
				sLogger.info("The extra aggregation of periodically data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
			}
		}
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the whole extra hourly list");
		sLogger.info("The extra hourly ctr task at hour " + hourFolder + " is done.\n");	
	}
}
