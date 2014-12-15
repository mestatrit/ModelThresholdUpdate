package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;

public class DailyGeneratingPredCTRList
{
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingPredCTRList.class);

	public void dailyCTRPeriodRollingEach(Configuration config, String pDateStr) throws Exception {		
		int hour = -1;
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);

		long time_s00 = System.currentTimeMillis();
		sLogger.info("\nThe daily predicted ctr task on " + pDateStr + " starts ...");
		DailyGeneratingPredCTR dailyCTR = new DailyGeneratingPredCTR();
		HourlyAggregationPredCTRRolling dPV = new HourlyAggregationPredCTRRolling();

		String ctrPredPipeNameStr = config.get("CTRPAggNames");
		String ctrAggFieldStr = config.get("CTRPAggFields");

		String ctrMVNameStr = config.get("CTRPMVNames");
		String ctrMVFieldStr = config.get("CTRPMVFields");
		ctrPredPipeNameStr = ctrPredPipeNameStr + ";" + ctrMVNameStr;
		ctrAggFieldStr = ctrAggFieldStr + ";" + ctrMVFieldStr;
		
		String[] ctrPredPipeNames = StringUtils.split(ctrPredPipeNameStr, ";");
		String[] ctrAggFields = StringUtils.split(ctrAggFieldStr, ";");
		for(int i=0; i<ctrPredPipeNames.length; i++){
			String infile_postfix = "_hourly";
			String outfile_postfix = "_daily";
			if(ctrPredPipeNames[i]!=null){
				sLogger.info("\nGenerating the daily predicted ctr on " + pDateStr + " ...");
				dailyCTR.dailyGeneratingPredCTR(config, ctrPredPipeNames[i], ctrAggFields[i], infile_postfix, outfile_postfix);
				sLogger.info("Generating the daily predicted ctr on " + pDateStr + " is done.");
			}
		}
		long time_s0 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s00, time_s0, "the daily aggregation");
		sLogger.info("The daily predicted ctr aggregation task on " + pDateStr + " is done.\n");	
		
		sLogger.info("\nThe periodical predicted ctr aggregation task on " + pDateStr + " starts ...");
		int numOfWeeks = config.getInt("NumOfWeeks", 4);
		int numOfPeriods = config.getInt("NumOfPeriods", 3);
		int interval1 = 1;
		int interval2 = 7*numOfPeriods*numOfWeeks;
		String infile_postfix = "_daily";
		String outfile_postfix = "_agg";
		if(numOfPeriods>0){
			sLogger.info("Generating the aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods ...");
			dPV.generatingPredCTRPeriodsEach(config, interval1, interval2, hour, ctrPredPipeNameStr, ctrAggFieldStr, infile_postfix, outfile_postfix);
			sLogger.info("The aggregation of periodical data on " + pDateStr + " over " + numOfPeriods + " periods is done.");	
		}	
		long time_s1 = System.currentTimeMillis();
		adoptUtils.loggingTimeUsed(time_s0, time_s1, "the periodical aggregation");
		sLogger.info("The peridical predicted ctr aggregation task on " + pDateStr + " is done.\n");	
		
		adoptUtils.loggingTimeUsed(time_s00, time_s1, "the whole daily list");
		sLogger.info("The daily predicted ctr task on " + pDateStr + " is done.\n");		
	}	
}
