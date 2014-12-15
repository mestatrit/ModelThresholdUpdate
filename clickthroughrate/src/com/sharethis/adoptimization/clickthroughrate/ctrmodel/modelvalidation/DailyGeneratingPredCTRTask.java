package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;


public class DailyGeneratingPredCTRTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingPredCTRTask.class);
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
		
		sLogger.info("\nAggregating the daily mv ctr task on " + pDateStr + " starts ...");
		DailyGeneratingPredCTRList dailyCTRList = new DailyGeneratingPredCTRList();
		dailyCTRList.dailyCTRPeriodRollingEach(config, pDateStr);
		sLogger.info("\nAggregating the daily mv ctr task on " + pDateStr + " is done.");
		
		sLogger.info("\nUploading daily mv ctr task on " + pDateStr + " starts ...");
		UploadingMVCTRData ulMVCTR = new UploadingMVCTRData();
		ulMVCTR.uploadingCTRDaily(config);
		sLogger.info("\nUploading daily mv ctr task on " + pDateStr + " is done.");
		return 0;		
	}				
}
