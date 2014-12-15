package com.sharethis.adoptimization.clickthroughrate.upload;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;

public class UploadingCTRTask extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(UploadingCTRTask.class);
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
		sLogger.info("Starting uploading ctr data ...\n");
		UploadingCTRData uploadingData = new UploadingCTRData();
		int dayAggLevel = config.getInt("DayAggLevel", 0);
		int dayAggLevelMap = config.getInt("DayAggLevelMap", 0);
		uploadingData.uploadingData(config, pDateStr, dayAggLevel, dayAggLevelMap);		
		sLogger.info("Uploading ctr data is done.\n");
		return 0;		
	}				
}
