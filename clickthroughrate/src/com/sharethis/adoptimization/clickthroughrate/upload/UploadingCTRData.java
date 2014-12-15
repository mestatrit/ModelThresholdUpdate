package com.sharethis.adoptimization.clickthroughrate.upload;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;

public class UploadingCTRData
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
	private static final Logger sLogger = Logger.getLogger(UploadingCTRData.class);
	
	public void uploadingData(Configuration config, String pDateStr, int dayAggLevel, int dayAggLevelMap) throws Exception {
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		String poolName = config.get("PoolName");
		Date pDate = sdf.parse(pDateStr);
		String keyValDaily = "ClickThroughRateDaily";
		boolean uploadingFlag = config.getBoolean("UploadingFlag", false);
		if(uploadingFlag){
			if(config.get("PipeNamesListDaily")!=null){
				UploadingCTRDailyData upD = new UploadingCTRDailyData();
				sLogger.info("Starting uploading the daily ctr on " + pDateStr + " ...\n");
				upD.uploadingCTRDaily(config);
				sLogger.info("Uploading the daily ctr on " + pDateStr + " is done.\n");
				Date adminDateCtr = adoptUtils.getAdminDate(poolName, keyValDaily);
				if(adminDateCtr==null || pDate.after(adminDateCtr)){
					sLogger.info("Starting updating the daily ctr into pv curve table on " + pDateStr + " ...\n");
					upD.updatingCTRDataIntoPVTable(config);
					sLogger.info("Updating the daily ctr into pv curve table on " + pDateStr + " is done.\n");
					adoptUtils.upsertAdminDate(poolName, keyValDaily, pDate);
				}
			}
			if(config.get("PipeNamesListWeekday")!=null&&dayAggLevel>=0){			
				sLogger.info("Starting uploading the weekday ctr data ...\n");
				UploadingCTRWeekdayData upWD = new UploadingCTRWeekdayData();
				upWD.uploadingCTRWeekday(config);
				sLogger.info("Uploading the weekday ctr data is done.\n");
				String keyVal = "ClickThroughRateAll";
				Date adminDate = adoptUtils.getAdminDate(poolName, keyVal);
				if(adminDate==null)
					adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
				else
					if(pDate.after(adminDate))
						adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
			}
			
			if(config.get("MapPipeNames")!=null&&dayAggLevelMap>=1){			
				sLogger.info("Starting uploading the daily setting-vertical mapping data ...\n");
				UploadingMappingsData upMapping = new UploadingMappingsData();
				upMapping.uploadingMappginsDaily(config);
				sLogger.info("Uploading the daily setting-vertical mapping data is done.\n");
				String keyVal = "AdSpaceCategory";
				Date adminDate = adoptUtils.getAdminDate(poolName, keyVal);
				if(adminDate==null)
					adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
				else
					if(pDate.after(adminDate))
						adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
			}
		}
	}	

	public void uploadingMapData(Configuration config, String pDateStr, int dayAggLevelMap) throws Exception {
		AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
		String poolName = config.get("PoolName");
		Date pDate = sdf.parse(pDateStr);
		boolean uploadingFlag = config.getBoolean("UploadingFlag", false);
		if(uploadingFlag){			
			if(config.get("MapPipeNames")!=null&&dayAggLevelMap>=1){			
				sLogger.info("Starting uploading the daily setting-vertical mapping data ...\n");
				UploadingMappingsData upMapping = new UploadingMappingsData();
				upMapping.uploadingMappginsDaily(config);
				sLogger.info("Uploading the daily setting-vertical mapping data is done.\n");
				String keyVal = "AdSpaceCategory";
				Date adminDate = adoptUtils.getAdminDate(poolName, keyVal);
				if(adminDate==null)
					adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
				else
					if(pDate.after(adminDate))
						adoptUtils.upsertAdminDate(poolName, keyVal, pDate);
			}
		}
	}	
}
