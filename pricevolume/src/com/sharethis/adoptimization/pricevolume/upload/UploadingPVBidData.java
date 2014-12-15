package com.sharethis.adoptimization.pricevolume.upload;

import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.PVConstants;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


public class UploadingPVBidData
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
    
	private static final Logger sLogger = Logger.getLogger(UploadingPVBidData.class);
	//private int delayDay = 2;	
	
	public int uploadingPVBidData(Configuration config) throws Exception {
		try{
//			String poolName = "rtbDelivery";
			int daysKept = 90;
//			String outFilePath = "/user/hadoop/output/";
			String fileNamePrefix = "adgroup;adgroup_cr";
			String keyFieldsList = "adgroup_id;adgroup_id,creative_id";				

			String bidTableName = "price_volume_bid";
			String columnName = "date_";
			String bidFieldBase = "sum_bids, sum_nobids";
			boolean doesTableExist = true;
//			String exportPoolName = "rtbDelivery";
			sLogger.info("Entering the class ...");
			long time_s0 = System.currentTimeMillis();
			if(config==null){
				sLogger.info("config is null.");
				throw new Exception("config is null.");
			}else{
				sLogger.info("config:" + config.toString());
			}
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			sLogger.info("Starting to get the parameters.");
			int dayInterval = config.getInt("DayInterval", 7);
			String poolName = config.get("PoolNameAdminDate");
			sLogger.info("Pool Name: " + poolName);
			String exportPoolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + exportPoolName);

			String outFilePathDaily = config.get("OutFilePathDaily");
//			outFilePath = config.get("OutFilePath", outFilePath);
			sLogger.info("Output File Path Daily: " + outFilePathDaily);
			fileNamePrefix = config.get("PipeNameList", fileNamePrefix) + ";" 
					+ config.get("PipeNameBase"); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			int numOfDays = config.getInt("numOfDays", 7);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
		
			keyFieldsList = config.get("KeyFieldsList", keyFieldsList) + ";" 
					+ config.get("KeyFieldsBase");
			String[] keyFields = StringUtils.split(keyFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] bidFieldList = new String[keyFieldLen];
		    String[] bidFieldListAgg = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	bidFieldList[i] = "(" + keyFields[i] + ", " + bidFieldBase + ")";
		    	bidFieldListAgg[i] = "(" + keyFields[i] + ", " + bidFieldBase + ")";
		    	sLogger.info("Bid data field list: " + bidFieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
			bidTableName = config.get("BidTableName", bidTableName);
			sLogger.info("Bid Table Name: " + bidTableName);
			doesTableExist = config.getBoolean("DoesTableExist", true);
			String pDateStr = pvUtils.getPDateStr(new Date(), dayInterval);
			config.set("ProcessingDate", pDateStr);
			if(!doesTableExist){
				pvUtils.createBidTable(exportPoolName, bidTableName);
			}
			sLogger.info("Starting uploading the bid data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String[] keys = null;
			for(int i=0; i < fileNames.length; i++){	
				keys = StringUtils.split(keyFields[i], ",");
				pvUtils.deleteDataByDate(exportPoolName, bidTableName, columnName, pDateStr, keys);    
				String bidFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_bid_p_daily";
		        sLogger.info("File Name: " + bidFilePath);
				if(pvUtils.doesFileExist(bidFilePath)&&!pvUtils.isEmptyFile(bidFilePath)){
					config.set("SourcePath", bidFilePath);
					String property = bidTableName+bidFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + bidFilePath + " does not exist or is empty!");	
				}
			}
			pvUtils.updatingColumns(exportPoolName, bidTableName, columnName, pDateStr, "num_days", 1);
			sLogger.info("Ended uploading the daily bid data!\n");			
			
			for(int i=0; i < fileNames.length; i++){					    
				String bidFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_bid_p_agg";    
				sLogger.info("File Name: " + bidFilePath);
				if(pvUtils.doesFileExist(bidFilePath)&&!pvUtils.isEmptyFile(bidFilePath)){
					config.set("SourcePath", bidFilePath);
					String property = bidTableName+bidFieldListAgg[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + bidFilePath + " does not exist or is empty!");	
				}
			}
			pvUtils.updatingColumns(exportPoolName, bidTableName, columnName, pDateStr, "num_days", numOfDays);
			sLogger.info("Ended uploading the aggregated bid data!\n");					
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the bid data");
			sLogger.info("Starting to delete the bid data ...\n");
			daysKept = config.getInt("DaysKept", daysKept);
			pvUtils.deleteDataBeforeDate(exportPoolName, bidTableName, columnName, pDateStr, daysKept);
			sLogger.info("Ended deleting the bid data.\n");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}		
}
