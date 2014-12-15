package com.sharethis.adoptimization.clickthroughrate.upload;

import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;

public class UploadingMappingsData
{
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
	private static final Logger sLogger = Logger.getLogger(UploadingMappingsData.class);

	public int uploadingMappginsDaily(Configuration config) throws Exception {
		try{
//			String poolName = "rtb";
//			String outFilePath = "/user/root/campaign_analytics/ctr/";
			String fileNamePrefix = "adgroup";
			String keyFieldsList = "adgroup_id";				

			String tableName = "AdSpaceCategory";
			String columnName = "time_modified";
			String fieldBase = "weight";
			sLogger.info("Entering the class ...");
			long time_s0 = System.currentTimeMillis();
			if(config==null){
				sLogger.info("config is null.");
				throw new Exception("config is null.");
			}else{
				sLogger.info("config:" + config.toString());
			}
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("Starting to get the parameters.");
			String poolName = config.get("RTBPoolName");
			sLogger.info("Export to MySql Pool Name: " + poolName);

			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("Output File Path DSaily: " + outFilePathDaily);
			fileNamePrefix = config.get("LoadingMappingName", fileNamePrefix); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
			else{
				sLogger.info("fileNamePrefix is null.");
				throw new Exception("fileNamePrefix is null.");
			}
		
			keyFieldsList = config.get("LoadingMappingKey", keyFieldsList);
			String[] keyFields = StringUtils.split(keyFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] fieldList = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	fieldList[i] = "(" + keyFields[i] + "," + fieldBase + ")";
		    	sLogger.info("Mapping data field list: " + fieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
			tableName = config.get("MappingTableName", tableName);
			sLogger.info("Mapping Table Name: " + tableName);
			String pDateStr = config.get("ProcessingDate");

			sLogger.info("Starting uploading the mapping data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			long timestamp = sdf.parse(pDateStr).getTime();
			for(int i=0; i < fileNames.length; i++){	
				String[] keys = StringUtils.split(keyFields[i], ",");
				adoptUtils.deleteDataByDate(poolName, tableName, columnName, timestamp, keys);    
				String filePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_daily";
		        sLogger.info("File Name: " + filePath);
				if(adoptUtils.doesFileExist(filePath)&&!adoptUtils.isEmptyFile(filePath)){
					config.set("PoolName", poolName);
					config.set("SourcePath", filePath);
					String property = tableName+fieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + filePath + " does not exist or is empty!");	
				}
			}
			
			adoptUtils.updatingColumns(poolName, tableName, columnName, timestamp);
			sLogger.info("Ended uploading the daily mapping data!\n");	
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the mapping data");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}			
}
