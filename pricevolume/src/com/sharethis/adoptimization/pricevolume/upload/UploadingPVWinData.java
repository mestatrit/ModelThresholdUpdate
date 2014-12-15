package com.sharethis.adoptimization.pricevolume.upload;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


public class UploadingPVWinData
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
    
	private static final Logger sLogger = Logger.getLogger(UploadingPVWinData.class);
	
	public int uploadingPVWinData(Configuration config) throws Exception {
		try{
			int daysKept = 85;
//			String exportPoolName = "rtbDelivery";
//			String outFilePath = "/user/hadoop/output/";
			String fileNamePrefix = "adgroup;adgroup_cr";
			String keyFieldsList = "adgroup_id;adgroup_id,creative_id";				

			String winTableName = "price_volume_win_ui";
			String columnName = "date_";
			String winFieldBase = "winning_price, sum_wins, sum_mwins, cum_wins, sum_bids, sum_nobids, num_days, date_, win_rate_hist, win_rate_pred";
			boolean doesTableExist = true;
			sLogger.info("Entering the class ...");
			long time_s0 = System.currentTimeMillis();
			if(config==null){
				sLogger.info("config is null.");
				throw new Exception("config is null.");
			}else{
				sLogger.info("config:" + config.toString());
			}
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");
			
 			sLogger.info("Starting to get the parameters.");
			String exportPoolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + exportPoolName);

//			outFilePath = config.get("OutFilePath", outFilePath);
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("Output File Path Daily: " + outFilePathDaily);
			fileNamePrefix = config.get("PipeNameList", fileNamePrefix) + ";" 
					+ config.get("PipeNameBase"); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");

			keyFieldsList = config.get("KeyFieldsList", keyFieldsList) + ";" 
					+ config.get("KeyFieldsBase");
			String[] keyFields = StringUtils.split(keyFieldsList,";");
		
			int numOfDays = config.getInt("numOfDays", 7);
			int keyFieldLen = keyFields.length;
		    String[] winFieldList = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	winFieldList[i] = "(" + keyFields[i] + ", " + winFieldBase + ")";
		    	sLogger.info("Win data field list: " + winFieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
		        
			sLogger.info("Win Table Name: " + winTableName);
			winTableName = config.get("WinTableNameUI");
			sLogger.info("Win Table Name: " + winTableName);
			columnName = config.get("ColumnName", columnName);
			sLogger.info("Column Name: " + columnName);
			doesTableExist = config.getBoolean("DoesTableExist", true);
			if(!doesTableExist){
				pvUtils.createTable(exportPoolName, winTableName);
			}
			sLogger.info("Starting uploading the win data ...\n");   
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        	
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String[] keys = null;
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			for(int i=0; i < fileNames.length; i++){
				keys = StringUtils.split(keyFields[i], ",");
				pvUtils.deleteDataByDate(exportPoolName, winTableName, columnName, pDateStr, keys);    
				String filePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_win_p_daily";    
				sLogger.info("File Name: " + filePath);
				if(pvUtils.doesFileExist(filePath)&&!pvUtils.isEmptyFile(filePath)){
					config.set("SourcePath", filePath);
					String property = winTableName+winFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + filePath + " does not exist or is empty!");	
				}					
			}
			sLogger.info("Ended loading the daily win data!\n");	
			for(int i=0; i < fileNames.length; i++){
				String filePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_win_p_agg";   
				sLogger.info("File Name: " + filePath);
				if(pvUtils.doesFileExist(filePath)&&!pvUtils.isEmptyFile(filePath)){
					config.set("SourcePath", filePath);
					String property = winTableName+winFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + filePath + " does not exist or is empty!");	
				}					
			}
			sLogger.info("Ended uploading the aggregated win data!\n");	
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the win data");
			sLogger.info("Starting to delete the old win data ...\n");
			daysKept = config.getInt("DaysKept", daysKept);
			pvUtils.deleteDataBeforeDate(exportPoolName, winTableName, columnName, pDateStr, daysKept);
			sLogger.info("Ended deleting the old win data.\n");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}		
}
