package com.sharethis.adoptimization.pricevolume.pvmodel;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.JdbcOperations;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


public class ModelUploadingModel
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
    
	private static final Logger sLogger = Logger.getLogger(ModelUploadingModel.class);
	
	public int uploadingPVModelData(Configuration config) throws Exception {
		try{
			int daysKept = 90;
//			String outFilePath = "/user/root/campaign_analytics/price_volume";
			String fileNamePrefix = "adgroup;domain";
			String modelFieldsList = "adgroup_id;domain_name";				

			String modelTableName = "price_volume_model_pred";
			String columnName = "date_";

			String modelFieldBase = "model_type, beta0, beta1, r_square, num_wp, win_rate, min_wp, max_wp, max_wp_pred, num_days, date_";
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
			String exportPoolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + exportPoolName);

			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("Output File Path Daily: " + outFilePathDaily);
			fileNamePrefix = config.get("ModelNameList", fileNamePrefix); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
			else{
				sLogger.info("fileNamePrefix is null.");
				throw new Exception("fileNamePrefix is null.");
			}
		
			modelFieldsList = config.get("ModelFieldsList", modelFieldsList);
			String[] keyFields = StringUtils.split(modelFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] modelFieldList = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	modelFieldList[i] = "(" + keyFields[i] + ", " + modelFieldBase + ")";
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    	sLogger.info("Model data field list: " + modelFieldList[i]);
		    }
			modelTableName = config.get("ModelTableName", modelTableName);
			sLogger.info("Model Table Name: " + modelTableName);
			doesTableExist = config.getBoolean("DoesTableExist", true);
			if(!doesTableExist){
				createModelTable(config, exportPoolName, modelTableName);
			}
			sLogger.info("Starting uploading the model data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String pDateStr = config.get("ProcessingDate");
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			for(int i=0; i < fileNames.length; i++){	
				String[] keys = StringUtils.split(keyFields[i], ",");
				pvUtils.deleteDataByDate(exportPoolName, modelTableName, columnName, pDateStr, keys);    
				String modelFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_model_p_daily";
		        sLogger.info("File Name: " + modelFilePath);
				if(pvUtils.doesFileExist(modelFilePath)&&!pvUtils.isEmptyFile(modelFilePath)){
					config.set("SourcePath", modelFilePath);
					String property = modelTableName+modelFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + modelFilePath + " does not exist or is empty!");	
				}
			}
			sLogger.info("Ended uploading the daily model data!\n");			
			
			for(int i=0; i < fileNames.length; i++){					    
				String modelFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_model_p_agg";    
				sLogger.info("File Name: " + modelFilePath);
				if(pvUtils.doesFileExist(modelFilePath)&&!pvUtils.isEmptyFile(modelFilePath)){
					config.set("SourcePath", modelFilePath);
					String property = modelTableName+modelFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + modelFilePath + " does not exist or is empty!");	
				}
			}
			sLogger.info("Ended uploading the aggregated model data!\n");					
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the model data");
			sLogger.info("Starting to delete the model data ...\n");
			daysKept = config.getInt("DaysKept", daysKept);
			pvUtils.deleteDataBeforeDate(exportPoolName, modelTableName, columnName, pDateStr, daysKept);
			sLogger.info("Ended deleting the model data.\n");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}	
	
	private void createModelTable(Configuration config, String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt1 = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 
					" adgroup_id bigint(30) default '-1', " +
					" creative_id bigint(30) default '-1', " +
					" domain_name varchar(120) default '-1', " +
					" beta0 double default '0', " +
					" beta1 double default '0', " +
					" r_square double default '0', " +
					" model_type int default '0', " +
 					" impressionDelivery double default '0', " +
					" ecpmDelivery double default '0', " +  
					" win_rate double default '0', " + 
					" num_wp int default '0', " +
					" min_wp double default '0', " + 
					" max_wp double default '0', " +
					" max_wp_pred double default '0', " +
					" sum_bids double default '0', " + 
					" sum_nobids double default '0', " +  
					" total_inventory double default '0', " + 
					" date_ date default NULL, " +
					" num_days int default '-1', " +
					" primary key (adgroup_id, creative_id, domain_name, date_, num_days) " + 
					" ) ENGINE=MyISAM";
			pstmt1 = con.prepareStatement(createTableQuery);
			pstmt1.execute();
		} catch (SQLException se){
			sLogger.error(se);
			throw se;
		} finally {
			pstmt1.close();				 
			con.close();					
		}
	}

}
