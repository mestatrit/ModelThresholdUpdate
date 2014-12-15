package com.sharethis.adoptimization.inventory.upload;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.JdbcOperations;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


public class UploadingInventory
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
    
	private static final Logger sLogger = Logger.getLogger(UploadingInventory.class);
	
	public int uploadingInventory(Configuration config) throws Exception {
		try{
//			String poolName = "rtbDelivery";
			int daysKept = 90;
//			String outFilePath = "/user/xibin/inventory/";
			String fileNamePrefix = "adgroup";
			String keyFieldsList = "adgroup_id";				

			String invTableName = "inventory_nobid";
			String columnName = "date_";
			String invFieldBase = "sum_nobids, sum_bp, sum_mbp, avg_bp, avg_mbp";
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

//			outFilePath = config.get("OutFilePath", outFilePath);
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("Output File Path Daily: " + outFilePathDaily);
			fileNamePrefix = config.get("PipeNamesListDB", fileNamePrefix); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			int numOfDays = config.getInt("NumOfDays", 7);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
		
			keyFieldsList = config.get("KeyFieldsListDB", keyFieldsList);
			String[] keyFields = StringUtils.split(keyFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] bidFieldList = new String[keyFieldLen];
		    String[] bidFieldListAgg = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	bidFieldList[i] = "(" + keyFields[i] + ", " + invFieldBase + ")";
		    	bidFieldListAgg[i] = "(" + keyFields[i] + ", " + invFieldBase + 
		    			", num_days, date_" + ")";
		    	sLogger.info("Inventory data field list: " + bidFieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
			invTableName = config.get("InvTableName", invTableName);
			sLogger.info("Inventory Table Name: " + invTableName);
			doesTableExist = config.getBoolean("DoesTableExist", true);
			String pDateStr = pvUtils.getPDateStr(new Date(), dayInterval);
			config.set("ProcessingDate", pDateStr);
			if(!doesTableExist){
				createInventoryTable(config, exportPoolName, invTableName);
			}
			sLogger.info("Starting uploading the inventory data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			for(int i=0; i < fileNames.length; i++){	
				String[] keys = StringUtils.split(keyFields[i], ",");
				pvUtils.deleteDataByDate(exportPoolName, invTableName, columnName, pDateStr, keys);    
				String invFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_daily";
		        sLogger.info("File Name: " + invFilePath);
				if(pvUtils.doesFileExist(invFilePath)&&!pvUtils.isEmptyFile(invFilePath)){
					config.set("SourcePath", invFilePath);
					String property = invTableName+bidFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + invFilePath + " does not exist or is empty!");	
				}
			}
			pvUtils.updatingColumns(exportPoolName, invTableName, columnName, pDateStr, "num_days", 1);
			sLogger.info("Ended uploading the daily inventory data!\n");			
		
			for(int i=0; i < fileNames.length; i++){					    
				String bidFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_weekly";    
				sLogger.info("File Name: " + bidFilePath);
				if(pvUtils.doesFileExist(bidFilePath)&&!pvUtils.isEmptyFile(bidFilePath)){
					config.set("SourcePath", bidFilePath);
					String property = invTableName+bidFieldListAgg[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + bidFilePath + " does not exist or is empty!");	
				}
			}
			pvUtils.updatingColumns(exportPoolName, invTableName, columnName, pDateStr, "num_days", numOfDays);
			sLogger.info("Ended uploading the aggregated inventory data!\n");					

			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the inventory data");
			sLogger.info("Starting to delete the inventory data ...\n");
			daysKept = config.getInt("DaysKept", daysKept);
			pvUtils.deleteDataBeforeDate(exportPoolName, invTableName, columnName, pDateStr, daysKept);
			sLogger.info("Ended deleting the inventory data.\n");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}	
		
	public void createInventoryTable(Configuration config, String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt1 = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 		
					" adgroup_id bigint(30) default '-1', " +
					" domain_name varchar(120) default '-1', " +
					" result_reason varchar(50) default '-1', " +
					" user_seg_id varchar(30) default '-1', " +
					" ctl1_id varchar(30) default '-1', " +
					" asl1_id varchar(30) default '-1', " +
					" max_cpm int(10) default '-1', " +
					" freq_cap int(5) default '-1', " +
					" sum_nobids double default '0', " +
					" sum_bp double default '0', " +
					" sum_mbp double default '0', " +
					" avg_bp double default '0', " +
					" avg_mbp double default '0', " +
					" num_days int(5) default '-1', " +
					" date_ date default NULL, " +
					" primary key (adgroup_id,domain_name,result_reason,user_seg_id,ctl1_id,asl1_id,max_cpm,freq_cap,num_days,date_) " + 
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
