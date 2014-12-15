package com.sharethis.adoptimization.clickthroughrate.upload;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.JdbcOperations;

public class UploadingCTRWeekdayData
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
    
	private static final Logger sLogger = Logger.getLogger(UploadingCTRWeekdayData.class);
	private Configuration config = new Configuration();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public int uploadingCTRWeekday(Configuration config) throws Exception {
		try{
			this.config = config;
//			String poolName = "rtbDelivery";
//			String outFilePath = "/user/root/campaign_analytics/ctr/";
			String fileNamePrefix = "adgroup";
			String keyFieldsList = "adgroup_id";				

			String ctrTableNameWD = "ctr_weekday";
			String columnName = "date_";
			String ctrFieldBase = "sum_imps, sum_clicks, sum_cost, ctr, e_cpm, e_cpc";
			boolean doesTableExist = true;
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
			String poolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + poolName);

			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("Output File Path Daily: " + outFilePathDaily);
			fileNamePrefix = config.get("LoadingNameList", fileNamePrefix); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
			else{
				sLogger.info("fileNamePrefix is null.");
				throw new Exception("fileNamePrefix is null.");
			}
		
			keyFieldsList = config.get("LoadingKeyList", keyFieldsList);
			String[] keyFields = StringUtils.split(keyFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] ctrFieldList = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	ctrFieldList[i] = "(" + keyFields[i] + ", " + ctrFieldBase + ")";
		    	sLogger.info("ctr data field list: " + ctrFieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
			ctrTableNameWD = config.get("CTRTableNameWD", ctrTableNameWD);
			sLogger.info("ctr Table Name: " + ctrTableNameWD);
			String pDateStr = config.get("ProcessingDate");
			doesTableExist = config.getBoolean("DoesTableExist", true);
			if(!doesTableExist){
				createCTRTable(poolName, ctrTableNameWD);
			}
			sLogger.info("Starting uploading the ctr data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			for(int i=0; i < fileNames.length; i++){	
				String[] keys = StringUtils.split(keyFields[i], ",");
				adoptUtils.deleteDataByDate(poolName, ctrTableNameWD, columnName, pDateStr, keys);    
				String ctrFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_weekday";
		        sLogger.info("File Name: " + ctrFilePath);
				if(adoptUtils.doesFileExist(ctrFilePath)&&!adoptUtils.isEmptyFile(ctrFilePath)){
					config.set("SourcePath", ctrFilePath);
					String property = ctrTableNameWD+ctrFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + ctrFilePath + " does not exist or is empty!");	
				}
			}
			adoptUtils.updatingColumns(poolName, ctrTableNameWD, columnName, pDateStr);
			int numOfWeeks = config.getInt("NumOfWeeksWD", 4);
			updatingWeekday(poolName, ctrTableNameWD, columnName, pDateStr, "weekday", "weekday_ind", numOfWeeks);
			sLogger.info("Ended uploading the daily ctr data!\n");						
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the ctr data");
			sLogger.info("Starting to delete the ctr weekday data ...\n");
			adoptUtils.deleteDataBeforeDate(poolName, ctrTableNameWD, columnName, pDateStr, 7);
			sLogger.info("Ended deleting the ctr weekday data.\n");
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}		
	
	public void createCTRTable(String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 
					" campaign_id bigint(30) default '-1', " +
					" adgroup_id bigint(30) default '-1', " +
					" weekday_ind int default '-1', " +
					" weekday varchar(10) default NULL, " +
					" sum_imps double default '0', " +
					" sum_clicks double default '0', " +
					" sum_cost double default '0', " +
					" ctr double default '0', " +
					" e_cpm double default '0', " +
					" e_cpc double default '0', " +
					" date_ date default NULL, " +
					" primary key (adgroup_id, weekday, date_) " + 
					" ) ENGINE=MyISAM";
			pstmt = con.prepareStatement(createTableQuery);
			pstmt.execute();
		} catch (SQLException se){
			sLogger.error(se);
			throw se;
		} finally {
			pstmt.close();				 
			con.close();					
		}
	}
		
	public void updatingWeekday(String poolName, String tableName, String columnName, String pDateStr, 
			String wdColumn, String wdInd, int numOfWeeks) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		Date pDate = sdf.parse(pDateStr);
		Calendar cal = Calendar.getInstance();
		String[] weekdays = new DateFormatSymbols().getWeekdays();
		cal.setTime(pDate);
		int wd = cal.get(Calendar.DAY_OF_WEEK);
		String wdStr = weekdays[wd];
		try {
			String query = "update " + tableName + " set " + wdInd + " = " + wd + ", " + wdColumn + " = \"" + wdStr 
					+ "\", sum_imps = round(sum_imps/" + numOfWeeks + "), sum_clicks = round(sum_clicks/" 
					+ numOfWeeks + "), sum_cost = round(100*sum_cost/" + numOfWeeks
					+ ")/100.0 where " + columnName + " = ?";
			sLogger.info("update query: " + query);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
}
