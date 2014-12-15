package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.JdbcOperations;

public class UploadingMVCTRData
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private static final Logger sLogger = Logger.getLogger(UploadingMVCTRData.class);
	private Configuration config = new Configuration();

	public int uploadingCTRDaily(Configuration config) throws Exception {
		try{
			this.config = config;
			String columnName = "date_";
			String ctrFieldBase = "sum_imps, sum_clicks, sum_cost, sum_clicks_pred, ctr, e_cpm, e_cpc, ctr_pred";
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
			String fileNamePrefix = config.get("CTRPMVLoadingNames"); 
			sLogger.info("File Name Prefix: " + fileNamePrefix);
			String[] fileNames = null;
			if(!(fileNamePrefix==null))			
				fileNames = StringUtils.split(fileNamePrefix, ";");
			else{
				sLogger.info("fileNamePrefix is null.");
				throw new Exception("fileNamePrefix is null.");
			}
		
			String keyFieldsList = config.get("CTRPMVLoadingFields");
			String[] keyFields = StringUtils.split(keyFieldsList,";");
			
			int keyFieldLen = keyFields.length;
		    String[] ctrFieldList = new String[keyFieldLen];
		    for(int i=0; i<keyFieldLen; i++){
		    	ctrFieldList[i] = "(" + keyFields[i] + ", " + ctrFieldBase + ")";
		    	sLogger.info("ctr data field list: " + ctrFieldList[i]);
		    	sLogger.info("File name prefix list: " + fileNames[i]);
		    	sLogger.info("Key field list: " + keyFields[i]);
		    }
			String mvCTRTableName = config.get("CTRPMVCTRTable", "mv_ctr_adg_daily");
			sLogger.info("MV ctr daily Table Name: " + mvCTRTableName);
			String pDateStr = config.get("ProcessingDate");
			sLogger.info("Starting uploading the mv ctr data ...\n");
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		        
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			for(int i=0; i < fileNames.length; i++){	
				String[] keys = StringUtils.split(keyFields[i], ",");
				adoptUtils.deleteDataByDate(poolName, mvCTRTableName, columnName, pDateStr, keys);    
				String ctrFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_daily";
		        sLogger.info("File Name: " + ctrFilePath);
				if(adoptUtils.doesFileExist(ctrFilePath)&&!adoptUtils.isEmptyFile(ctrFilePath)){
					config.set("PoolName", poolName);
					config.set("SourcePath", ctrFilePath);
					String property = mvCTRTableName+ctrFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + ctrFilePath + " does not exist or is empty!");	
				}
			}
			adoptUtils.updatingColumns(poolName, mvCTRTableName, columnName, pDateStr);
			computingMVCTRAvg(poolName, mvCTRTableName, pDateStr);
			computingMVCTROverallAvg(poolName, mvCTRTableName, pDateStr);
			sLogger.info("Ended uploading the daily mv ctr data!\n");						
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the mv ctr data");
			sLogger.info("Starting to delete the old mv ctr daily data ...\n");
			adoptUtils.deleteDataBeforeDate(poolName, mvCTRTableName, columnName, pDateStr, 7);
			sLogger.info("Ended deleting the old mv ctr daily data.\n");
			//updatingCTRDataIntoRTBTable(config);
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}		
	
	private void computingMVCTRAvg(String poolName, String mvCTRTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Computing the average of mv ctr on " + pDateStr + " ...\n");	
			String sumQuery = "select adgroup_id, -99, sum(sum_imps) , sum(sum_clicks), " +
					"TRUNCATE(sum(sum_cost), 6), TRUNCATE(sum(sum_clicks_pred), 6), date_ from " + 
					mvCTRTableName + " where date_= ? and bucket_id>=0 group by adgroup_id, date_";
			
			String insertQuery = "insert into "  + mvCTRTableName + "(adgroup_id, bucket_id, sum_imps, sum_clicks, " +
					"sum_cost, sum_clicks_pred, date_) " + sumQuery;
			
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(insertQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("Computing and inserting the average of mv ctr query: " + stmt.toString());
			stmt.executeUpdate();
			String updateQuery1 = "update "  + mvCTRTableName + " set ctr = TRUNCATE(sum_clicks/sum_imps, 6), " +
					"e_cpm = TRUNCATE(sum_cost/sum_imps*1000, 6),  e_cpc = if(sum_clicks>=1, TRUNCATE(sum_cost/sum_clicks, 6), null), " +
					"ctr_pred = TRUNCATE(sum_clicks_pred/sum_imps, 6) where bucket_id=-99 and date_ = ?";
			stmt = con.prepareStatement(updateQuery1);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Computing the average of mv ctr on " + pDateStr + " is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	

	private void computingMVCTROverallAvg(String poolName, String mvCTRTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Computing the overall average of mv ctr on " + pDateStr + " ...\n");	
			String sumQuery = "select -99, -99, sum(sum_imps), sum(sum_clicks), " +
					"TRUNCATE(sum(sum_cost), 6), TRUNCATE(sum(sum_clicks_pred), 6), date_ from " + 
					mvCTRTableName + " where date_= ? and bucket_id=-99 group by bucket_id, date_";
			
			String insertQuery = "insert into "  + mvCTRTableName + "(adgroup_id, bucket_id, sum_imps, sum_clicks, " +
					"sum_cost, sum_clicks_pred, date_) " + sumQuery;
			
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(insertQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("Computing and inserting the average of mv ctr query: " + stmt.toString());
			stmt.executeUpdate();
			String updateQuery1 = "update "  + mvCTRTableName + " set ctr = TRUNCATE(sum_clicks/sum_imps, 6), " +
					"e_cpm = TRUNCATE(sum_cost/sum_imps*1000, 6), e_cpc = if(sum_clicks>=1, TRUNCATE(sum_cost/sum_clicks, 6), null), " +
					"ctr_pred = TRUNCATE(sum_clicks_pred/sum_imps, 6) where adgroup_id=-99 and bucket_id=-99 and date_ = ?";
			stmt = con.prepareStatement(updateQuery1);			
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			stmt.executeUpdate();
			sLogger.info("Computing the overall average of mv ctr on " + pDateStr + " is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
	
	private void updatingCTRDataIntoRTBTable(Configuration config)
			throws SQLException, ParseException{
		//RTB.RTB_AdGroupProperty
		String poolName = config.get("PoolName");
		String rtbPoolName = config.get("PoolNameRTB");
		String ctrTableName = config.get("CTRPMVCTRTable");
		String optTableName = config.get("CTROptTable");
		String rtbTableName = config.get("CTRPMVCTRThreshold");
		String pDateStr = config.get("ProcessingDate");
		sLogger.info("Updating the ctr threshold into " + rtbTableName + " on " + pDateStr + " ...");
		updatingOverallCTRDataIntoRTBTable(poolName, ctrTableName, optTableName, rtbPoolName, rtbTableName, pDateStr);
		updatingCTRDataIntoRTBTable(poolName, ctrTableName, optTableName, rtbPoolName, rtbTableName, pDateStr);
		sLogger.info("Updating the ctr threshold into " + rtbTableName + " on " + pDateStr + " is done.");
	}

	private void updatingOverallCTRDataIntoRTBTable(String poolName, String ctrTableName, 
			String optTableName, String rtbPoolName, String rtbTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Updating the overall ctr threshold value into " + rtbTableName + " ...\n");	
			String selectQuery = "select ctr_pred, date_ from " + poolName + 
					"." + ctrTableName + " where adgroup_id=-99 and bucket_id=-99 and date_ = ?";
			String updateQuery = "update " + rtbPoolName + "." + rtbTableName + " a, (" + 
					selectQuery + ") b," + poolName + 
					"." + optTableName + " c set a.minCTR4AdGroup = b.ctr_pred, " +
					"a.updateDate = ? where c.adgroup_id=-99 and c.updatingCTRThresValFlag=1";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(updateQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));	
			Date date= new java.util.Date();
			stmt.setTimestamp(2, new Timestamp(date.getTime()));	
			sLogger.info("Updating the overall average of mv ctr query: " + stmt.toString());
			stmt.executeUpdate();
	
			sLogger.info("Updating the ctr threshold value into " + rtbTableName + " is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
	
	private void updatingCTRDataIntoRTBTable(String poolName, String ctrTableName, 
			String optTableName, String rtbPoolName, String rtbTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Updating the ctr threshold value into " + rtbTableName + " ...\n");	
			String selectQuery = "select adgroup_id, ctr_pred, date_ from " + poolName + 
					"." + ctrTableName + " where bucket_id = -99 and date_ = ? " +
					"and sum_imps >= 10000 and sum_clicks > 0";
			String updateQuery = "update " + rtbPoolName + "." + rtbTableName + " a, (" + 
					selectQuery + ") b," + poolName + 
					"." + optTableName + " c set a.minCTR4AdGroup = b.ctr_pred, " +
					"a.updateDate = ? where a.adGroupId = b.adgroup_id and " +
					"a.adGroupId = c.adgroup_id and c.updatingCTRThresValFlag = 1";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(updateQuery);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));			
			Date date= new java.util.Date();
			stmt.setTimestamp(2, new Timestamp(date.getTime()));	
			sLogger.info("Updating the average of mv ctr query: " + stmt.toString());
			stmt.executeUpdate();
	
			sLogger.info("Updating the ctr threshold value into " + rtbTableName + " is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
}
