package com.sharethis.adoptimization.clickthroughrate.upload;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.JdbcOperations;

public class UploadingCTRPModel
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery2?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
	private static final Logger sLogger = Logger.getLogger(UploadingCTRPModel.class);
	private Configuration config = new Configuration();

	public int uploadingCTRDaily(Configuration config) throws Exception {
		try{
			this.config = config;
			String fileNamePrefix = "adgroup";
			String keyFieldsList = "adgroup_id";				

			String ctrTableNameD = "ctr_daily";
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
			ctrTableNameD = config.get("CTRTableNameD", ctrTableNameD);
			sLogger.info("ctr daily Table Name: " + ctrTableNameD);
			String pDateStr = config.get("ProcessingDate");
			doesTableExist = config.getBoolean("DoesTableExist", true);
			if(!doesTableExist){
				createCTRTable(poolName, ctrTableNameD);
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
				adoptUtils.deleteDataByDate(poolName, ctrTableNameD, columnName, pDateStr, keys);    
				String ctrFilePath = outFilePathDaily+dayStr+"/"+fileNames[i]+"_daily";
		        sLogger.info("File Name: " + ctrFilePath);
				if(adoptUtils.doesFileExist(ctrFilePath)&&!adoptUtils.isEmptyFile(ctrFilePath)){
					config.set("PoolName", poolName);
					config.set("SourcePath", ctrFilePath);
					String property = ctrTableNameD+ctrFieldList[i];
					config.set("TableProperties", property);
					int res = ToolRunner.run(config, tool, args0);
				}else{							
					sLogger.info("No data is uploaded into DB since the file: " + ctrFilePath + " does not exist or is empty!");	
				}
			}
			adoptUtils.updatingColumns(poolName, ctrTableNameD, columnName, pDateStr);
			sLogger.info("Ended uploading the daily ctr data!\n");						
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the ctr data");
			sLogger.info("Starting to delete the old ctr daily data ...\n");
			adoptUtils.deleteDataBeforeDate(poolName, ctrTableNameD, columnName, pDateStr, 7);
			sLogger.info("Ended deleting the old ctr daily data.\n");
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
					" sum_imps double default '0', " +
					" sum_clicks double default '0', " +
					" sum_cost double default '0', " +
					" ctr double default '0', " +
					" e_cpm double default '0', " +
					" e_cpc double default '0', " +
					" date_ date default NULL, " +
					" primary key (campaign_id, adgroup_id, date_) " + 
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
	
	public void updatingCTRDataIntoPVTable(Configuration config)
			throws SQLException, ParseException{
		String poolName = config.get("PoolName");
		String ctrTableName = config.get("CTRTableNameD");
		String pvTableName = config.get("PriceVolumeCurve");
		String pDateStr = config.get("ProcessingDate");
		sLogger.info("Updating the click data into PV table on " + pDateStr);
		updatingCTRDataIntoPVTable(poolName, ctrTableName, pvTableName, pDateStr);
	}
	
	private void updatingCTRDataIntoPVTable(String poolName, String ctrTableName, String pvTableName, String pDateStr) 
			throws SQLException, ParseException{
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		try {
			sLogger.info("Start updating the ctr data into the pv data table ...\n");	
			String query = "update " + pvTableName + " a, " + ctrTableName + " b set a.clickDelivery = round(b.sum_clicks), " +
						"a.updateDate=b.date_ where a.adGroupId = b.adgroup_id and a.updateDate=b.date_ and b.date_ = ? " +
						"and b.sum_imps>=10";
			Date pDate = sdf.parse(pDateStr);
			stmt = con.prepareStatement(query);
			stmt.setTimestamp(1, new Timestamp(pDate.getTime()));
			sLogger.info("UpdatingCTRDataIntoPVTable query: " + stmt.toString());
			stmt.executeUpdate();
			sLogger.info("Updating the ctr data into the pv data table is done.\n");	
		}
		finally {
			stmt.close();
			con.close();
		}
	}	
}
