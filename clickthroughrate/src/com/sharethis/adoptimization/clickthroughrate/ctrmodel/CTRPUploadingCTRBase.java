package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.JdbcOperations;

public class CTRPUploadingCTRBase
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery2";
    
	private static final Logger sLogger = Logger.getLogger(CTRPUploadingCTRBase.class);
	private Configuration config = new Configuration();

	public int uploadingCTRBase(Configuration config) throws Exception {
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("Starting to get the parameters.");
			String poolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + poolName);
			String outFilePathDaily = config.get("ModelFilePath");
			sLogger.info("Base CTR File Path: " + outFilePathDaily);
			String pDateStr = config.get("ProcessingDate");
			String ctrBaseTableName = config.get("CTRPBaseTable");
			String columnName = "date_";
			adoptUtils.deleteDataByDate(poolName, ctrBaseTableName, columnName, pDateStr);

			String dataXNamesList = config.get("DataXNamesListUL");
			String dataXFieldsList = config.get("DataXFieldsListUL");
		    String[] dataXNamesArr = StringUtils.split(dataXNamesList, "|");
		    String[] dataXFieldsArr = StringUtils.split(dataXFieldsList, "|");
			sLogger.info("Starting uploading the ctr base ...");
			long time_s0 = System.currentTimeMillis();
			String[] args0=new String[1];
			String qName = config.get("QueueName");
			args0[0]="-Dmapred.job.queue.name="+qName;
			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
			Tool tool = (Tool) jobClass.newInstance();
		    for(int i=0; i<dataXNamesArr.length; i++){
			    String[] dataXNames = StringUtils.split(dataXNamesArr[i], ";");
			    String[] dataXFields = StringUtils.split(dataXFieldsArr[i], ";");
				for(int j=0; j<dataXNames.length; j++){
					String[] xFields = StringUtils.split(dataXFields[j], ",");
					String ctrFieldList = "(key_value_0";
					for(int j_f=1; j_f<xFields.length; j_f++){
						if((xFields[j_f].equalsIgnoreCase("campaign_id")&&!ctrFieldList.contains("campaign_id"))
								||(xFields[j_f].equalsIgnoreCase("adgroup_id")&&!ctrFieldList.contains("adgroup_id"))
								||(xFields[j_f].equalsIgnoreCase("creative_id")&&!ctrFieldList.contains("creative_id"))){
							ctrFieldList = ctrFieldList + ", " + xFields[j_f];					
						}else{
							ctrFieldList = ctrFieldList + ", key_value_" + j_f;
						}
					}
		    		ctrFieldList = ctrFieldList + ", sum_imps, sum_clicks, ctr)";				
		    		String ctrFilePath = outFilePathDaily+dayStr+"/"+dataXNames[j]+"_base";
		    		sLogger.info("File Name: " + ctrFilePath);
		    		if(adoptUtils.doesFileExist(ctrFilePath)&&!adoptUtils.isEmptyFile(ctrFilePath)){
		    			config.set("PoolName", poolName);
		    			config.set("SourcePath", ctrFilePath);
		    			String property = ctrBaseTableName+ctrFieldList;
		    			config.set("TableProperties", property);
		    			int res = ToolRunner.run(config, tool, args0);
		    			adoptUtils.updatingColumns(poolName, ctrBaseTableName, "key_name", dataXNames[j], "-1");
		    		}else{							
		    			sLogger.info("No data is uploaded into DB since the file: " + ctrFilePath + " does not exist or is empty!");	
		    		}
				}
			}
			adoptUtils.updatingColumns(poolName, ctrBaseTableName, columnName, pDateStr);
			sLogger.info("Ended uploading the ctr base!\n");						
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the ctr base");
			sLogger.info("Starting to delete the old ctr base ...\n");
			adoptUtils.deleteDataBeforeDate(poolName, ctrBaseTableName, columnName, pDateStr, 1);
			sLogger.info("Ended deleting the old ctr base.\n");
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
					" key_name varchar(120) default '-1', " +
					" key_value_0 varchar(120) default '-1', " +
					" key_value_1 varchar(120) default '-1', " +
					" key_value_2 varchar(120) default '-1', " +
					" sum_imps double default '0', " +
					" sum_clicks double default '0', " +
					" ctr double default '0', " +
					" date_ date default NULL, " +
					" primary key (key_name, key_value, date_) " + 
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
}
