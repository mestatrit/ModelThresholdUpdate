package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.JdbcOperations;

public class CTRPUploadingModels
{
    public static final String USER = "sharethis";
    public static final String PASSWORD = "sharethis";
    public static final String URL = "jdbc:mysql://adopsdb1001.east.sharethis.com/rtbDelivery2";
    
	private static final Logger sLogger = Logger.getLogger(CTRPUploadingModels.class);
	private Configuration config = new Configuration();

	public int uploadingModels(Configuration config) throws Exception {
		try{
			this.config = config;
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("Starting to get the parameters.");
			String poolName = config.get("PoolName");
			sLogger.info("Export to MySql Pool Name: " + poolName);
			String outFilePathDaily = config.get("ModelFilePath");
			sLogger.info("Model File Path: " + outFilePathDaily);
			String pDateStr = config.get("ProcessingDate");
			String modelTableName = config.get("CTRPModelTable");
			String columnName = "date_";
			adoptUtils.deleteDataByDate(poolName, modelTableName, columnName, pDateStr);
			String dataColumns = "r_square, adj_r_square, num_points, model_type";

			String modelNamesListUL = config.get("CTRPModelNamesListUL");
			String xNamesFieldsListUL = config.get("CTRPXNamesListUL");
			String modelNamesList = config.get("CTRPModelNamesList");
			String yModelFieldsList = config.get("CTRPYModelFieldsList");
			String[] modelNamesListULArr = StringUtils.split(modelNamesListUL, ":");
		    String[] xNamesFieldsListULArr = StringUtils.split(xNamesFieldsListUL, ":");
		    String[] modelNamesListArr = StringUtils.split(modelNamesList, ":");
		    String[] yModelFieldsListArr = StringUtils.split(yModelFieldsList, ":");
	    	long time_s0 = System.currentTimeMillis();
		    for(int ul=0; ul<modelNamesListULArr.length; ul++){
		    	String[] modelNamesArrUL = StringUtils.split(modelNamesListULArr[ul], "|");
		    	String[] xNamesArrUL = StringUtils.split(xNamesFieldsListULArr[ul], "|");
		    	String[] modelNamesArr = StringUtils.split(modelNamesListArr[ul], "|");
		    	String[] yFieldsArr = StringUtils.split(yModelFieldsListArr[ul], "|");
		    	String[] args0=new String[1];
		    	String qName = config.get("QueueName");
		    	args0[0]="-Dmapred.job.queue.name="+qName;
		    	String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
		    	sLogger.info("Starting uploading the ctr model ...\n");
		    	Class jobClass = Class.forName("com.sharethis.adoptimization.common.ExportToDBJob");   
		    	Tool tool = (Tool) jobClass.newInstance();
		    	int[] i_val = new int[modelNamesArrUL.length];
		    	for(int i=0; i<modelNamesArrUL.length; i++){
		    		for(int j=0; j<modelNamesArr.length; j++){
					if(modelNamesArrUL[i].equalsIgnoreCase(modelNamesArr[j]))
						i_val[i]=j;
		    		}
		    		String ctrFieldList = "(model_value, intercept";
		    		String[] xNames = StringUtils.split(xNamesArrUL[i], ";"); 
		    		for(int j=0; j<xNames.length; j++){
		    			ctrFieldList = ctrFieldList + ", " + xNames[j];
		    		}
		    		ctrFieldList = ctrFieldList + ", " + dataColumns + ")";
				
		    		String ctrFilePath = outFilePathDaily+dayStr+"/CTRP_"+modelNamesArrUL[i]+"_model";
//					String ctrFilePath = outFilePathDaily+dayStr+"/CTRP_"+modelNamesArrUL[i]+"_model_0";
		    		sLogger.info("File Name: " + ctrFilePath);
		    		if(adoptUtils.doesFileExist(ctrFilePath)&&!adoptUtils.isEmptyFile(ctrFilePath)){
		    			config.set("PoolName", poolName);
		    			config.set("SourcePath", ctrFilePath);
		    			String property = modelTableName+ctrFieldList;
		    			config.set("TableProperties", property);
		    			int res = ToolRunner.run(config, tool, args0);
		    		}else{							
		    			sLogger.info("No data is uploaded into DB since the file: " + ctrFilePath + " does not exist or is empty!");	
		    		}
		    		adoptUtils.updatingColumns(poolName, modelTableName, "model_name", modelNamesArrUL[i], "-1");
		    		adoptUtils.updatingColumns(poolName, modelTableName, "model_level", yFieldsArr[i_val[i]], "-1");
		    	}
		    }
			adoptUtils.updatingColumns(poolName, modelTableName, columnName, pDateStr);
			sLogger.info("Ended uploading the ctr model!\n");						
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "Uploading the ctr model");
			sLogger.info("Starting to delete the old ctr model ...\n");
			adoptUtils.deleteDataBeforeDate(poolName, modelTableName, columnName, pDateStr, 1);
			sLogger.info("Ended deleting the old ctr model.\n");
			findBestModel(poolName, modelTableName);
			//String ctrBaseTable = config.get("CTRPBaseTable");
			//findCTRThresholdValue(poolName, modelTableName, ctrBaseTable);
		}catch(Exception ee){
			sLogger.info("Exception in run: \n" + ee.toString());
			throw new Exception(ee);
		}
		return 0;		
	}		
	
	public void createCTRPModelTable(String poolName, String tableName) throws IOException, SQLException {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement pstmt1 = null;
		try{
			String createTableQuery = "CREATE TABLE " + tableName + " ( " + 
					" model_name varchar(120) default '-1', " +
					" model_level varchar(120) default '-1', " +
					" model_value varchar(120) default '-1', " +
					" intercept double default '0', " +					
					" crtv_coeff double default '0', " +
					" asv_coeff double default '0', " +
					" net_coeff double default '0', " + 
					" usg_coeff double default '0', " + 
					" vert_coeff double default '0', " + 
					" set_coeff double default '0', " + 
					" ctl1_coeff double default '0', " + 
					" asl1_coeff double default '0', " +
					" asi_coeff double default '0', " +
					" ctl1asl1_coeff double default '0', " + 
					" r_square double default '0', " +
					" adj_r_square double default '0', " +
					" num_points bigint(10) default '0', " +
					" model_type int(5) default '-1', " +
					" date_ date default null, " +
					" primary key (model_name, model_level, model_value, model_type, date_) " + 
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
	
	private void findBestModel(String poolName, String tableName) throws SQLException, Exception {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		String bestModelName = null;
		double max_r_square = -1;
		int bestModelType = -1;
		String keyNameStr = config.get("CTRPModelFeatureList");
		String[] keyNames = StringUtils.split(keyNameStr, ";");
		String[] coeffNames = new String[keyNames.length];
		for(int i=0; i<keyNames.length; i++)
			coeffNames[i] = keyNames[i] + "_coeff";				
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.prepareStatement("select * from " + tableName);
			sLogger.info("Selecting query: " + stmt.toString());
			rs = stmt.executeQuery();
			double coeff = 0;
			if (rs!=null){
				while(rs.next()) {
					boolean coeffFlag = true;
					for(int i=0; i<coeffNames.length; i++){
						coeff = rs.getDouble(coeffNames[i]);
						if(coeff<0){
							coeffFlag = false;
						}
					}
					String modelName = rs.getString("model_name");
					double r_square = rs.getDouble("r_square");
					int model_type = rs.getInt("model_type");
					sLogger.info("Coefficient Flag: " + coeffFlag + "  r_square: " + r_square);
					if(coeffFlag&&(r_square>max_r_square)&&(model_type==0)){
						max_r_square = r_square;
						bestModelName = modelName;
						bestModelType = model_type;
					}
				}
				stmt = con.prepareStatement("update " + tableName 
						+ " set model_flag = 1 where model_name = '" 
						+ bestModelName + "' and model_type = " + bestModelType);
				sLogger.info("Updating query: " + stmt.toString());
				stmt.executeUpdate();
			}else{
				sLogger.info(tableName + " is empty.");
			}
		}finally{
			rs.close();
			stmt.close();
		}
	}
	
	private Map<String, Double> getDefaultCTRBase(String poolName, String tableName) throws SQLException, Exception {
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		Map<String, Double> dCTRBaseMap = new HashMap<String, Double>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			double avgCTR = 0;
			int cnt = 0;
			String selectQuery = "select key_name, ctr from " + tableName + " where key_value_0='-99'";
			stmt = con.prepareStatement(selectQuery);
			sLogger.info("Selecting query: " + stmt.toString());
			rs = stmt.executeQuery();
			if (rs!=null){
				while(rs.next()) {
					avgCTR += rs.getDouble("ctr");
					cnt ++;
					dCTRBaseMap.put(rs.getString("key_name"), rs.getDouble("ctr"));
				}
			}else{
				sLogger.info(tableName + " is empty.");
			}
			if(cnt>0)
				dCTRBaseMap.put("-99", avgCTR/cnt);
			else
				dCTRBaseMap.put("-99", 0.0);				
			return dCTRBaseMap;
		}finally{
			rs.close();
			stmt.close();
		}
	}
	
	private void findCTRThresholdValue(String poolName, String modelTable, String ctrBaseTable) throws SQLException, Exception {
		Map<String, Double> ctrMap = getDefaultCTRBase(poolName, ctrBaseTable);
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		String keyNameStr = config.get("CTRPModelFeatureList");
		String[] keyNames = StringUtils.split(keyNameStr, ";");
		String[] coeffNames = new String[keyNames.length];
		for(int i=0; i<keyNames.length; i++)
			coeffNames[i] = keyNames[i] + "_coeff";				
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.prepareStatement("select * from " + modelTable + " where model_type = 0");
			sLogger.info("Selecting query: " + stmt.toString());
			rs = stmt.executeQuery();
			double coeff = 0;
			double ctrThVal = 0;
			double defaultCTR = 0;
			if (rs!=null){
				while(rs.next()) {
					ctrThVal = rs.getDouble("intercept");
					for(int i=0; i<coeffNames.length; i++){
						coeff = rs.getDouble(coeffNames[i]);
						if(ctrMap.containsKey(keyNames[i]))
							defaultCTR = ctrMap.get(keyNames[i]);
						else
							defaultCTR = ctrMap.get("-99");							
						ctrThVal += coeff*defaultCTR;
					}
					String modelName = rs.getString("model_name");
					int model_type = rs.getInt("model_type");
					PreparedStatement stmt1 = con.prepareStatement("update " + modelTable 
						+ " set ctr_threshold = " + ctrThVal + " where model_name = '" 
						+ modelName + "' and model_type = " + model_type);
					sLogger.info("Updating query: " + stmt1.toString());
					stmt1.executeUpdate();
				}
			}else{
				sLogger.info(modelTable + " is empty.");
			}
		}finally{
			rs.close();
			stmt.close();
		}
	}
}
