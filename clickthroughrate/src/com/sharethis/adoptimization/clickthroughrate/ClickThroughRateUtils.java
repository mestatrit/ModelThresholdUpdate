package com.sharethis.adoptimization.clickthroughrate;

import java.text.ParseException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.sharethis.adoptimization.common.JdbcOperations;


/**
 * This is the class to run the flow.
 */

public class ClickThroughRateUtils 
{	
	private static final Logger sLogger = Logger.getLogger(ClickThroughRateUtils.class);	
	private Configuration config = new Configuration();

	public ClickThroughRateUtils(Configuration config){
		this.config=config;
	}
	
	public Map<String, List<String>> CreativeCategory() 
			throws SQLException, ParseException, Exception{
		String poolName = "rtb";
		String sourceTable = "CreativeCategoryM";
		sLogger.info("Entering to get the creative category ...");						
		poolName = config.get("RTBPoolName", poolName);
		sLogger.info("Export to MySql Pool Name: " + poolName);		
		sourceTable = config.get("CreativeCategoryTable", sourceTable);
		sLogger.info("Creative Category Table: " + sourceTable);							        
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		try {										
			return GetCategoryMap(poolName, sourceTable, con, "creative_id", "category_id");
		}
		finally {
			con.close();
		}
	}    

	public Map<String, List<String>> AdSpaceCategory() 
			throws SQLException, ParseException, Exception{
		String poolName = "rtb";
		String sourceTable = "AdSpaceCategoryM";
		sLogger.info("Entering to get the ad slot category ...");						
		poolName = config.get("RTBPoolName", poolName);
		sLogger.info("Export to MySql Pool Name: " + poolName);		
		sourceTable = config.get("AdSpaceCategoryTable", sourceTable);
		sLogger.info("AdSpace Category Table: " + sourceTable);							        
		Connection con = new JdbcOperations(config, poolName).getConnection();	            
		try {										
			return GetCategoryMap(poolName, sourceTable, con, "adspace_id", "category_id");
		}
		finally {
			con.close();
		}
	}    
	
	private Map<String, List<String>> GetCategoryMap(String poolName, String sourceTable, Connection con, 
			String keyName1, String keyName2) 
			throws SQLException, ParseException, Exception{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Map<String, List<String>> categoryMap = new HashMap<String, List<String>>();
		try {										
			String selectMapQuery = "select " + keyName1 + ", " + keyName2 + " from " + sourceTable + " where flag = 0";					
			sLogger.info("Select mapping data query: " + selectMapQuery);				
			stmt = con.prepareStatement(selectMapQuery);
			rs = stmt.executeQuery();
			List<String> categoryIdList = new ArrayList<String>();
			if(rs!=null){
				while (rs.next()) {
					int i_cnt = 0;
					String keyId = rs.getString(keyName1);
					String categoryId = rs.getString(keyName2);
					//sLogger.info("keyId: " + keyId + "   categoryId: " + categoryId);
					if (categoryMap.containsKey(keyId)) {
						categoryIdList = (List<String>) categoryMap.get(keyId);
						i_cnt = categoryIdList.size();
					}else{
						categoryIdList = new ArrayList<String>();
						i_cnt = 0;						
					}
					categoryIdList.add(i_cnt, categoryId);
					categoryMap.put(keyId, categoryIdList);
				}
			}else{
				sLogger.warn("It is null for the mapping of key and category");
			}				
			return categoryMap;
		}
		finally {
			rs.close();
			stmt.close();
		}
	}    	
}