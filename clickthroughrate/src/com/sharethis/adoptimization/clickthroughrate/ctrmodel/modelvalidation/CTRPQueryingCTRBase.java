package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.common.JdbcOperations;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPQueryingCTRBase
{
	private static final Logger sLogger = Logger.getLogger(CTRPQueryingCTRBase.class);
	
	private String[] keyNames = null;
	
	private double[] ctrBase;
	
	public Map<String,Map<String,Double>> quaryingCTRBase(Configuration config, 
			String modelFeature) throws Exception{
		String dataTableName = config.get("ctrp_base_table");
		//String modelFeature = config.get("ctrp_model_feature");			
		keyNames = StringUtils.split(modelFeature, ";");
		String keyList = "'" + keyNames[0] + "'";
		for(int i=1; i<keyNames.length; i++){
			keyList = keyList + ",'" + keyNames[i] + "'";
		}

		String poolName = config.get("PoolName");
		Connection conn = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String selectQuery = "select * from " + dataTableName + " where key_name in (" + keyList + ")";
			stmt = conn.prepareStatement(selectQuery);
			//sLogger.info("Select ctr base query: " + stmt.toString());
			rs = stmt.executeQuery();
			Map<String, Map<String,Double>> allMap = new HashMap<String, Map<String,Double>>(0);
			Map<String, Double> keyValMap = new HashMap<String, Double>(0);
			if(rs!=null){
				while (rs.next()) {
					String keyName = rs.getString("key_name");
					String keyValue = rs.getString("key_value_0");
					double ctr = rs.getDouble("ctr");
					if(allMap.containsKey(keyName)){
						keyValMap = allMap.get(keyName);
						keyValMap.put(keyValue, ctr);
						allMap.put(keyName, keyValMap);
					}else{
						keyValMap = new HashMap<String, Double>(0);
						keyValMap.put(keyValue, ctr);
						allMap.put(keyName, keyValMap);
					}
				}
			}
			return allMap;
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	} 

	public Map<String,Map<String,Double>> quaryingCTRBase(Configuration config, 
			String[] keyNames) throws Exception{
		String dataTableName = config.get("CTRPBaseTable");
		String keyList = "'" + keyNames[0] + "'";
		for(int i=1; i<keyNames.length; i++){
			keyList = keyList + ",'" + keyNames[i] + "'";
		}

		String poolName = config.get("PoolName");
		Connection conn = new JdbcOperations(config, poolName).getConnection();	            
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String selectQuery = "select * from " + dataTableName + " where key_name in (" + keyList + ")";
			stmt = conn.prepareStatement(selectQuery);
			//sLogger.info("Select ctr base query: " + stmt.toString());
			rs = stmt.executeQuery();
			Map<String, Map<String,Double>> allMap = new HashMap<String, Map<String,Double>>(0);
			Map<String, Double> keyValMap = new HashMap<String, Double>(0);
			if(rs!=null){
				while (rs.next()) {
					String keyName = rs.getString("key_name");
					String keyValue = rs.getString("key_value_0");
					double ctr = rs.getDouble("ctr");
					if(allMap.containsKey(keyName)){
						keyValMap = allMap.get(keyName);
						keyValMap.put(keyValue, ctr);
						allMap.put(keyName, keyValMap);
					}else{
						keyValMap = new HashMap<String, Double>(0);
						keyValMap.put(keyValue, ctr);
						allMap.put(keyName, keyValMap);
					}
				}
			}
			return allMap;
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	} 
	
	public double[] quaryingCTRBase(Map<String, Map<String, Double>> ctrBaseMap, 
			String[] keyNames, String[] keyValues){
		ctrBase = new double[keyNames.length];
		//sLogger.info("CTR base map: " + ctrBaseMap.toString());
		for(int i=0; i<keyNames.length; i++){
			//sLogger.info("Key Name: " + keyNames[i]);
			Map<String, Double> keyValMap = ctrBaseMap.get(keyNames[i]);
			if(keyValMap.containsKey(keyValues[i]))
				ctrBase[i] = (double) keyValMap.get(keyValues[i]);
			else
				ctrBase[i] = (double) keyValMap.get("-99");					
		}
		return ctrBase;
	} 

	public String[] getKeyNames(){
		return keyNames;
	}

	public double[] getCTRBase(){
		return ctrBase;
	}
	
	public static void main(String[] args) {
		try{
			Configuration config = ConfigurationUtil.setConf(args);

			sLogger.info("Loading the CTR base from db table starts ...");
			CTRPQueryingModel qModel = new CTRPQueryingModel();
			sLogger.info("Loading the CTR predictive model from db table starts ...");
			String modelStr = qModel.loadingModelMap(config);
			double intercept = qModel.getIntercept();
			double[] modelCoeffs = qModel.getModelCoeffs();
			String[] keyNames = qModel.getKeyNames();
			sLogger.info("Loading the CTR predictive model from db table is done.");
			CTRPQueryingCTRBase qCTR = new CTRPQueryingCTRBase();
			Map<String, Map<String,Double>> ctrBaseMap = qCTR.quaryingCTRBase(config, keyNames);
			
			String[] keyValues = null;
			String inPath = "input/ctr_key_value.txt";
			  
			sLogger.info("CTR key file path:" + inPath);		
			FileInputStream fstream = new FileInputStream(inPath);			  
			// Get the object of DataInputStream			  
			DataInputStream in = new DataInputStream(fstream);			  
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			 
			String outPath = "output/ctr_base_file.txt";
			sLogger.info("CTR base file path:" + outPath);		
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outPath)));
			String line = null;
			int numLine = 0;
			while ((line = reader.readLine()) != null && numLine <= 10){
				keyValues = StringUtils.split(line, "\t");
				//sLogger.info("Key Values:");
				//for(int i=0; i<keyValues.length; i++)
				//	sLogger.info(keyValues[i]);
				
				double[] ctrBase = qCTR.quaryingCTRBase(ctrBaseMap, keyNames, keyValues);
				//sLogger.info("CTR base:");
				//for(int i=0; i<ctrBase.length; i++)
				//	sLogger.info(ctrBase[i]);	
				for(int i=0; i<keyNames.length; i++)
					bw.write(keyNames[i] +":" + keyValues[i] + ":" + ctrBase[i] + "\n");
				numLine++;
			}		
			reader.close();
			bw.close();			
			sLogger.info("Loading the CTR base from db table is done!");
		}
		catch(Exception e) {
			sLogger.error(e);
		}		
		System.exit(0);
	}
}
