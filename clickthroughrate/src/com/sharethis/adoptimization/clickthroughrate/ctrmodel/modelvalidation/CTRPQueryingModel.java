package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sharethis.adoptimization.common.ConfigurationUtil;
import com.sharethis.adoptimization.common.JdbcOperations;

/**
 * This is the assembly to load the CTR predictive model from the table.
 */

public class CTRPQueryingModel
{
	private static final Logger sLogger = Logger.getLogger(CTRPQueryingModel.class);

	private String[] keyNames = null;
	private String[] keyIds = null;
	private double[] modelCoeffs;
	private double intercept = 0;

	public Map<String, Double> quaryingModel(Configuration config) throws Exception{
		String poolName = config.get("PoolName");
		Connection conn = new JdbcOperations(config, poolName).getConnection();	            
		String modelTableName = config.get("CTRPModelTable");	
		String modelName = config.get("CTRPModelName");
		String modelFeatureList = config.get("CTRPModelFeatureList");
		String modelFeatureIdList = config.get("CTRPModelFeatureIdList");
		String[] modelFeatureAll = StringUtils.split(modelFeatureList, ";");
		String[] modelFeatureIdAll = StringUtils.split(modelFeatureIdList, ";");
		
		//sLogger.info("Model Table Name: " + modelTableName);
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Map<String, Double> modelMap = new HashMap<String, Double>();
		try {
			String selectQuery = "select * from " + modelTableName + " where model_type = 0 and model_flag = 1";
			if(modelName!=null&&!"null".equalsIgnoreCase(modelName)&&!modelName.isEmpty())
				selectQuery = "select * from " + modelTableName + " where model_type = 0 and model_name = '" + modelName + "'";
			stmt = conn.prepareStatement(selectQuery);
			sLogger.info("Select a model query: " + stmt.toString());
			rs = stmt.executeQuery();
			if(rs!=null){
				if (rs.next()) {
					modelName = rs.getString("model_name");
					String modelFeature = modelName.replaceAll("overall_", "");
					keyNames = StringUtils.split(modelFeature, "_");
					keyIds = new String[keyNames.length];
					//sLogger.info("Model Name: " + modelName);
					modelCoeffs = new double[keyNames.length];
					intercept = rs.getDouble("intercept");
					modelMap.put("intercept", intercept);
					for(int i=0; i<keyNames.length; i++){
						//sLogger.info("Model Features: " + keyNames[i]);
						modelCoeffs[i] = rs.getDouble(keyNames[i]+"_coeff");
						modelMap.put(keyNames[i], rs.getDouble(keyNames[i]+"_coeff"));
						for(int j=0; j<modelFeatureAll.length; j++){
							if(modelFeatureAll[j].equalsIgnoreCase(keyNames[i]))
								keyIds[i] = modelFeatureIdAll[j];
						}
						//sLogger.info("Model Feature Ids: " + keyIds[i]);
					}
				}
			}
			return modelMap;
		}
		finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	} 

	public String loadingModelMap(Configuration config) throws Exception{
		Map<String, Double> modelMap = quaryingModel(config);
		if (modelMap == null || modelMap.size() == 0) return null;

        return new JSONObject(modelMap).toString();
	}
	
	public String[] getKeyNames(){
		return keyNames;
	}
	
	public double[] getModelCoeffs(){
		return modelCoeffs;
	}
	
	public double getIntercept(){
		return intercept;
	}
	
	public String[] getKeyIds() {
		return keyIds;
	}

	public void setKeyIds(String[] keyIds) {
		this.keyIds = keyIds;
	}

	public static void main(String[] args) {
		try{					
//			String[] args1 = new String[1];
//			args1[0]="bin/res/ctr_mdoel.properties";			
			Configuration config = ConfigurationUtil.setConf(args);
			//Configuration config = ConfigurationUtil.getConfig("bin/res/ctr_model.properties");
			CTRPQueryingModel qModel = new CTRPQueryingModel();
			sLogger.info("Loading the CTR predictive model from db table starts ...");
			String modelStr = qModel.loadingModelMap(config);
			double intercept = qModel.getIntercept();
			double[] modelCoeffs = qModel.getModelCoeffs();
			String[] keyNames = qModel.getKeyNames();
			String[] keyIds = qModel.getKeyIds();
			
			sLogger.info("Model Coefficients");
			sLogger.info(intercept);
			for(int i=0; i<modelCoeffs.length; i++)
				sLogger.info(modelCoeffs[i]);
			sLogger.info("Loading the CTR predictive model from db table is done!");
			
			String path = "output/ctr_model_file.txt";
			sLogger.info("CTR predictive model file path:" + path);		
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			bw.write(modelStr+"\n");
			for(int i=0; i<keyNames.length; i++){
				bw.write(keyNames[i] + "  " + keyIds[i] + "\n");
			}
			bw.close();
		}
		catch(Exception e) {
			sLogger.error(e);
		}		
		System.exit(0);
	}
}
