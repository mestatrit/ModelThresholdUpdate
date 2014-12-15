package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.Map;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPProcessingCTRBase extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPProcessingCTRBase.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPProcessingCTRBase(Map<String, Tap> sources, Configuration config, String pDateStr, 
			String ctrFilePath, String modFilePath, String ctrPipeNamesList, String ctrKeyFieldsList, 
			 String all_id, String infile_postfix) throws Exception{
		try{			
		    String[] pipeNamesArr = StringUtils.split(ctrPipeNamesList, ";");
			String[] keyFieldsArr = StringUtils.split(ctrKeyFieldsList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = new Pipe[2*numOfPipes];

			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
							    	
	    	int modThresholdVal = config.getInt("CTRPThresholdModel", 5000);
	    	int baseThresholdVal = config.getInt("CTRPThresholdData", 1000);
	    	double ctrThresholdVal = config.getFloat("CTRPThresholdFilter", 0.01f);
			
	    	Fields ctrFields = new Fields("ctr");

	    	String masKeyVal = config.get("CTRPDefaultKeyId");
	    	//Loading the data for the independent variables;
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	Fields keyFieldsMore_i = new Fields();
		    	int numKeyFields = keyFieldsMoreArr.length;
		    	for(int j=0; j<numKeyFields;j++){
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    		keyFieldsMore_i = keyFieldsMore_i.append(new Fields(keyFieldsMoreArr[j]+"_"+i));
		    	}
		    	Fields xFields = keyFieldsMore.append(dataFields);
		    	Class[] types = new Class[xFields.size()];
		    	for(int j=0; j<numKeyFields;j++){
		    		types[j] = String.class;
		    	}
		    	for(int j=numKeyFields; j<xFields.size()-1; j++){
		    		types[j] = Double.class;
		    	}
		    	types[xFields.size()-1] = String.class;
		    	
		    	ctrFields = ctrFields.append(new Fields("ctr_"+i));
		    	
		    	Pipe ctrAssembly_tmp = new LoadingCTRDataForModels(sources, ctrFilePath, pDateStr, 
		    			config, pipeNamesArr[i], xFields, types, infile_postfix).getTails()[0];
		    			    	
		    	Pipe[] outAssembly = new CTRPMassagingCTRBaseData(keyFieldsMore_i, keyFieldsMore,
		    				ctrAssembly_tmp, modThresholdVal, baseThresholdVal, ctrThresholdVal, 
		    				all_id, i, masKeyVal).getTails();
		    		
		    	ctrAssembly[i] = new Pipe(outAssembly[1].getName(), outAssembly[1]);		    		
		    	ctrAssembly[numOfPipes+i] = new Pipe(outAssembly[2].getName(), outAssembly[2]);		    				    		
		    }
			setTails(ctrAssembly);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
