package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.Map;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.clickthroughrate.ConstructingCTRKeys;
import com.sharethis.adoptimization.common.FilterOutDataLEDouble;
import com.sharethis.adoptimization.common.FilterOutNullData;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPProcessingData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPProcessingData.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPProcessingData(Map<String, Tap> sources, Configuration config, String pDateStr, 
			String ctrFilePath, String modFilePath, String dataYPipeName, String dataYKeyFields, 
			String pipeNamesList, String keyFieldsList, String dataFileName, 
			String dataFilterField, String dataFilterValue, String all_id,
			String infile_postfix) throws Exception{
		try{			
			String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = new Pipe[2*numOfPipes+1];
		    			
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
						
			//Loading the data for the dependent variable.
	    	String[] keyFieldsStr = StringUtils.split(dataYKeyFields, ",");
	    	Fields keyFields = new Fields();
	    	for(int j=0; j<keyFieldsStr.length;j++)
	    		keyFields = keyFields.append(new Fields(keyFieldsStr[j]));
	    	Fields yFields = keyFields.append(dataFields);
	    	Class[] yTypes = new Class[yFields.size()];
	    	for(int j=0; j<keyFieldsStr.length;j++){
	    		yTypes[j] = String.class;
	    	}
	    	for(int j=keyFieldsStr.length; j<yFields.size()-1; j++){
	    		yTypes[j] = Double.class;
	    	}
	    	yTypes[yFields.size()-1] = String.class;
	    	
	    	ctrAssembly[2*numOfPipes] = new Pipe(dataFileName, new LoadingCTRDataForModels(sources, ctrFilePath, pDateStr, 
	    			config, dataYPipeName, yFields, yTypes, infile_postfix).getTails()[0]);
	    	
	    	if(!"null".equalsIgnoreCase(dataFilterField)&&dataFilterField!=null&&!dataFilterField.isEmpty()){
	    		Filter<?> dataFilter = new FilterOutNullData();
	    		ctrAssembly[2*numOfPipes] = new Each(ctrAssembly[2*numOfPipes], new Fields(dataFilterField), dataFilter); 
	    	}
	    	
	    	int modThresholdVal = config.getInt("CTRPThresholdModel", 1000);
	    	int baseThresholdVal = config.getInt("CTRPThresholdData", 1000);
	    	double ctrThresholdVal = config.getFloat("CTRPThresholdFilter", 0.02f);
			
			//Massaging the ctr if sum_imps <= thresholdVal
			Fields yNewField = new Fields(all_id);
	    	ctrAssembly[2*numOfPipes] = new CTRPFilteringData(ctrAssembly[2*numOfPipes], 
	    			keyFields, modThresholdVal, yNewField).getTails()[0];

			Fields allFields = keyFields.append(yNewField);
			allFields = allFields.append(new Fields("sum_imps","ctr"));

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
		    	
		    	if(!config.getBoolean("MassagingFlag", false)){
				    String ctrAssemblyName = ctrAssembly_tmp.getName();
		    		ctrAssembly[i] = new Pipe(ctrAssemblyName+"_base", ctrAssembly_tmp);
//				    Filter<?> filter = new FilterOutDataLEDouble(thresholdVal);
//				    ctrAssembly[i] = new Each(ctrAssembly[i], new Fields("sum_imps"), filter); 
		    		Fields baseDataFields = new Fields("sum_imps", "sum_clicks", "ctr");
		    		Fields keptFields = keyFieldsMore.append(baseDataFields);
		    		ctrAssembly[i] = new Each(ctrAssembly[i], keptFields, new Identity());

		    		ctrAssembly[numOfPipes+i] = new Pipe("rtb_" + ctrAssemblyName + "_base", ctrAssembly[i]);

		    		//Constructing the data file for rtb to pick up the ctr base data.
					Fields resultFields_i1 = new Fields("type", "keys");
					Fields allFields_i1 = resultFields_i1.append(baseDataFields);
					
					String ctrType = ctrAssemblyName;
					///////////////////////////////////////////////////////////////////
					//The following lines are added to support the special type 'crtv' for rtb
					if(ctrAssemblyName.equalsIgnoreCase("crtv_adg"))
						ctrType = "crtv";
					if(ctrAssemblyName.equalsIgnoreCase("crtv"))
						ctrType = "crtv_0";

					if(ctrAssemblyName.equalsIgnoreCase("set")||ctrAssemblyName.equalsIgnoreCase("net")){
					    Filter<?> ctrFilter = new FilterOutDataLEDouble(ctrThresholdVal);
					    ctrAssembly[numOfPipes+i] = new Each(ctrAssembly[numOfPipes+i], new Fields("ctr"), new Not(ctrFilter)); 
					}
					
					////////////////////////////////////////////////////////////////////
					
					Function<?> keyFunc = new ConstructingCTRKeys(resultFields_i1, ctrType);
					ctrAssembly[numOfPipes+i] = new Each(ctrAssembly[numOfPipes+i], keyFieldsMore, keyFunc, allFields_i1);		    		

				    Filter<?> filter = new FilterOutDataLEDouble(modThresholdVal);
				    ctrAssembly[i] = new Each(ctrAssembly[i], new Fields("sum_imps"), filter); 
					
		    		ctrAssembly_tmp = new Each(ctrAssembly_tmp, keyFieldsMore.append(new Fields("ctr")), new Identity());

		    		Fields outFields = keyFieldsMore_i.append(new Fields("ctr_"+i));
		    		allFields = allFields.append(outFields);	
		    		
		    		// Building the pipe to join x-data into y-data	
		    		ctrAssembly[2*numOfPipes] = new CoGroup(ctrAssembly[2*numOfPipes].getName(), ctrAssembly[2*numOfPipes], 
		    				keyFieldsMore, ctrAssembly_tmp, keyFieldsMore, allFields, new LeftJoin());	
		    	}else{
		    		Pipe[] outAssembly = new CTRPMassagingModelData(keyFieldsMore_i, keyFieldsMore,
		    				ctrAssembly_tmp, modThresholdVal, baseThresholdVal, ctrThresholdVal, 
		    				all_id, i, masKeyVal).getTails();
		    		
		    		ctrAssembly_tmp = new Pipe(outAssembly[0].getName(), outAssembly[0]); 		    		
		    		ctrAssembly[i] = new Pipe(outAssembly[1].getName(), outAssembly[1]);		    		
		    		ctrAssembly[numOfPipes+i] = new Pipe(outAssembly[2].getName(), outAssembly[2]);		    		
		    		
		    		allFields = allFields.append(keyFieldsMore_i);
		    		allFields = allFields.append(new Fields("ctr_"+i));	

		    		// Building the pipe to join x-data into y-data	
		    		ctrAssembly[2*numOfPipes] = new CoGroup(ctrAssembly[2*numOfPipes].getName(), ctrAssembly[2*numOfPipes], 
		    			keyFieldsMore, ctrAssembly_tmp, keyFieldsMore_i, allFields, new LeftJoin());
		    	}
		    }
    		ctrAssembly[2*numOfPipes] = new Unique(ctrAssembly[2*numOfPipes], ctrFields, 100000);
    		
			setTails(ctrAssembly);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
