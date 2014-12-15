package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.Map;

import cascading.operation.Filter;
import cascading.operation.Identity;
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

import com.sharethis.adoptimization.common.FilterOutNullData;

/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPProcessingModelData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPProcessingModelData.class);
	private static final long serialVersionUID = 1L;
	
	public CTRPProcessingModelData(Map<String, Tap> sources, Configuration config, String pDateStr, 
			String ctrFilePath, String modFilePath, String dataYPipeName, String dataYKeyFields, 
			String pipeNamesList, String keyFieldsList, String dataFileName, 
			String dataFilterField, String dataFilterValue, String all_id,
			String infile_postfix) throws Exception{
		try{			
			String baseFilePath = config.get("CTRBaseFilePath");
			String base_infile_postfix="_base";
			String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
		    			
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
	    	
	    	Pipe ctrAssembly = new Pipe(dataFileName, new LoadingCTRDataForModels(sources, ctrFilePath, pDateStr, 
	    			config, dataYPipeName, yFields, yTypes, infile_postfix).getTails()[0]);

	    	if(!"null".equalsIgnoreCase(dataFilterField)&&dataFilterField!=null&&!dataFilterField.isEmpty()){
	    		Filter<?> dataFilter = new FilterOutNullData();
	    		ctrAssembly = new Each(ctrAssembly, new Fields(dataFilterField), dataFilter); 
	    	}
	    	
	    	int modThresholdVal = config.getInt("CTRPThresholdModel", 1000);
			
			//Massaging the ctr if sum_imps <= thresholdVal
			Fields yNewField = new Fields(all_id);
	    	ctrAssembly = new CTRPFilteringData(ctrAssembly, 
	    			keyFields, modThresholdVal, yNewField).getTails()[0];

			Fields allFields = keyFields.append(yNewField);
			allFields = allFields.append(new Fields("sum_imps","ctr"));

	    	Fields ctrFields = new Fields("ctr");

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
				Fields dataFields_i = new Fields("sum_imps_"+i, "sum_clicks_"+i, "ctr_"+i);

		    	Fields xFields = keyFieldsMore_i.append(dataFields_i);
		    	Class[] types = new Class[xFields.size()];
		    	for(int j=0; j<numKeyFields;j++){
		    		types[j] = String.class;
		    	}
		    	for(int j=numKeyFields; j<xFields.size()-1; j++){
		    		types[j] = Double.class;
		    	}
		    	types[xFields.size()-1] = String.class;
		    	
		    	ctrFields = ctrFields.append(new Fields("ctr_"+i));
		    	
		    	Pipe ctrXAssembly = new LoadingCTRDataForModels(sources, baseFilePath, pDateStr, 
		    			config, pipeNamesArr[i], xFields, types, base_infile_postfix).getTails()[0];
		    	
		    	//Keeping the fields defined in keptFields.
		    	Fields keptFields = new Fields();
		    	keptFields = keptFields.append(keyFieldsMore_i);
		    	keptFields = keptFields.append(new Fields("ctr_"+i));
		    	ctrXAssembly = new Each(ctrXAssembly, keptFields, new Identity());

		    	allFields = allFields.append(keptFields);

		    	// Building the pipe to join x-data into y-data	
		    	ctrAssembly = new CoGroup(ctrAssembly.getName(), ctrAssembly, 
		    			keyFieldsMore, ctrXAssembly, keyFieldsMore_i, allFields, new LeftJoin());

		    }
    		ctrAssembly = new Unique(ctrAssembly, ctrFields, 100000);
    		
			setTails(ctrAssembly);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
