package com.sharethis.adoptimization.brandlift.aggregating;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.sharethis.adoptimization.common.DataWeightedFunction;


/**
 * This is the class to run the flow.
 */

public class PeriodicalAggregatingBLPipe extends SubAssembly
{	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger sLogger = Logger.getLogger(PeriodicalAggregatingBLPipe.class);	
				
	public PeriodicalAggregatingBLPipe(Map<String, Tap> sources_agg, Configuration config, 
			String pDateStr, String outFilePathDaily, String pipeNamesList, String keyFieldsList) throws Exception{
		try{
			sLogger.info("\nStarting to generate the periodical BL on " + pDateStr + " ...");			
			String[] dataFieldsArr = new String[]{"sum_imps", "sum_clicks", "sum_cost", "sum_vote"};
			Fields dataFields = new Fields(dataFieldsArr[0], dataFieldsArr[1], dataFieldsArr[2], dataFieldsArr[3]);
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;

	    	Pipe[] assemblyAgg = new Pipe[2*numOfPipes];

		    for(int i_pipe=0; i_pipe<numOfPipes; i_pipe++){
		    	String[] baseKeyFieldsArr = StringUtils.split(keyFieldsArr[i_pipe], ",");
		    	Fields keyFields = new Fields();
		    	for(int j=0; j<baseKeyFieldsArr.length;j++){
		    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
		    	}
		    	Fields baseFields = keyFields.append(dataFields);
		    	Class[] types = new Class[baseFields.size()];
		    	for(int j=0; j<baseKeyFieldsArr.length;j++){
		    		types[j] = String.class;
		    	}
		    	for(int j=baseKeyFieldsArr.length; j<baseFields.size()-1; j++){
		    		types[j] = Double.class;
		    	}
		    	types[baseFields.size()-1] = String.class;
	    		    	
		    	Pipe[] blAssemblyDaily = new PeriodicalUploadingBLData(sources_agg, 
		    			outFilePathDaily, pDateStr, 7, 0, config, baseFields, 
		    			pipeNamesArr[i_pipe], types, "_daily").getTails();

		    	if(blAssemblyDaily!=null){
		    		assemblyAgg[i_pipe] = new DailyAggregatingBLBase(blAssemblyDaily, new String[]{pipeNamesArr[i_pipe]+"_weekly"}, 
		    				new String[]{keyFieldsArr[i_pipe]}, dataFieldsArr, config).getTails()[0];
		    	}

		    	if(assemblyAgg[i_pipe]!=null){
			    	Pipe prevAssemblyAgg = new PeriodicalUploadingBLData(sources_agg, 
			    			outFilePathDaily, pDateStr, 1, 7, config, baseFields, 
			    			pipeNamesArr[i_pipe], types, "_agg").getTails()[0];

			    	if(prevAssemblyAgg!=null){
			    		if(!prevAssemblyAgg.getName().equalsIgnoreCase("No_Data")){
			    			Function<?> wFunc = new DataWeightedFunction(baseFields, baseFields, keyFields.size(), dataFields.size(), 1);
			    			prevAssemblyAgg = new Each(prevAssemblyAgg, baseFields, wFunc, Fields.RESULTS);

			    			assemblyAgg[numOfPipes+i_pipe] = new DailyAggregatingBLBase(new Pipe[]{assemblyAgg[i_pipe], prevAssemblyAgg}, 
			    					new String[]{pipeNamesArr[i_pipe]+"_agg"}, new String[]{keyFieldsArr[i_pipe]},
			    					dataFieldsArr, config).getTails()[0];
			    		}else{
				    		assemblyAgg[numOfPipes+i_pipe] = new Pipe(pipeNamesArr[i_pipe]+"_agg", assemblyAgg[i_pipe]);			    			
			    		}
			    	}else{
			    		assemblyAgg[numOfPipes+i_pipe] = new Pipe(pipeNamesArr[i_pipe]+"_agg", assemblyAgg[i_pipe]);
			    	}
		    	}else{
					sLogger.info("The aggregation of " + pipeNamesArr[i_pipe] + " on " + pDateStr + " fialed.");
					throw new Exception("No aggregation data for " + pipeNamesArr[i_pipe] + " on " + pDateStr);
		    	}
		    }   
		    setTails(assemblyAgg);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
}
