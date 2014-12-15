package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;

import com.sharethis.adoptimization.clickthroughrate.AggregatingCTRAll;
import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the class to run the flow.
 */

public class AggregatingCTRDataForModels 
{	
	private static final Logger sLogger = Logger.getLogger(AggregatingCTRDataForModels.class);	
	
	public AggregatingCTRDataForModels(){
	}
		
	public void aggregatingCTRData(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String ctrFilePath = config.get("CTRFilePath");
			String basePipeName = config.get("BasePipeName");
			String baseKeyFields = config.get("BaseKeyFields");
			String pipeNamesList = config.get("AggPipeNamesList");
			String keyFieldsList = config.get("AggKeyFieldsList");
			String infile_postfix = config.get("BaseFilePostfix"); //"_agg";
			String outfile_postfix = config.get("AggFilePostfix"); // "_agg";
			if(basePipeName==null||"null".equalsIgnoreCase(basePipeName)||basePipeName.isEmpty()
					||pipeNamesList==null||"null".equalsIgnoreCase(pipeNamesList)||pipeNamesList.isEmpty()){
				sLogger.info("No aggregation is defined or needed for models!");
			}else{
				sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
						"\nCTRFilePath = " + ctrFilePath +
						"\nBasePipeName = " + basePipeName +
						"\nBaseKeyFields = " + baseKeyFields +
						"\nAggPipeNamesList = " + pipeNamesList +
						"\nAggKeyFieldsList = " + keyFieldsList);
				aggregatingCTRData(config, pDateStr, ctrFilePath, basePipeName, 
						baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingCTRData(Configuration config, String pDateStr, String ctrFilePath, 
			String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to aggregate the ctr data for models on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = new Pipe[numOfPipes];
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields pFields = keyFields.append(dataFields);
	    	Class[] types = new Class[pFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-1] = String.class;
	    		    	
		    Pipe ctrAssembly_tmp = new LoadingCTRDataForModels(sources_agg, ctrFilePath, pDateStr, 
	    			config, basePipeName, pFields, types, infile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = ctrFilePath+dayStr+"/";			
            adoptUtils.makeFileDir(outFilePathDate);
            
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();		
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];
				String fileName = outFilePathDate + pipeNamesArr[i] + outfile_postfix;
				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine(Compress.ENABLE);
				if(pipeNamesArr[i].length()<20)
					sinkScheme.setNumSinkParts(5);
				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);				
		    }
		    
			sLogger.info("Connecting the aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, AggregatingCTRDataForModels.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("agg_ctr_for_model", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingCTRDataForModels");
			sLogger.info("Aggregating the ctr data for models on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
}
