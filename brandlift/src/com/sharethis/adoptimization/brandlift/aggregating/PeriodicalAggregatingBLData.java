package com.sharethis.adoptimization.brandlift.aggregating;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;

import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the class to run the flow.
 */

public class PeriodicalAggregatingBLData 
{	
	private static final Logger sLogger = Logger.getLogger(PeriodicalAggregatingBLData.class);	
	
	public PeriodicalAggregatingBLData(){
	}
		
	public void aggregatingBLDataPeriodical(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathDaily = config.get("OutFilePathDaily");
			String pipeNamesListBL = config.get("PipeNamesListPeriodicalBL");
			String keyFieldsListBL = config.get("KeyFieldsListPeriodicalBL");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathDaily = " + outFilePathDaily +
					"\nPipeNamesListPeriodicalBL = " + pipeNamesListBL +
					"\nKeyFieldsListPeriodicalBL = " + keyFieldsListBL);

			if(pipeNamesListBL==null || "null".equalsIgnoreCase(pipeNamesListBL) || pipeNamesListBL.isEmpty()
					||keyFieldsListBL==null || "null".equalsIgnoreCase(keyFieldsListBL) || keyFieldsListBL.isEmpty()){
				String errMsg = "PipeNamesListDailyBL or KeyFieldsListDailyBL is not defined or empty!";
				sLogger.info(errMsg);
			}
			
			aggregatingPeriodicalBLAll(config, pDateStr, outFilePathDaily, pipeNamesListBL, 
					keyFieldsListBL);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingPeriodicalBLAll(Configuration config, String pDateStr, 
			String outFilePathDaily, String pipeNamesList, String keyFieldsList) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the periodical BL on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();

	    	Pipe[] allAssembly = new PeriodicalAggregatingBLPipe(sources_agg, config, 
	    			pDateStr, outFilePathDaily, pipeNamesList, keyFieldsList).getTails();
	    	
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";			
            adoptUtils.makeFileDir(outFilePathDate);
            
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();		
			for(int i=0; i<allAssembly.length; i++){				
				String pipeName = allAssembly[i].getName();
				String fileName = null;
				fileName = outFilePathDate + pipeName;					
				sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine();
	    		sinkScheme = new TextLine(Compress.ENABLE);
				sinkScheme.setNumSinkParts(5);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeName, sink);								
		    }
			sLogger.info("Connecting the periodical aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			//properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, PeriodicalAggregatingBLData.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("bl_period_aggregating", sources_agg, sinks_agg, allAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingPeriodicalCTR");
			sLogger.info("Generating the periodical ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
}
