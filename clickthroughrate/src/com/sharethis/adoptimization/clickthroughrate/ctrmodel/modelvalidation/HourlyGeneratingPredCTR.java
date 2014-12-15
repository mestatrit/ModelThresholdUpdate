package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

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
import com.sharethis.adoptimization.common.CTRConstants;


/**
 * This is the class to run the flow.
 */

public class HourlyGeneratingPredCTR 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingPredCTR.class);	
	
	public HourlyGeneratingPredCTR(){
	}
		
	public void generatingPredCTRHourly(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String icFilePath = config.get("IcFilePath");
			String icFileName = config.get("IcFileName");

			//String mvFilePath = config.get("MVFilePathHourly");
			String mvFileName = config.get("MVFileName");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\nIcFilePath = " + icFilePath +
					"\nIcFileName = " + icFileName +
					"\nMVFileName = " + mvFileName);
			generatingPredCTROneHour(config, pDateStr, outFilePathHourly, 
					hour, icFilePath, icFileName);
			generatingMVCTROneHour(config, pDateStr, hour, outFilePathHourly, mvFileName);			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		

	private void generatingPredCTROneHour(Configuration config, String pDateStr, String outFilePath, 
			int hour, String icFilePath, String icFileName) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			String hourFolder = CTRConstants.hourFolders[hour];
			sLogger.info("\nStarting to generate the hourly predicted ctr at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    
		    Pipe[] dataAssembly = new HourlyDataForModelValidation(sources_hourly, icFilePath, icFileName, 
					pDateStr, hour, config).getTails();	
		        				
		    if(!(dataAssembly==null)){			    
		    	for(int i=0; i<dataAssembly.length; i++){
		    		String allPipeName = dataAssembly[i].getName();
		    		String fileName = outFilePathDate + allPipeName + "_hourly";
		    		sLogger.info("pipeName: " + allPipeName + "    fileName: " + fileName);
		    		adoptUtils.deleteFile(fileName);
		    		Scheme sinkScheme = new TextLine(Compress.ENABLE);
		    		sinkScheme.setNumSinkParts(numOfReducersInt);
		    		Tap sink = new Hfs(sinkScheme, fileName);
		    		sinks_hourly.put(allPipeName, sink);
		    	}	    	
		        sLogger.info("Connecting the hourly model validation ... ");		    	
		        long time_s0 = System.currentTimeMillis();		    	
		        Properties properties = new Properties();				
		        properties.setProperty("mapred.job.queue.name", config.get("QueueName"));		    	
		        properties.setProperty("mapred.reduce.tasks", numOfReducers);
			   
		        AppProps.setApplicationJarClass(properties, HourlyGeneratingPredCTR.class);			    
		        FlowConnector flowConnector = new HadoopFlowConnector(properties);			    
		        Flow flow = flowConnector.connect("ctr_pred_hourly", sources_hourly, sinks_hourly, dataAssembly);
		        flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingPredCTROneHour");
		    	sLogger.info("Generating the hourly predicted ctr at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is no ic_pb data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  
	
	private void generatingMVCTROneHour(Configuration config, String pDateStr, int hour, 
			String mvFilePath, String mvFileName) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = 1;
			String hourFolder = CTRConstants.hourFolders[hour];
			sLogger.info("\nStarting to generate the hourly mv ctr at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = mvFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    
		    Pipe[] dataAssembly = new HourlyBucketDataForModelValidation(sources_hourly, mvFilePath, mvFileName, 
					pDateStr, hour, config).getTails();	
		        				
		    if(!(dataAssembly==null)){			    
		    	for(int i=0; i<dataAssembly.length; i++){
		    		String allPipeName = dataAssembly[i].getName();
		    		String fileName = outFilePathDate + allPipeName + "_hourly";
		    		sLogger.info("pipeName: " + allPipeName + "    fileName: " + fileName);
		    		adoptUtils.deleteFile(fileName);
		    		Scheme sinkScheme = new TextLine(Compress.ENABLE);
		    		sinkScheme.setNumSinkParts(numOfReducersInt);
		    		Tap sink = new Hfs(sinkScheme, fileName);
		    		sinks_hourly.put(allPipeName, sink);
		    	}	    	
		        sLogger.info("Connecting the hourly model validation - bucketizing ... ");		    	
		        long time_s0 = System.currentTimeMillis();		    	
		        Properties properties = new Properties();				
		        properties.setProperty("mapred.job.queue.name", config.get("QueueName"));		    	
		        properties.setProperty("mapred.reduce.tasks", numOfReducers);
			   
		        AppProps.setApplicationJarClass(properties, HourlyGeneratingPredCTR.class);			    
		        FlowConnector flowConnector = new HadoopFlowConnector(properties);			    
		        Flow flow = flowConnector.connect("ctr_mv_hourly", sources_hourly, sinks_hourly, dataAssembly);
		        flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingMVCTROneHour");
		    	sLogger.info("Generating the hourly mv ctr at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is no base data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  			
}
