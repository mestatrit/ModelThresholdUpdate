package com.sharethis.adoptimization.brandlift.parsing;

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
import com.sharethis.adoptimization.common.BLConstants;


/**
 * This is the class to run the flow.
 */

public class HourlyParsingBLData 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyParsingBLData.class);	
	
	public HourlyParsingBLData(){
	}
		
	public void parsingBLDataHourly(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String retargFilePath = config.get("RetargFilePath");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\nimpFilePath = " + retargFilePath);
			parsingDataOneHour(config, pDateStr, outFilePathHourly, retargFilePath, hour);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void parsingDataOneHour(Configuration config, String pDateStr, String outFilePath, 
			String retargFilePath, int hour) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = 30;
			if(numOfReducers != null)
				numOfReducersInt = Integer.parseInt(numOfReducers);
			String hourFolder = BLConstants.hourFolders[hour];
			sLogger.info("\nStarting to parse the hourly brand lift data at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    
		    Pipe[] dataAssembly = new BrandLiftEventDataJson(sources_hourly, retargFilePath, pDateStr, 
					hour, config).getTails();
		    
		    if(dataAssembly!=null){
		    	Scheme sinkScheme = new TextLine(Compress.ENABLE);
		    	sinkScheme.setNumSinkParts(2);
		    	for(int i=0; i<dataAssembly.length; i++){
		    		if(!(dataAssembly[i]==null)){			    
		    			String dataPipeName = dataAssembly[i].getName();
		    			String fileName = outFilePathDate + dataPipeName + "_hourly";
		    			sLogger.info("pipeName: " + dataPipeName + "    fileName: " + fileName);
		    			adoptUtils.deleteFile(fileName);
		    			Tap sink = new Hfs(sinkScheme, fileName);
		    			sinks_hourly.put(dataPipeName, sink);
		    		}
		    	}
		    	sLogger.info("Connecting the hourly bl data pipe line and assemblys ... ");
		    	long time_s0 = System.currentTimeMillis();
		    	Properties properties = new Properties();
				properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
		    	//properties.setProperty("mapred.reduce.tasks", numOfReducers);

			    AppProps.setApplicationJarClass(properties, HourlyParsingBLData.class);
			    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			    Flow flow = flowConnector.connect("bl_parsing_hourly", sources_hourly, sinks_hourly, dataAssembly);
			    flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "ParsingBLOneHour");
		    	sLogger.info("Parsing the hourly data at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is no retarg data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
