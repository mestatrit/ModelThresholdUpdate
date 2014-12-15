package com.sharethis.adoptimization.campaigndata;

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

public class HourlyGeneratingData 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingData.class);	
	
	public HourlyGeneratingData(){
	}
		
	public void generatingDataHourly(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String impFilePath = config.get("ImpFilePath");
			String clickFilePath = config.get("ClickFilePath");
			String clickFilePathSVR = config.get("ClickFilePathSVR");
			String priceFilePath = config.get("PriceFilePath");
 		    int numOfHoursClick = config.getInt("NumOfHoursClick", 3);
			String sbFilePath = config.get("SbFilePath");
			String sbFileName = config.get("SbFileName");
 		    int numOfHoursBid = config.getInt("NumOfHoursBid", 3);
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\nimpFilePath = " + impFilePath +
					"\nclickFilePath = " + clickFilePath +
					"\nnumOfHoursClick = " + numOfHoursClick + 
					"\nsbFilePath = " + sbFilePath +
					"\nsbFilePath = " + sbFileName +
					"\nnumOfHoursBid = " + numOfHoursBid);
			generatingDataOneHour(config, pDateStr, outFilePathHourly, impFilePath, clickFilePath, 
					 clickFilePathSVR, numOfHoursClick, hour, priceFilePath, 
					 sbFilePath, sbFileName, numOfHoursBid, 1);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		

	
	private void generatingDataOneHour(Configuration config, String pDateStr, String outFilePath, 
			String impFilePath, String clickFilePath, String clickFilePathSVR, int numOfHoursClick, int hour, 
			String priceFilePath, String sbFilePath, String sbFileName, int numOfHoursBid, int pc_flag) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = 30;
			if(numOfReducers != null)
				numOfReducersInt = Integer.parseInt(numOfReducers);
			String hourFolder = CTRConstants.hourFolders[hour];
			sLogger.info("\nStarting to generate the hourly data at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    
		    Pipe[] dataAssembly = null;
		    if(pc_flag==0)
		    	dataAssembly = new HourlyDataImpClickJsonPriceBid(sources_hourly, impFilePath, clickFilePath,
					clickFilePathSVR, pDateStr, hour, numOfHoursClick, priceFilePath, 
					sbFilePath, sbFileName, numOfHoursBid, config).getTails();
		    else
		    	dataAssembly = new HourlyDataPriceImpClickBid(sources_hourly, impFilePath, clickFilePath,
					clickFilePathSVR, pDateStr, hour, numOfHoursClick, priceFilePath, 
					sbFilePath, sbFileName, numOfHoursBid, config).getTails();
		    	
		    
		    if(dataAssembly!=null){
		    	Scheme sinkScheme = new TextLine(Compress.ENABLE);
		    	sinkScheme.setNumSinkParts(numOfReducersInt);
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
		    	sLogger.info("Connecting the hourly data pipe line and assemblys ... ");
		    	long time_s0 = System.currentTimeMillis();
		    	Properties properties = new Properties();
				properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
		    	properties.setProperty("mapred.reduce.tasks", numOfReducers);

			    AppProps.setApplicationJarClass(properties, HourlyGeneratingData.class);
			    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			    Flow flow = flowConnector.connect("data_hourly", sources_hourly, sinks_hourly, dataAssembly);
			    flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROneHour");
		    	sLogger.info("Generating the hourly data at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is either no impression data or no click data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
