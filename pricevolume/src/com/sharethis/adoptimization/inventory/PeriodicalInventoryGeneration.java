package com.sharethis.adoptimization.inventory;

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

import com.sharethis.adoptimization.common.PVConstants;
import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;


/**
 * This is the class to run the flow.
 */

public class PeriodicalInventoryGeneration 
{	
	private static final Logger sLogger = Logger.getLogger(PeriodicalInventoryGeneration.class);	
	
	public PeriodicalInventoryGeneration(){
	}
		
	public void generatingInventory(Configuration config, int numOfPeriods, int interval, int hour, 
			String basePipeName, String baseKeyFields, String pipeNamesList, String keyFieldsList, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathDaily = " + outFilePathDaily +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList +
					"\nnumOfPeriodss = " + numOfPeriods);
			generatingInventoryData(config, pDateStr, numOfPeriods, interval, outFilePathDaily, basePipeName, 
					baseKeyFields, pipeNamesList, keyFieldsList, hour, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingInventoryData(Configuration config, String pDateStr, int numOfPeriods, int interval, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, int hour, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			sLogger.info("\nStarting to generate the periodically aggregated inventory data on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
	    	
	    	Pipe[] nbAssembly = new PeriodicalInventorySubAssembly(sources_agg, outFilePathDaily, pDateStr, numOfPeriods, interval, config, 
	    			pipeNamesList, keyFieldsList, basePipeName, baseKeyFields, hour, infile_postfix).getTails();

            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			if(hour>=0){
				outFilePathDate = outFilePathDate + PVConstants.hourFolders[hour] + "/";
			}
	        pvUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			int pipeLen = nbAssembly.length; 
			for(int i=0; i<pipeLen; i++){
				String pipeName = nbAssembly[i].getName();
				String fileName = outFilePathDate + pipeName + outfile_postfix;
				sLogger.info("i = " + i + "   pipe name: " + pipeName + "   file name: " + fileName);
				pvUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine();	
//				if (pipeName.equalsIgnoreCase(basePipeName))
//					sinkScheme = new TextLine(Compress.ENABLE);
				if(!pipeName.equalsIgnoreCase(basePipeName)&&!pipeName.contains("domain"))
					sinkScheme.setNumSinkParts(3);
				else
					sinkScheme.setNumSinkParts(10);
				Tap sink = new Hfs(sinkScheme, fileName);	
				sinks_agg.put(pipeName, sink);
			}
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));

		    AppProps.setApplicationJarClass(properties, PeriodicalInventoryGeneration.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("inventory_aggregation", sources_agg, sinks_agg, nbAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingInventory");
			sLogger.info("Generating the aggregated inventory data on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
