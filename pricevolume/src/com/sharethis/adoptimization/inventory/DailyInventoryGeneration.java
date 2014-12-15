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

public class DailyInventoryGeneration 
{	
	private static final Logger sLogger = Logger.getLogger(DailyInventoryGeneration.class);	
	
	public DailyInventoryGeneration(){
	}

	public void generatingInventoryDaily(Configuration config, String basePipeName, 
			String baseKeyFields, String pipeNamesList, String keyFieldsList,
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathDaily = " + outFilePathDaily +
					"\nPipeNamesList = " + pipeNamesList +
					"\nKeyFieldsList = " + keyFieldsList + 
					"\nBasePipeName = " + basePipeName +
					"\nBaseKeyFields = " + baseKeyFields);
			sLogger.info("Starting to aggregate the inventory data over hours on " + pDateStr + " ...");
			Map<String, Tap> sources = new HashMap<String, Tap>();		
			Map<String, Tap> sinks = new HashMap<String, Tap>();
			Pipe[] nbAssembly = null;
	
			nbAssembly = new DailyInventorySubAssembly(sources, outFilePathHourly, pDateStr, 
					PVConstants.hourFolders, config, pipeNamesList, keyFieldsList, basePipeName, baseKeyFields).getTails(); 

            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			int pipeLen = nbAssembly.length; 
			for(int i=0; i<pipeLen; i++){
				String pipeName = nbAssembly[i].getName();
				String fileName = outFilePathDate + pipeName + "_daily";
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
				sinks.put(pipeName, sink);
			}

			sLogger.info("Connecting the inventory data pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));

		    AppProps.setApplicationJarClass(properties, DailyInventoryGeneration.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("inventory_daily", sources, sinks, nbAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "DailyInventoryGeneration");
			sLogger.info("Generating the daily inventory data for " + pDateStr + " is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
}
