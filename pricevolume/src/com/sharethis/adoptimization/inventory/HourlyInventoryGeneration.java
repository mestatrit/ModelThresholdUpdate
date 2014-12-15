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

public class HourlyInventoryGeneration 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyInventoryGeneration.class);	
	
	public HourlyInventoryGeneration(){
	}
		
	public void generatingInventoryHourly(Configuration config) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String dataFilePath = config.get("DataFilePath");
			String basePipeNameHour = config.get("BasePipeNameHourly");
			String baseKeyFieldsHour = config.get("BaseKeyFieldsHourly");
			String pipeNamesHour = config.get("PipeNamesHourly");
			String keyFieldsHour = config.get("KeyFieldsHourly");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathHourly = " + outFilePathHourly +
					"\nDataFilePath = " + dataFilePath +
					"\nBasePipeNameHour = " + basePipeNameHour +
					"\nBaseKeyFieldsHour = " + baseKeyFieldsHour +
					"\nPipeNameHour = " + pipeNamesHour +
					"\nKeyFieldsHour = " + keyFieldsHour);
			int hour = config.getInt("HourOfDay", 0);
			String hourFolder = PVConstants.hourFolders[hour];
			sLogger.info("Starting to generate the hourly inventory data at hour " + hourFolder + " on " + pDateStr + " ...");
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Pipe[] allAssembly = new HourlyInventorySubAssembly(sources_hourly, dataFilePath, hourFolder, 
					pDateStr, hour, config, basePipeNameHour, baseKeyFieldsHour, pipeNamesHour, keyFieldsHour).getTails();
            int pipeLen = allAssembly.length;		
            String pipeName = null; 
            String fileName = null;
            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            //String outFilePathDate = outFilePath+dayStr+"/"+hourFolder+"/";
            String outFilePathDate = outFilePathHourly+dayStr+hourFolder+"/";
            pvUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
			for(int i=0; i<pipeLen; i++){
				pipeName = allAssembly[i].getName();	
				fileName = outFilePathDate + pipeName + "_hourly";
				sLogger.info("i = " + i + "  pipeName: " + pipeName + "    fileName: " + fileName);
				pvUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine();	
//				Scheme sinkScheme = new TextLine(Compress.ENABLE);	
				if(!pipeName.equalsIgnoreCase(basePipeNameHour))
					sinkScheme.setNumSinkParts(3);
				else
					sinkScheme.setNumSinkParts(10);
					
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_hourly.put(pipeName, sink);
			}

			sLogger.info("Connecting the hourly inventory data pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));
			
		    AppProps.setApplicationJarClass(properties, HourlyInventoryGeneration.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("inventory_hourly", sources_hourly, sinks_hourly, allAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingInventoryHourly");
			sLogger.info("Generating the hourly inventory data is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
}
