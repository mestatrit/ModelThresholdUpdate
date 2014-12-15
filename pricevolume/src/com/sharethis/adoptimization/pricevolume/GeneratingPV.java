package com.sharethis.adoptimization.pricevolume;

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

/**
 * This is the class to run the flow.
 */

public class GeneratingPV 
{	
	private static final Logger sLogger = Logger.getLogger(GeneratingPV.class);	
	
	public GeneratingPV(){
	}
	
	
	public void generatingPVAgg(Configuration config) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathDaily = config.get("OutFilePathDaily");
			String bidFilePath = config.get("BidFilePath");
			String priceFilePath = config.get("PriceFilePath");
			String pipeNameList = config.get("PipeNameList");
			String keyFieldsList = config.get("KeyFieldsList");
			String pipeNameBase = config.get("PipeNameBase");
			String keyFieldsBase = config.get("KeyFieldsBase");
			String bidThValHour = config.get("BidThValHour");
			String bidThValList = config.get("BidThValList");
			int numOfDays = Integer.parseInt(config.get("NumOfDays", "1"));
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathDaily = " + outFilePathDaily +
					"\nBidFilePath = " + bidFilePath +
					"\nPriceFilePath = " + priceFilePath +
					"\nPipeNameList = " + pipeNameList +
					"\nKeyFieldsList = " + keyFieldsList + 
					"\nPipeNameBase = " + pipeNameBase +
					"\nKeyFieldsBase = " + keyFieldsBase + 
					"\nBidThValHour = " + bidThValHour + 
					"\nBidThValList = " + bidThValList + 
					"\nNumOfDays = " + numOfDays);
			
			sLogger.info("Starting generating the price-volume ...");
			Map<String, Tap> sources = new HashMap<String, Tap>();		
			Map<String, Tap> sinks = new HashMap<String, Tap>();
			Pipe[] pvAssembly = null;
	
			pvAssembly = new AppendingDailyData(sources, outFilePathDaily, pDateStr, numOfDays, config, 
					pipeNameList, keyFieldsList, pipeNameBase, keyFieldsBase, bidThValHour, bidThValList).getTails();

            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			int pipeLen = pvAssembly.length; 
			String[] pipeNames = new String[pipeLen];  
			String[] fileNames = new String[pipeLen];
			for(int i=0; i<pipeLen; i++){
				pipeNames[i] = pvAssembly[i].getName();
				fileNames[i] = outFilePathDate + pipeNames[i] + "_agg";
				pvUtils.deleteFile(fileNames[i]);
				Scheme sinkScheme = new TextLine();
//				if (!fileNames[i].contains("_p_"))
//					sinkScheme = new TextLine(Compress.ENABLE);	
				Tap sink = new Hfs(sinkScheme, fileNames[i]);	
				sinks.put(pipeNames[i], sink);
			}

			sLogger.info("Connecting the pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));

		    AppProps.setApplicationJarClass(properties, GeneratingPV.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("price_volume_all", sources, sinks, pvAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingPV");
			sLogger.info("Generating the price-volume is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
	
	
	public void generatingPVHourly(Configuration config) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String bidFilePath = config.get("BidFilePath");
			String priceFilePath = config.get("PriceFilePath");
			String pipeNameHour = config.get("PipeNameHour");
			String keyFieldsHour = config.get("KeyFieldsHour");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathHourly = " + outFilePathHourly +
					"\nBidFilePath = " + bidFilePath +
					"\nPriceFilePath = " + priceFilePath +
					"\nPipeNameHour = " + pipeNameHour +
					"\nKeyFieldsHour = " + keyFieldsHour);
			int hour = config.getInt("HourOfDay", 0);
			String hourFolder = PVConstants.hourFolders[hour];
			sLogger.info("Starting to generate the hourly price-volume at hour " + hourFolder + " on " + pDateStr + " ...");
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Pipe[] allAssembly = (new HourlyDataAggregationAssembly(sources_hourly, bidFilePath, priceFilePath, hourFolder,
					pDateStr, hour, config, pipeNameHour, keyFieldsHour)).getTails();
            int pipeLen = allAssembly.length;		
            String[] pipeNames = new String[pipeLen]; 
            String[] fileNames = new String[pipeLen];
            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
//            String outFilePathDate = outFilePath+dayStr+"/"+hourFolder+"/";
            // The change is to handle the new data path: yyyyMMddhh
            String outFilePathDate = outFilePathHourly+dayStr+hourFolder+"/";

            pvUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
			for(int i=0; i<pipeLen; i++){
				pipeNames[i] = allAssembly[i].getName();	
				fileNames[i] = outFilePathDate + pipeNames[i] + "_hourly";
				sLogger.info("i = " + i + "  pipeNames: " + pipeNames[i]+ "    fileNames:" + fileNames[i]);
				pvUtils.deleteFile(fileNames[i]);
//				Scheme sinkScheme = new TextLine();	
				Scheme sinkScheme = new TextLine(Compress.ENABLE);	
				if(pipeNames[i].contains("rtb"))
					sinkScheme.setNumSinkParts(300);
				else
					sinkScheme.setNumSinkParts(20);
				Tap sink = new Hfs(sinkScheme, fileNames[i]);
				sinks_hourly.put(pipeNames[i], sink);
			}

			sLogger.info("Connecting the hourly pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));
			
		    AppProps.setApplicationJarClass(properties, GeneratingPV.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			Flow flow = flowConnector.connect("price_volume_hourly", sources_hourly, sinks_hourly, allAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingPVHourly");
			sLogger.info("Generating the hourly price-volume is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		

	public void generatingPVDailyAgg(Configuration config) throws Exception{
		try{
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			String bidFilePath = config.get("BidFilePath");
			String priceFilePath = config.get("PriceFilePath");
			String pipeNameList = config.get("PipeNameList");
			String keyFieldsList = config.get("KeyFieldsList");
//			String pipeNameHour = config.get("PipeNameHour");
//			String keyFieldsHour = config.get("KeyFieldsHour");
			String pipeNameBase = config.get("PipeNameBase");
			String keyFieldsBase = config.get("KeyFieldsBase");
			String bidThValHour = config.get("BidThValHour");
			String bidThValList = config.get("BidThValList");
			int numOfDays = Integer.parseInt(config.get("NumOfDays", "1"));
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathDaily = " + outFilePathDaily +
					"\nBidFilePath = " + bidFilePath +
					"\nPriceFilePath = " + priceFilePath +
					"\nPipeNameList = " + pipeNameList +
					"\nKeyFieldsList = " + keyFieldsList + 
					"\nPipeNameBase = " + pipeNameBase +
					"\nKeyFieldsBase = " + keyFieldsBase + 
					"\nBidThValHour = " + bidThValHour + 
					"\nBidThValList = " + bidThValList + 
					"\nNumOfDays = " + numOfDays);
			sLogger.info("Starting to aggregate the price-volume over hours on " + pDateStr + " ...");
			Map<String, Tap> sources = new HashMap<String, Tap>();		
			Map<String, Tap> sinks = new HashMap<String, Tap>();
			Pipe[] pvAssembly = null;
	
			pvAssembly = new HourlyAppendingData(sources, outFilePathHourly, pDateStr, PVConstants.hourFolders, 
					config, pipeNameList, keyFieldsList, pipeNameBase, keyFieldsBase, bidThValHour, bidThValList).getTails();

            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            // The change is to handle the new daily data path: yyyyMMdd
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			int pipeLen = pvAssembly.length; 
			String[] pipeNames = new String[pipeLen];  
			String[] fileNames = new String[pipeLen];
			for(int i=0; i<pipeLen; i++){
				pipeNames[i] = pvAssembly[i].getName();
				fileNames[i] = outFilePathDate + pipeNames[i] + "_daily";
				sLogger.info("i = " + i + "   pipe name: " + pipeNames[i] + "   file name: " + fileNames[i]);
				pvUtils.deleteFile(fileNames[i]);
				Scheme sinkScheme = new TextLine();	
//				if (!fileNames[i].contains("_p_"))
//					sinkScheme = new TextLine(Compress.ENABLE);	
				Tap sink = new Hfs(sinkScheme, fileNames[i]);	
				sinks.put(pipeNames[i], sink);
			}

			sLogger.info("Connecting the pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));

		    AppProps.setApplicationJarClass(properties, GeneratingPV.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			Flow flow = flowConnector.connect("price_volume_daily", sources, sinks, pvAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingPV");
			sLogger.info("Generating the daily price-volume for " + pDateStr + " is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
}
