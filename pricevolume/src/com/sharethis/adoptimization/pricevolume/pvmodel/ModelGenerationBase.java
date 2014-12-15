package com.sharethis.adoptimization.pricevolume.pvmodel;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.pricevolume.PriceVolumeUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class ModelGenerationBase { 

	private static final Logger sLogger = Logger.getLogger(ModelGenerationBase.class);
	
	public void generatingBaseModels(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePath = config.get("OutFilePath");
			String modelNameList = config.get("ModelNameList");
			String modelFieldsList = config.get("ModelFieldsList");
			int numOfPoints = config.getInt("NumOfPoints", 15);
			double rSquareVal = config.getFloat("RSquareVal", 0.9f);
			String numOfDaysList = config.get("NumOfDaysList");
			double maxPredWR = config.getFloat("MaxPredWR", 0.75f);
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePath = " + outFilePath +
					"\nModelNameList = " + modelNameList +
					"\nModelFieldsList = " + modelFieldsList +
					"\nNumOfPoints = " + numOfPoints +
					"\nRSquareVal = " + rSquareVal +
					"\nNumOfDaysList = " + numOfDaysList);
			String[] numOfDaysArr = StringUtils.split(numOfDaysList, ",");
			for(int i=0; i<numOfDaysArr.length; i++){
				int numOfDays = Integer.parseInt(numOfDaysArr[i]);
				generatingBaseModels(config, pDateStr, outFilePath, modelNameList, 
						modelFieldsList, numOfPoints, rSquareVal, numOfDays, maxPredWR);
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
		
	private void generatingBaseModels(Configuration config, String pDateStr, String outFilePath, String modelNameList, 
			String modelFieldsList, int numOfPoints, double rSquareVal, int numOfDays, double maxPredWR) throws Exception{
		try{			
			PriceVolumeUtils pvUtils = new PriceVolumeUtils(config);
			sLogger.info("Starting generating the price-volume base models for numOfDays = " + numOfDays + " ...");
			Map<String, Tap> sources = new HashMap<String, Tap>();		
			Map<String, Tap> sinks = new HashMap<String, Tap>();
			Pipe[] bmAssembly = new ModelBaseModelSubAssembly(sources, outFilePath, pDateStr, config, 
					modelNameList, modelFieldsList, numOfPoints, rSquareVal, numOfDays, maxPredWR).getTails();

            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+"/";
			int pipeLen = bmAssembly.length; 
			String[] pipeNames = new String[pipeLen];  
			String[] fileNames = new String[pipeLen];
			for(int i=0; i<pipeLen; i++){
				pipeNames[i] = bmAssembly[i].getName();
				fileNames[i] = outFilePathDate + pipeNames[i] + "_daily";
				if(numOfDays==7)
					fileNames[i] = outFilePathDate + pipeNames[i] + "_agg";					
				pvUtils.deleteFile(fileNames[i]);
				Scheme sinkScheme = new TextLine();
				Tap sink = new Hfs(sinkScheme, fileNames[i]);	
				sinks.put(pipeNames[i], sink);
			}
			sLogger.info("Connecting the pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));

		    AppProps.setApplicationJarClass(properties, ModelGenerationBase.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			Flow flow = flowConnector.connect("pv_data_"+numOfDays, sources, sinks, bmAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingBaseModels");
			sLogger.info("Generating the price-volume base models is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
