package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class PCGeneratingModel { 

	private static final Logger sLogger = Logger.getLogger(PCGeneratingModel.class);
	
	public void generatingPCModels(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String ctrFilePath = config.get("CTRFilePath");
			String modFilePath = config.get("ModelFilePath");
			String pcModelNamesList = config.get("PCModelNamesList");
			String pcModelFieldsList = config.get("PCModelFieldsList");
			String infile_postfix = config.get("AggFilePostfix");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nCTRFilePath = " + ctrFilePath +
					"\nPCModelNamesList = " + pcModelNamesList +
					"\nPCModelFieldsList = " + pcModelFieldsList);
			if(pcModelNamesList==null||"null".equalsIgnoreCase(pcModelNamesList)
					||pcModelNamesList.isEmpty()){
				sLogger.info("No price-click model is defined!");
			}else{			
				String[] modelName = StringUtils.split(pcModelNamesList, "|");
				String[] modelFields = StringUtils.split(pcModelFieldsList, "|");
				if((modelName.length==modelFields.length)&&(modelName.length>0)){
					for(int i=0; i<modelName.length; i++){
						generatingOnePCModel(config, pDateStr, ctrFilePath, modFilePath, 
								modelName[i], modelFields[i], infile_postfix);
					}
				}else{
					sLogger.info("The number of PC model names and the number of PC model key field lists are not the same!");					
				}
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
		
	private void generatingOnePCModel(Configuration config, String pDateStr, String ctrFilePath, String modFilePath, 
			String modelName, String modelFields, String infile_postfix) throws Exception{
		try{			
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("Starting generating the price-click models ...");
			Map<String, Tap> sources = new HashMap<String, Tap>();		
			Map<String, Tap> sinks = new HashMap<String, Tap>();
			Pipe[] bmAssembly = new PCProcessingModel(sources, config, pDateStr, 
					ctrFilePath, modFilePath, modelName, modelFields, infile_postfix).getTails();
            String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = modFilePath+dayStr+"/";
			int pipeLen = bmAssembly.length; 
			String[] pipeNames = new String[pipeLen];  
			String[] fileNames = new String[pipeLen];
			for(int i=0; i<pipeLen; i++){
				pipeNames[i] = bmAssembly[i].getName();
				fileNames[i] = outFilePathDate + pipeNames[i];					
				adoptUtils.deleteFile(fileNames[i]);
				Scheme sinkScheme = new TextLine();
				Tap sink = new Hfs(sinkScheme, fileNames[i]);	
				sinks.put(pipeNames[i], sink);
			}
			sLogger.info("Connecting the pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("PCNumOfReducers"));

		    AppProps.setApplicationJarClass(properties, PCGeneratingModel.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			Flow flow = flowConnector.connect("pc_model", sources, sinks, bmAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingPCModels");
			sLogger.info("Generating the price-click models is done.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
