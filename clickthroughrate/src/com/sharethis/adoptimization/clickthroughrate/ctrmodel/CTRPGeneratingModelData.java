package com.sharethis.adoptimization.clickthroughrate.ctrmodel;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPGeneratingModelData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPGeneratingModelData.class);
	private static final long serialVersionUID = 1L;
	
	public void generatingModelData(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String ctrFilePath = config.get("CTRFilePath");
			String modFilePath = config.get("ModelFilePath");
			String dataYNamesList = config.get("DataYNamesList");
			String dataYFieldsList = config.get("DataYFieldsList");
			String dataXNamesList = config.get("DataXNamesList");
			String dataXFieldsList = config.get("DataXFieldsList");
			String dataFileNamesList = config.get("DataFileNamesList");
			String dataFilterFieldList = config.get("DataFilterFieldList");
			String dataFilterValueList = config.get("DataFilterValueList");
			String modelNamesList = config.get("CTRPModelNamesList");
			String yModelFieldsList = config.get("CTRPYModelFieldsList");
			String xModelFieldsList = config.get("CTRPXModelFieldsList");
			String xModelFieldsListKept = config.get("CTRPXModelFieldsListKept");
			String infile_postfix = config.get("AggFilePostfix");
			String outfile_postfix = config.get("CTRPDataPostfix");
			String all_id = config.get("CTRPOverallID");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nCTRFilePath = " + ctrFilePath +
					"\nModelFilePath = " + modFilePath +
					"\nDataYNamesList = " + dataYNamesList +
					"\nDataYFieldsList = " + dataYFieldsList +
					"\nDataXNamesList = " + dataXNamesList +
					"\nDataXFieldsList = " + dataXFieldsList);			
			
			if(dataYNamesList==null||"null".equalsIgnoreCase(dataYNamesList)
					||dataYNamesList.isEmpty()||dataXNamesList==null
					||"null".equalsIgnoreCase(dataXNamesList)
					||dataXNamesList.isEmpty()){
				sLogger.info("No predictive model is defined!");
			}else{			
				String[] dataYNames = StringUtils.split(dataYNamesList, "|");
				String[] dataYFields = StringUtils.split(dataYFieldsList, "|");
				String[] dataXNames = StringUtils.split(dataXNamesList, "|");
				String[] dataXFields = StringUtils.split(dataXFieldsList, "|");
				String[] dataFileNames = StringUtils.split(dataFileNamesList, "|");
				String[] dataFilterFields = StringUtils.split(dataFilterFieldList, "|");
				String[] dataFilterValues = StringUtils.split(dataFilterValueList, "|");
				String[] modelNames = StringUtils.split(modelNamesList, ":");
				String[] modelYFields = StringUtils.split(yModelFieldsList, ":");
				String[] modelXFields = StringUtils.split(xModelFieldsList, ":");
				String[] modelXFieldsKept = StringUtils.split(xModelFieldsListKept, ":");

				if((dataYNames.length==dataYFields.length)
						&&(dataXNames.length==dataXFields.length) 
						&&(dataYNames.length==dataXNames.length)
						&&(dataYNames.length>0)){		
					sLogger.info("Length of dataYNames: " + dataYNames.length + "\n"
							+ "Length of dataXNames: " + dataXNames.length + "\n"
							+ "Length of dataFilterFields: " + dataFilterFields.length + "\n"
							+ "Length of dataFilterValues: " + dataFilterValues.length + "\n"
							+ "Length of modelNames: " + modelNames.length + "\n"
							+ "Length of modelYFields: " + modelYFields.length + "\n"
							+ "Length of modelXFields: " + modelXFields.length + "\n"
							+ "Length of modelXFieldsKept: " + modelXFieldsKept.length + "\n");
					for(int i=0; i<dataYNames.length; i++){
						processingCTRData(config, pDateStr, ctrFilePath, modFilePath, dataYNames[i], 
								dataYFields[i], dataXNames[i], dataXFields[i], dataFileNames[i], 
								dataFilterFields[i], dataFilterValues[i],
								modelNames[i], modelYFields[i], modelXFields[i], modelXFieldsKept[i],
								infile_postfix, all_id, outfile_postfix, i);
					}
				}else{
						String msg = "The number of data names and the number of data key field lists are not the same!";	
						throw new Exception(msg);
				}	
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	

	private void processingCTRData(Configuration config, String pDateStr, String ctrFilePath, 
			String modFilePath, String dataYPipeName, String dataYKeyFields, String pipeNamesList, 
			String keyFieldsList, String dataFileName, String dataFilterField, 
			String dataFilterValue, String modelNames, String modelYFields, 
			String modelXFields, String modelXFieldsKept, String infile_postfix, String all_id,
			String outfile_postfix, int i_data) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("CTRPNumOfReducersData");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the ctr data for models on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			
			Pipe[] dataAssembly = new CTRPProcessingModelData(sources_agg, config, pDateStr, 
					ctrFilePath, modFilePath, dataYPipeName, dataYKeyFields, pipeNamesList, 
					keyFieldsList, dataFileName, dataFilterField, dataFilterValue, 
					all_id, infile_postfix).getTails();			
			int numOfDataPipes = dataAssembly.length;

			Pipe[] modAssembly = null;
			int numOfModPipes = 0;

			modAssembly = new CTRPGeneratingModel(config, dataAssembly[numOfDataPipes-1], 
				keyFieldsList, modelNames, modelYFields, modelXFields, modelXFieldsKept).getTails();
			numOfModPipes = modAssembly.length;
			int numOfPipes = numOfDataPipes + numOfModPipes;
			Pipe[] resAssembly = new Pipe[numOfPipes];
			for(int i=0; i<numOfDataPipes; i++)
				resAssembly[i] = new Pipe(dataAssembly[i].getName(), dataAssembly[i]);
			for(int i=0; i<numOfModPipes; i++)
				resAssembly[numOfDataPipes + i] = new Pipe(modAssembly[i].getName(), modAssembly[i]);
			
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String modFilePathDate = modFilePath + dayStr + "/";					
		
            adoptUtils.makeFileDir(modFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
//			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = null;

			for(int i=0; i<numOfPipes; i++){
				String pipeName = resAssembly[i].getName();
				//Constructing the required output format
				if(pipeName.startsWith("rtb_"))
					fileName = modFilePathDate + "rtb/" + pipeName;
				else
					fileName = modFilePathDate+pipeName;
//					fileName = modFilePathDate+pipeName+"_"+i_data;
				adoptUtils.deleteFile(fileName);
				if(!pipeName.contains("model")){
					sinkScheme.setNumSinkParts(numOfReducersInt);
				}else{
					sinkScheme.setNumSinkParts(2);
				}
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeName, sink);							
		    }			
		    
			sLogger.info("Connecting the loading data pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, CTRPGeneratingModelData.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctrp_data", sources_agg, sinks_agg, resAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "CTRPGeneratingData");
			sLogger.info("Generating the ctr data for models on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
