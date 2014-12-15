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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the assembly to read price confirmation data.
 */

public class CTRPGeneratingBaseData extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(CTRPGeneratingBaseData.class);
	private static final long serialVersionUID = 1L;
	
	public void generatingBaseData(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String ctrFilePath = config.get("CTRFilePath");
			String modFilePath = config.get("ModelFilePath");
			String infile_postfix = config.get("AggFilePostfix");
			String outfile_postfix = config.get("CTRPDataPostfix");
			String all_id = config.get("CTRPOverallID");
			String ctrBaseNamesList = config.get("CTRBaseNamesList");
			String ctrBaseFieldsList = config.get("CTRBaseFieldsList");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nCTRFilePath = " + ctrFilePath +
					"\nModelFilePath = " + modFilePath +
					"\nCTRBaseNamesList = " + ctrBaseNamesList +
					"\nCTRBaseFieldsList = " + ctrBaseFieldsList);

			if(ctrBaseNamesList!=null&&ctrBaseFieldsList!=null)
				processingCTRBaseData(config, pDateStr, ctrFilePath, modFilePath, ctrBaseNamesList, 
						ctrBaseFieldsList, infile_postfix, all_id, outfile_postfix);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
	
	private void processingCTRBaseData(Configuration config, String pDateStr, String ctrFilePath, 
			String modFilePath, String ctrPipeNamesList, String ctrKeyFieldsList, String infile_postfix, 
			String all_id, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = "10";
			sLogger.info("\nStarting to generate the ctr base data on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			
			Pipe[] dataAssembly = new CTRPProcessingCTRBase(sources_agg, config, pDateStr, 
					ctrFilePath, modFilePath, ctrPipeNamesList, ctrKeyFieldsList, 
					all_id, infile_postfix).getTails();			
			int numOfPipes = dataAssembly.length;

			Pipe[] resAssembly = new Pipe[numOfPipes];
			for(int i=0; i<numOfPipes; i++)
				resAssembly[i] = new Pipe(dataAssembly[i].getName(), dataAssembly[i]);
			
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
			String modFilePathDate = modFilePath + dayStr + "/";					
		
            adoptUtils.makeFileDir(modFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
			sinkScheme.setNumSinkParts(10);
			Tap sink = null;

			for(int i=0; i<numOfPipes; i++){
				String pipeName = resAssembly[i].getName();
				//Constructing the required output format
				if(pipeName.startsWith("rtb_"))
					fileName = modFilePathDate + "rtb/" + pipeName;
				else
					fileName = modFilePathDate+pipeName;
				adoptUtils.deleteFile(fileName);
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeName, sink);							
		    }			
		    
			sLogger.info("Connecting the ctr base data pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, CTRPGeneratingBaseData.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_base_data", sources_agg, sinks_agg, resAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "CTRPGeneratingBaseData");
			sLogger.info("Generating the ctr base data on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
