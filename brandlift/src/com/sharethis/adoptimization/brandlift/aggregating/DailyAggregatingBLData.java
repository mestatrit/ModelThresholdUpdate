package com.sharethis.adoptimization.brandlift.aggregating;

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
import cascading.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the class to run the flow.
 */

public class DailyAggregatingBLData 
{	
	private static final Logger sLogger = Logger.getLogger(DailyAggregatingBLData.class);	
	
	public DailyAggregatingBLData(){
	}
		
	public void aggregatingBLDataDaily(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			String basePipeNamesBL = config.get("BasePipeNamesDailyBL");
			String baseKeyFieldsBL = config.get("BaseKeyFieldsDailyBL");
			String pipeNamesListBL = config.get("PipeNamesListDailyBL");
			String keyFieldsListBL = config.get("KeyFieldsListDailyBL");
			int startHour = config.getInt("StartHour", 7);
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathHourly = " + outFilePathHourly +
					"\nOutFilePathDaily = " + outFilePathDaily +
					"\nBasePipeNameDailyBL = " + basePipeNamesBL +
					"\nBaseKeyFieldsDailyBL = " + baseKeyFieldsBL +
					"\nPipeNamesListDailyBL = " + pipeNamesListBL +
					"\nKeyFieldsListDailyBL = " + keyFieldsListBL);
			if(basePipeNamesBL==null || "null".equalsIgnoreCase(basePipeNamesBL) || basePipeNamesBL.isEmpty()
					||baseKeyFieldsBL==null || "null".equalsIgnoreCase(baseKeyFieldsBL) || baseKeyFieldsBL.isEmpty()){
				String errMsg = "BasePipeNameHourlyBL or BaseKeyFieldsHourlyBL is not defined or empty!";
				sLogger.info(errMsg);
//				throw new Exception(errMsg);
			}

			if(pipeNamesListBL==null || "null".equalsIgnoreCase(pipeNamesListBL) || pipeNamesListBL.isEmpty()
					||keyFieldsListBL==null || "null".equalsIgnoreCase(keyFieldsListBL) || keyFieldsListBL.isEmpty()){
				String errMsg = "PipeNamesListDailyBL or KeyFieldsListDailyBL is not defined or empty!";
				sLogger.info(errMsg);
			}
			
			aggregatingDailyBLAll(config, pDateStr, outFilePathHourly, outFilePathDaily, 
					basePipeNamesBL, baseKeyFieldsBL, pipeNamesListBL, 
					keyFieldsListBL, startHour, "_hourly", "_daily");
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyBLAll(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, int startHour, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the daily BL on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			String[] dataFieldsArr = new String[]{"sum_imps", "sum_clicks", "sum_cost", "sum_vote"};
			Fields dataFields = new Fields(dataFieldsArr[0], dataFieldsArr[1], dataFieldsArr[2], dataFieldsArr[3]);
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields baseFields = keyFields.append(dataFields);
	    	Class[] types = new Class[baseFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<baseFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[baseFields.size()-1] = String.class;
	    		    	
		    Pipe[] blAssembly_tmp = new DailyUploadingHourlyBLData(sources_agg, 
					outFilePathHourly, pDateStr, 24, 1, config, 
					baseFields, basePipeName, startHour, types, infile_postfix).getTails();

		    int baseAssemblyDailyLen = 0;
		    Pipe[] baseAssemblyDaily = null;
		    if(blAssembly_tmp!=null){
		    	baseAssemblyDaily = new DailyAggregatingBLBase(blAssembly_tmp, new String[]{basePipeName}, 
		    			new String[]{baseKeyFields}, dataFieldsArr, config).getTails();
		    	baseAssemblyDailyLen = baseAssemblyDaily.length;
		    }

		    int allAssemblyDailyLen = 0;
		    Pipe[] allAssemblyDaily = null;
		    if(baseAssemblyDaily!=null){
		    	allAssemblyDaily = new DailyAggregatingBLBase(new Pipe[]{baseAssemblyDaily[0]}, pipeNamesArr, 
		    		keyFieldsArr, dataFieldsArr, config).getTails();
		    	allAssemblyDailyLen = allAssemblyDaily.length;
		    }
		    
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";			
            adoptUtils.makeFileDir(outFilePathDate);
            
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();		

			int numOfAssembly = baseAssemblyDailyLen+allAssemblyDailyLen;
		    Pipe[] allAssembly = new Pipe[numOfAssembly];

			for(int i=0; i<numOfAssembly; i++){
				if(i<baseAssemblyDailyLen)
					allAssembly[i] = baseAssemblyDaily[i];
				else
					allAssembly[i] = allAssemblyDaily[i-baseAssemblyDailyLen];
				
				String pipeName = allAssembly[i].getName();
				String fileName = outFilePathDate + pipeName + outfile_postfix;
				sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine();
	    		sinkScheme = new TextLine(Compress.ENABLE);
				sinkScheme.setNumSinkParts(5);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeName, sink);								
		    }
		    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			//properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyAggregatingBLData.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("bl_daily_aggregating", sources_agg, sinks_agg, allAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyCTR");
			sLogger.info("Generating the daily ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
}
