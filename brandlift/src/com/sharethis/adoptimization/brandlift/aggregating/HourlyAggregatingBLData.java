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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.BLConstants;


/**
 * This is the class to run the flow.
 */

public class HourlyAggregatingBLData 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingBLData.class);	
	
	public HourlyAggregatingBLData(){
	}
		
	public void aggregatingBLDataHourly(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String blFilePath = config.get("BLFilePath");
			String blFileName = config.get("BLFileName");
			String allFilePath = config.get("AllFilePath");
			String allFileName = config.get("AllFileName");
			String basePipeNamesBL = config.get("BasePipeNameHourlyBL");
			String baseKeyFieldsBL = config.get("BaseKeyFieldsHourlyBL");
			String pipeNamesListBL = config.get("PipeNamesListHourlyBL");
			String keyFieldsListBL = config.get("KeyFieldsListHourlyBL");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\nOutFilePathHourly = " + outFilePathHourly +
					"\nBLFilePath = " + blFilePath +
					"\nBLFileName = " + blFileName +
					"\nAllFilePath = " + allFilePath +
					"\nAllFileName = " + allFileName +
					"\nBasePipeNameHourlyBL = " + basePipeNamesBL +
					"\nBaseKeyFieldsHourlyBL = " + baseKeyFieldsBL +
					"\npipeNamesListHourlyBL = " + pipeNamesListBL +
					"\nkeyFieldsListHourlyBL = " + keyFieldsListBL);
			String[] basePipeNamesArr = null;
			String[] baseKeyFieldsArr = null;
			if(basePipeNamesBL!=null && !"null".equalsIgnoreCase(basePipeNamesBL) && !basePipeNamesBL.isEmpty()
					&&baseKeyFieldsBL!=null && !"null".equalsIgnoreCase(baseKeyFieldsBL) && !baseKeyFieldsBL.isEmpty()){
				basePipeNamesArr = StringUtils.split(basePipeNamesBL, ";");
				baseKeyFieldsArr = StringUtils.split(baseKeyFieldsBL, ";");
			}else{
				String errMsg = "BasePipeNameHourlyBL or BaseKeyFieldsHourlyBL is not defined or empty!";
				sLogger.info(errMsg);
//				throw new Exception(errMsg);
			}

			String[] pipeNamesArr = null;
			String[] keyFieldsArr = null;
			if(pipeNamesListBL!=null && !"null".equalsIgnoreCase(pipeNamesListBL) && !pipeNamesListBL.isEmpty()
					&&keyFieldsListBL!=null && !"null".equalsIgnoreCase(keyFieldsListBL) && !keyFieldsListBL.isEmpty()){
				pipeNamesArr = StringUtils.split(pipeNamesListBL, ";");
				keyFieldsArr = StringUtils.split(keyFieldsListBL, ";");
			}else{
				String errMsg = "PipeNamesListHourlyBL or KeyFieldsListHourlyBL is not defined or empty!";
				sLogger.info(errMsg);
			}
			
			aggregatingDataOneHour(config, pDateStr, outFilePathHourly, allFilePath, 
					allFileName, blFilePath, blFileName, hour, basePipeNamesArr, 
					baseKeyFieldsArr, pipeNamesArr, keyFieldsArr);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDataOneHour(Configuration config, String pDateStr, String outFilePath, 
			String allFilePath, String allFileName, String blFilePath, String blFileName, 
			int hour, String[] basePipeNamesArr, String[] baseKeyFieldsArr,
			String[] pipeNamesArr, String[] keyFieldsArr) throws Exception{

		String hourFolder = BLConstants.hourFolders[hour];
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = 40;
			if(numOfReducers != null)
				numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the hourly brand lift data at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		            			
		    Pipe[] allBLAssembly = new HourlyDataAllBrandLift(sources_hourly, allFilePath, allFileName, blFilePath, 
		    		blFileName, pDateStr, hour, config).getTails();

		    Pipe[] baseBLAssembly = new HourlyAggregatingBLBase(allBLAssembly[0], basePipeNamesArr, baseKeyFieldsArr, 
					config).getTails();

		    int baseBLAssemblyLen = 0;
		    if(baseBLAssembly!=null)
		    	baseBLAssemblyLen = baseBLAssembly.length;

		    Pipe[] sumBLAssembly = new Pipe[0];
		    if(baseBLAssemblyLen>0)
		    	sumBLAssembly = new HourlyAggregatingBLAll(baseBLAssembly[0], pipeNamesArr, keyFieldsArr, 
					config).getTails();
		    
		    int sumBLAssemblyLen = 0;
		    if(sumBLAssembly!=null)
		    	sumBLAssemblyLen = sumBLAssembly.length;
		    
		    Pipe[] dataAssembly = null;
		    Scheme[] sinkSchemes = null;
		    if(allBLAssembly!=null){
		    	dataAssembly = new Pipe[allBLAssembly.length+baseBLAssemblyLen+sumBLAssemblyLen];
		    	sinkSchemes = new Scheme[allBLAssembly.length+baseBLAssemblyLen+sumBLAssemblyLen];
		    	//Scheme sinkScheme = new TextLine(Compress.ENABLE);
		    	//sinkScheme.setNumSinkParts(numOfReducersInt);
		    	for(int i=0; i<dataAssembly.length; i++){
		    		sinkSchemes[i] = new TextLine(Compress.ENABLE);
		    		sinkSchemes[i].setNumSinkParts(2);
		    		if(i<allBLAssembly.length){
		    			dataAssembly[i] = allBLAssembly[i];
		    		}else{
		    			if(i<allBLAssembly.length+baseBLAssembly.length)
		    				dataAssembly[i] = baseBLAssembly[i-allBLAssembly.length];
		    			else
		    				dataAssembly[i] = sumBLAssembly[i-allBLAssembly.length-baseBLAssemblyLen];
		    		}
		    		if(!(dataAssembly[i]==null)){			    
		    			String dataPipeName = dataAssembly[i].getName();
		    			String fileName = outFilePathDate + dataPipeName + "_hourly";
		    			sLogger.info("pipeName: " + dataPipeName + "    fileName: " + fileName);
		    			adoptUtils.deleteFile(fileName);
		    			Tap sink = new Hfs(sinkSchemes[i], fileName);
		    			sinks_hourly.put(dataPipeName, sink);
		    		}
		    	}

		    	sLogger.info("Connecting the hourly aggregating bl data pipe line and assemblys ... ");
		    	long time_s0 = System.currentTimeMillis();
		    	Properties properties = new Properties();
				properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
		    	//properties.setProperty("mapred.reduce.tasks", numOfReducers);

			    AppProps.setApplicationJarClass(properties, HourlyAggregatingBLData.class);
			    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			    Flow flow = flowConnector.connect("brand_lift_hourly", sources_hourly, sinks_hourly, dataAssembly);
			    flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingBLOneHour");
		    	sLogger.info("Generating the hourly data at hour " + hourFolder + " is done.\n");
		    }
		}catch(Exception ee){
			sLogger.info("There is no generated brand lift data for the hour " + hourFolder + " on " + pDateStr + "\n");	
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}	
}
