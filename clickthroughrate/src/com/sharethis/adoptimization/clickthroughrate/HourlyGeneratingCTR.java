package com.sharethis.adoptimization.clickthroughrate;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.CTRConstants;


/**
 * This is the class to run the flow.
 */

public class HourlyGeneratingCTR 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyGeneratingCTR.class);	
	
	public HourlyGeneratingCTR(){
	}
	
	public void generatingCTRHourly(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String icpFilePath = config.get("IcpFilePath");
			String icpFileName = config.get("IcpFileName");
			String basePipeName = config.get("BasePipeNameHourly");
			String baseKeyFields = config.get("BaseKeyFieldsHourly");
			String pipeNamesList = config.get("PipeNamesListHourly");
			String keyFieldsList = config.get("KeyFieldsListHourly");
			String pbFilePath = config.get("PbFilePath");
			String pbFileName = config.get("PbFileName");
			String mapKeys = config.get("MapKeys");
			String mapPipeNames = config.get("MapPipeNames");
 		    int numOfHoursClick = config.getInt("NumOfHoursClick", 3);
			int numOfHoursImp = config.getInt("NumOfHoursImp", 2);
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\nimpFilePath = " + icpFilePath +
					"\nicpFileName = " + icpFileName +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields +
					"\nnumOfHoursClick = " + numOfHoursClick + 
					"\npbFilePath = " + pbFilePath +
					"\npbFileName = " + pbFileName +
					"\nmapPipeNames = " + mapPipeNames +
					"\nmapKeys = " + mapKeys +
					"\nnumOfHoursImp = " + numOfHoursImp);
			generatingCTROneHour(config, pDateStr, outFilePathHourly, icpFilePath, icpFileName, basePipeName, 
					baseKeyFields, pipeNamesList, keyFieldsList, numOfHoursClick, hour, //);
					pbFilePath, pbFileName, numOfHoursImp, mapPipeNames, mapKeys);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		

	private void generatingCTROneHour(Configuration config, String pDateStr, String outFilePath, 
			String icpFilePath, String icpFileName, String basePipeName, String baseKeyFields, 
			String pipeNamesList, String keyFieldsList, int numOfHoursClick, int hour, 
			String pbFilePath, String pbFileName, int numOfHoursImp, String mapPipeNames, 
			String mapKeys) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			String hourFolder = CTRConstants.hourFolders[hour];
			sLogger.info("\nStarting to generate the hourly ctr at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = null;
		    int numOfPipes = 0;
	    	int num_pipes = 0;
		    if(pipeNamesList!=null){
		    	pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    	numOfPipes = pipeNamesArr.length;
		    }
		    num_pipes = numOfPipes;
		    Pipe[] allAssembly = null;
		    
		    allAssembly = new HourlyDataImpClickPriceBid(sources_hourly, icpFilePath, icpFileName,
					pDateStr, hour, numOfHoursClick, pbFilePath, pbFileName, numOfHoursImp, 
					mapPipeNames, mapKeys, config).getTails();	
		    	
		    if(!(allAssembly[0]==null)){			    
		    	int numOfAssemblys = allAssembly.length;
		    	Pipe[] ctrAssembly = new Pipe[1+num_pipes+numOfAssemblys];
		        for(int i=0; i<numOfAssemblys; i++){
		        	String allPipeName = allAssembly[i].getName();
		        	String dataPipeName = "rtb_" + allPipeName;
		        	ctrAssembly[1+num_pipes+i] = new Pipe(dataPipeName, allAssembly[i]);
		        	String fileName = outFilePathDate + allPipeName + "_hourly";
		        	sLogger.info("pipeName: " + dataPipeName + "    fileName: " + fileName);
		        	adoptUtils.deleteFile(fileName);
//		        	Scheme sinkScheme = new TextLine();
		        	Scheme sinkScheme = new TextLine(Compress.ENABLE);
					if(allPipeName.contains("mapping"))
						sinkScheme.setNumSinkParts(numOfReducersInt/2);
					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					
					if(allPipeName.contains("_click")||allPipeName.contains("_count"))
						sinkScheme.setNumSinkParts(1);
					
		        	Tap sink = new Hfs(sinkScheme, fileName);
		        	sinks_hourly.put(dataPipeName, sink);
		        }
		    		    	
		    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    	Fields keyFields = new Fields();
		    	for(int j=0; j<baseKeyFieldsArr.length;j++)
		    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
		    	ctrAssembly[num_pipes] = new HourlyAggregatingCTRBase(allAssembly[0], basePipeName, keyFields).getTails()[0];

		    	Pipe ctrAssembly_tmp = new Pipe(ctrAssembly[num_pipes].getName()+"_tmp", ctrAssembly[num_pipes]);
		    	String fileName = outFilePathDate + basePipeName + "_hourly";
		    	sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
		    	adoptUtils.deleteFile(fileName);
//		    	Scheme sinkScheme = new TextLine();
		    	Scheme sinkScheme = new TextLine(Compress.ENABLE);
				sinkScheme.setNumSinkParts(numOfReducersInt);
		    	Tap sink = new Hfs(sinkScheme, fileName);
		    	sinks_hourly.put(basePipeName, sink);
		    
		    	for(int i=0; i<num_pipes; i++){
		    		String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    		Fields keyFieldsMore = new Fields();
		    		for(int j=0; j<keyFieldsMoreArr.length;j++)
		    			keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    		ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];
		    		fileName = outFilePathDate + pipeNamesArr[i] + "_hourly";
//		    		sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
		    		adoptUtils.deleteFile(fileName);
//		    		sinkScheme = new TextLine();
		    		sinkScheme = new TextLine(Compress.ENABLE);
//					if(pipeNamesArr[i].length()<20)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
		    		sink = new Hfs(sinkScheme, fileName);
		    		sinks_hourly.put(pipeNamesArr[i], sink);
		    	}
		    
		    	sLogger.info("Connecting the hourly pipe line and assemblys ... ");
		    	long time_s0 = System.currentTimeMillis();
		    	Properties properties = new Properties();
				properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
		    	properties.setProperty("mapred.reduce.tasks", numOfReducers);

			    AppProps.setApplicationJarClass(properties, HourlyGeneratingCTR.class);
			    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			    Flow flow = flowConnector.connect("ctr_hourly", sources_hourly, sinks_hourly, ctrAssembly);
			    flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROneHour");
		    	sLogger.info("Generating the hourly ctr at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is either no impression data or no click data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	

	
	public void generatingCTRHourlyNew(Configuration config, int hour) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String allFilePath = config.get("AllFilePath");
			String allFileName = config.get("AllFileName");
			String basePipeName = config.get("BasePipeNameHourly");
			String baseKeyFields = config.get("BaseKeyFieldsHourly");
			String pipeNamesList = config.get("PipeNamesListHourly");
			String keyFieldsList = config.get("KeyFieldsListHourly");
			String mapKeys = config.get("MapKeys");
			String mapPipeNames = config.get("MapPipeNames");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\nallFilePath = " + allFilePath +
					"\nallFileName = " + allFileName +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields +
					"\nmapPipeNames = " + mapPipeNames +
					"\nmapKeys = " + mapKeys);
			generatingCTROneHourNew(config, pDateStr, outFilePathHourly, allFilePath, allFileName, basePipeName, 
					baseKeyFields, pipeNamesList, keyFieldsList, hour, mapPipeNames, mapKeys);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROneHourNew(Configuration config, String pDateStr, String outFilePath, 
			String allFilePath, String allFileName, String basePipeName, String baseKeyFields, 
			String pipeNamesList, String keyFieldsList, int hour, String mapPipeNames, 
			String mapKeys) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			String hourFolder = CTRConstants.hourFolders[hour];
			sLogger.info("\nStarting to generate the hourly ctr at hour " + hourFolder + " on " + pDateStr + " ...");			
			Map<String, Tap> sources_hourly = new HashMap<String, Tap>();
			Map<String, Tap> sinks_hourly = new HashMap<String, Tap>();
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+hourFolder+"/";
            adoptUtils.makeFileDir(outFilePathDate);
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = null;
		    int numOfPipes = 0;
	    	int num_pipes = 0;
		    if(pipeNamesList!=null){
		    	pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    	numOfPipes = pipeNamesArr.length;
		    }
		    num_pipes = numOfPipes;
		    Pipe[] allAssembly = null;
		    
		    allAssembly = new HourlyDataImpClickPriceBidNew(sources_hourly, allFilePath, allFileName,
					pDateStr, hour, mapPipeNames, mapKeys, config).getTails();	
		    	
		    if(!(allAssembly[0]==null)){			    
		    	int numOfAssemblys = allAssembly.length;
		    	Pipe[] ctrAssembly = new Pipe[1+num_pipes+numOfAssemblys];
		        for(int i=0; i<numOfAssemblys; i++){
		        	String allPipeName = allAssembly[i].getName();
		        	String dataPipeName = "rtb_" + allPipeName;
		        	ctrAssembly[1+num_pipes+i] = new Pipe(dataPipeName, allAssembly[i]);
		        	String fileName = outFilePathDate + allPipeName + "_hourly";
		        	sLogger.info("pipeName: " + dataPipeName + "    fileName: " + fileName);
		        	adoptUtils.deleteFile(fileName);
//		        	Scheme sinkScheme = new TextLine();
		        	Scheme sinkScheme = new TextLine(Compress.ENABLE);
					if(allPipeName.contains("mapping"))
						sinkScheme.setNumSinkParts(numOfReducersInt/2);
					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					
					if(allPipeName.contains("_click")||allPipeName.contains("_count"))
						sinkScheme.setNumSinkParts(1);
					
		        	Tap sink = new Hfs(sinkScheme, fileName);
		        	sinks_hourly.put(dataPipeName, sink);
		        }
		    		    	
		    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    	Fields keyFields = new Fields();
		    	for(int j=0; j<baseKeyFieldsArr.length;j++)
		    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
		    	ctrAssembly[num_pipes] = new HourlyAggregatingCTRBase(allAssembly[0], basePipeName, keyFields).getTails()[0];

		    	Pipe ctrAssembly_tmp = new Pipe(ctrAssembly[num_pipes].getName()+"_tmp", ctrAssembly[num_pipes]);
		    	String fileName = outFilePathDate + basePipeName + "_hourly";
		    	sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
		    	adoptUtils.deleteFile(fileName);
//		    	Scheme sinkScheme = new TextLine();
		    	Scheme sinkScheme = new TextLine(Compress.ENABLE);
				sinkScheme.setNumSinkParts(numOfReducersInt);
		    	Tap sink = new Hfs(sinkScheme, fileName);
		    	sinks_hourly.put(basePipeName, sink);
		    
		    	for(int i=0; i<num_pipes; i++){
		    		String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    		Fields keyFieldsMore = new Fields();
		    		for(int j=0; j<keyFieldsMoreArr.length;j++)
		    			keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    		ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];
		    		fileName = outFilePathDate + pipeNamesArr[i] + "_hourly";
//		    		sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
		    		adoptUtils.deleteFile(fileName);
//		    		sinkScheme = new TextLine();
		    		sinkScheme = new TextLine(Compress.ENABLE);
//					if(pipeNamesArr[i].length()<20)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
		    		sink = new Hfs(sinkScheme, fileName);
		    		sinks_hourly.put(pipeNamesArr[i], sink);
		    	}
		    
		    	sLogger.info("Connecting the hourly pipe line and assemblys ... ");
		    	long time_s0 = System.currentTimeMillis();
		    	Properties properties = new Properties();
				properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
		    	properties.setProperty("mapred.reduce.tasks", numOfReducers);

			    AppProps.setApplicationJarClass(properties, HourlyGeneratingCTR.class);
			    FlowConnector flowConnector = new HadoopFlowConnector(properties);
			    Flow flow = flowConnector.connect("ctr_hourly", sources_hourly, sinks_hourly, ctrAssembly);
			    flow.complete();
		    	
		    	long time_s1 = System.currentTimeMillis();
		    	adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROneHour");
		    	sLogger.info("Generating the hourly ctr at hour " + hourFolder + " is done.\n");
		    }else{
				sLogger.info("There is either no impression data or no click data for the hour " + hourFolder + " on " + pDateStr + "\n");	
				throw new Exception("no data");
		    }
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  			
}
