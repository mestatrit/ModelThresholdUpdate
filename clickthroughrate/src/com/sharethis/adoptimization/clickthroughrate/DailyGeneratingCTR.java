package com.sharethis.adoptimization.clickthroughrate;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import com.sharethis.adoptimization.common.AdoptimizationUtils;


/**
 * This is the class to run the flow.
 */

public class DailyGeneratingCTR 
{	
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingCTR.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public DailyGeneratingCTR(){
	}
		
	public void dailyGeneratingCTR(Configuration config, String basePipeName, 
			String baseKeyFields, String pipeNamesList, String keyFieldsList,
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePath = config.get("OutFilePath");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePath = " + outFilePath +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList);
			aggregatingDailyCTR(config, pDateStr, outFilePath, basePipeName, 
					baseKeyFields, pipeNamesList, keyFieldsList, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyCTR(Configuration config, String pDateStr,  
			String outFilePath, String basePipeName, 
			String baseKeyFields, String pipeNamesList, String keyFieldsList, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the daily ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = null;
			if(outfile_postfix.endsWith("_agg"))
				ctrAssembly = new Pipe[2*numOfPipes+1];
			else
				ctrAssembly = new Pipe[numOfPipes+1];
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields pFields = keyFields.append(dataFields);
	    	Class[] types = new Class[pFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-1] = String.class;
	    		    	
	    	int endHour = config.getInt("EndHour", 7);
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));
	    	ctrAssembly[numOfPipes] = new DailyAggregationCTRPipe(sources_agg, outFilePath, pDateStr1, 
	    			24, 1, config, keyFields, pFields, basePipeName, endHour, types, infile_postfix).getTails()[0];

		    Pipe ctrAssembly_tmp = new Pipe(ctrAssembly[numOfPipes].getName()+"_tmp", ctrAssembly[numOfPipes]);
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+"/";
			
			String fileName = outFilePathDate + basePipeName + outfile_postfix;
	
			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			
			adoptUtils.deleteFile(fileName);
			Scheme sinkScheme = new TextLine();
//			if (!"adg".equalsIgnoreCase(basePipeName))
//				sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);			
		
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];
				fileName = outFilePathDate + pipeNamesArr[i] + outfile_postfix;
				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
//				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
//					sinkScheme = new TextLine(Compress.ENABLE);
//				else
				sinkScheme = new TextLine();
//				if(pipeNamesArr[i].length()<10)
//					sinkScheme.setNumSinkParts(5);
//				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);				

				//Constructing the required output format
				if(outfile_postfix.endsWith("_agg")){
					String pipeName = "rtb_" + ctrAssembly[i].getName();
					Fields resultFields = new Fields("type", "keys");
						
					Pipe ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					Fields allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					Function<?> keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
						
					ctrAssembly[numOfPipes+1+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					adoptUtils.deleteFile(fileName);
//					sinkScheme = new TextLine();
					sinkScheme = new TextLine(Compress.ENABLE);
//					if(pipeName.length()<24)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);	
				}					
		    }
		    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyGeneratingCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyCTR");
			sLogger.info("Generating the daily ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	
	
	public void dailyGeneratingCTREach(Configuration config, String pipeNamesList, String keyFieldsList, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePath = config.get("OutFilePath");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePath = " + outFilePath +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList);
			aggregatingDailyCTREach(config, pDateStr, outFilePath, pipeNamesList, 
					keyFieldsList, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyCTREach(Configuration config, String pDateStr, String outFilePath, 
			String pipeNamesList, String keyFieldsList, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the daily ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
	    	
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));	    	
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+"/";
			
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();			
			Scheme sinkScheme = new TextLine();
			Pipe[] ctrAssembly = null;
			if(outfile_postfix.endsWith("_agg"))
				ctrAssembly = new Pipe[2*numOfPipes];
			else
				ctrAssembly = new Pipe[numOfPipes];

	    	int endHour = config.getInt("EndHour", 7);
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	Fields pFields = keyFieldsMore.append(dataFields);
		    	Class[] types = new Class[pFields.size()];
		    	for(int j=0; j<keyFieldsMoreArr.length;j++){
		    		types[j] = String.class;
		    	}
		    	for(int j=keyFieldsMoreArr.length; j<pFields.size()-1; j++){
		    		types[j] = Double.class;
		    	}
		    	types[pFields.size()-1] = String.class;
		    	
		    	ctrAssembly[i] = new DailyAggregationCTRPipe(sources_agg, outFilePath, pDateStr1, 
		    			24, 1, config, keyFieldsMore, pFields, pipeNamesArr[i], endHour, types, infile_postfix).getTails()[0];
				String fileName = outFilePathDate + pipeNamesArr[i] + outfile_postfix;
				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
					sinkScheme = new TextLine(Compress.ENABLE);
//				if(pipeNamesArr[i].length()<10)
//					sinkScheme.setNumSinkParts(5);
//				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);	

				//Constructing the required output format
				if(outfile_postfix.endsWith("_agg")){
					String pipeName = "rtb_" + ctrAssembly[i].getName();
					Fields resultFields = new Fields("type", "keys");
					
					Pipe ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					Fields allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					Function<?> keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
					ctrAssembly[numOfPipes+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					adoptUtils.deleteFile(fileName);
					sinkScheme = new TextLine();
//					sinkScheme = new TextLine(Compress.ENABLE);
//					if(pipeName.length()<24)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);	
				}				
		    }
		    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, DailyGeneratingCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyCTREach");
			sLogger.info("Generating the daily ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		

	public void dailyGeneratingCTRAll(Configuration config, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\noutFilePathDaily = " + outFilePathDaily +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList);
			aggregatingDailyCTROnePipe(config, pDateStr, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, infile_postfix, outfile_postfix, "ctr_daily_base");
			aggregatingDailyCTRAll(config, pDateStr, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, outfile_postfix);
			String extraPipeNameList = config.get("ExtraPipeNameListDaily");
			String extraKeyFieldList = config.get("ExtraKeyFieldListDaily");
			if(extraPipeNameList!=null&&!extraPipeNameList.isEmpty()){
				String[] extraPipeNames = StringUtils.split(extraPipeNameList, ";");
				String[] extraKeyFields = StringUtils.split(extraKeyFieldList, ";");
				if(extraPipeNames!=null&&extraKeyFields!=null&&extraPipeNames.length>0){
					if(extraPipeNames.length==extraKeyFields.length){
						for(int i=0; i<extraPipeNames.length; i++){
							String taskName = "ctr_daily_extra_"+i;
							aggregatingDailyCTROnePipe(config, pDateStr, outFilePathHourly, outFilePathDaily, 
								extraPipeNames[i], extraKeyFields[i], infile_postfix, outfile_postfix, taskName);
						}
					}else{
						sLogger.warn("The name list and the key list for the extra daily aggregation have the different size.");
					}
				}else{
					sLogger.info("There is no extra daily aggregation.");
				}
			}else{
				sLogger.info("There is no extra daily aggregation.");
			}
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyCTRBase(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			sLogger.info("\nStarting to generate the daily ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields pFields = keyFields.append(dataFields);
	    	Class[] types = new Class[pFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-1] = String.class;
	    		    	
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));
	    	int endHour = config.getInt("EndHour", 7);
	    	Pipe ctrAssembly = new DailyAggregationCTRPipe(sources_agg, outFilePathHourly, pDateStr1, 
	    			24, 1, config, keyFields, pFields, basePipeName, endHour, types, infile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			
			String fileName = outFilePathDate + basePipeName + outfile_postfix;
	
			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			
			adoptUtils.deleteFile(fileName);
			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);			
				    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyGeneratingCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily_base", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyCTR");
			sLogger.info("Generating the daily ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	

	private void aggregatingDailyCTROnePipe(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, 
			String infile_postfix, String outfile_postfix, String taskName) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			sLogger.info("\nStarting to generate the daily ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields pFields = keyFields.append(dataFields);
	    	Class[] types = new Class[pFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-1] = String.class;
	    		    	
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));
	    	int endHour = config.getInt("EndHour", 7);
	    	Pipe ctrAssembly = new DailyAggregationCTRPipe(sources_agg, outFilePathHourly, pDateStr1, 
	    			24, 1, config, keyFields, pFields, basePipeName, endHour, types, infile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			
			String fileName = outFilePathDate + basePipeName + outfile_postfix;
	
			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			
			adoptUtils.deleteFile(fileName);
			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);			
				    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyGeneratingCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect(taskName, sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyCTR");
			sLogger.info("Generating the daily ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	
	
	private void aggregatingDailyCTRAll(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the daily ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = null;
			if(outfile_postfix.endsWith("_agg"))
				ctrAssembly = new Pipe[2*numOfPipes];
			else
				ctrAssembly = new Pipe[numOfPipes];
		    
	    	String[] baseKeyFieldsArr = StringUtils.split(baseKeyFields, ",");
		    Fields keyFields = new Fields();
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		keyFields = keyFields.append(new Fields(baseKeyFieldsArr[j]));
	    	}
	    	Fields pFields = keyFields.append(dataFields);
	    	Class[] types = new Class[pFields.size()];
	    	for(int j=0; j<baseKeyFieldsArr.length;j++){
	    		types[j] = String.class;
	    	}
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-1; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-1] = String.class;
	    		    	
		    Pipe ctrAssembly_tmp = new HourlyBaseDataAssembly(sources_agg, outFilePathHourly, outFilePathDaily, 
		    		pDateStr, config, pFields, basePipeName, types, -1, outfile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";			
            adoptUtils.makeFileDir(outFilePathDate);
            
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();		
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];
				String fileName = outFilePathDate + pipeNamesArr[i] + outfile_postfix;
				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				Scheme sinkScheme = new TextLine();
				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
					sinkScheme = new TextLine(Compress.ENABLE);
//				if(pipeNamesArr[i].length()<10)
//					sinkScheme.setNumSinkParts(5);
//				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);				

				//Constructing the required output format
				if(outfile_postfix.endsWith("_agg")){
					String pipeName = "rtb_" + ctrAssembly[i].getName();
					Fields resultFields = new Fields("type", "keys");

					Pipe ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					Fields allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					Function<?> keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
					ctrAssembly[numOfPipes+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					adoptUtils.deleteFile(fileName);
//					sinkScheme = new TextLine();
					sinkScheme = new TextLine(Compress.ENABLE);
//					if(pipeName.length()<24)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);	
				}					
		    }
		    
			sLogger.info("Connecting the daily aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyGeneratingCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily_all", sources_agg, sinks_agg, ctrAssembly);
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
