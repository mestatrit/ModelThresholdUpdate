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

public class HourlyAggregationCTR 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationCTR.class);	
	
	public HourlyAggregationCTR(){
	}
		
	public void generatingCTRPeriods(Configuration config, int numOfPeriods, int interval, int hour, 
			String basePipeName, String baseKeyFields, String pipeNamesList, String keyFieldsList, 
			String infile_postfix, String outfile_postfix) throws Exception{
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
					"\nkeyFieldsList = " + keyFieldsList +
					"\nnumOfPeriodss = " + numOfPeriods);
			generatingCTROnePeriod(config, pDateStr, numOfPeriods, interval, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, hour, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROnePeriod(Configuration config, String pDateStr, int numOfPeriods, int interval, 
			String outFilePathHourly, String outFilePathDaily, String basePipeName, String baseKeyFields, 
			String pipeNamesList, String keyFieldsList,int hour, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("\nStarting to generate the periodically aggregated ctr on " + pDateStr + " ...");			
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
	    	
	    	ctrAssembly[numOfPipes] = new HourlyAggregationCTRPipe(sources_agg, outFilePathHourly, outFilePathDaily, pDateStr, 
	    			numOfPeriods, interval, config, keyFields, pFields, basePipeName, types, hour, infile_postfix).getTails()[0];

		    Pipe ctrAssembly_tmp = new Pipe(ctrAssembly[numOfPipes].getName()+"_tmp", ctrAssembly[numOfPipes]);
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
//            String outFilePathDate = outFilePath+dayStr+"/";
//			if(hour>=0){
//				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour] + "/";
//			}
			String outFilePathDate = null;
			if(hour>=0){
				outFilePathDate = outFilePathHourly + dayStr + CTRConstants.hourFolders[hour] + "/";
			}else{
				outFilePathDate = outFilePathDaily + dayStr + "/";					
			}
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
//			if (!"adg".equalsIgnoreCase(basePipeName))
//				sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = null;

            fileName = outFilePathDate + basePipeName + outfile_postfix;
//			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
			adoptUtils.deleteFile(fileName);
			sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);

			//Constructing the required output format
			String pipeName = null;
			Pipe ctrAssembly_i = null;
			Fields resultFields = null;
			Fields allFields = null;
			Function<?> keyFunc = null;
			
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];

		    	fileName = outFilePathDate + pipeNamesArr[i]  + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
//				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
//					sinkScheme = new TextLine(Compress.ENABLE);
//				else
					sinkScheme = new TextLine();
				if(pipeNamesArr[i].length()<20)
					sinkScheme.setNumSinkParts(5);
				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
					
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);
				
				//Constructing the required output format
				if(outfile_postfix.endsWith("_agg")){
					pipeName = "rtb_" + ctrAssembly[i].getName();
					ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					resultFields = new Fields("type", "keys");
					allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
					ctrAssembly[numOfPipes+1+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
//					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					adoptUtils.deleteFile(fileName);
					sinkScheme = new TextLine();
//					sinkScheme = new TextLine(Compress.ENABLE);
					if(pipeName.length()<24)
						sinkScheme.setNumSinkParts(5);
					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);		  
				}
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_aggregation", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriod");
			sLogger.info("Generating the aggregated ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
	
	public void generatingCTRPeriodsEach(Configuration config, int numOfPeriods, int interval, int hour, 
			String pipeNamesList, String keyFieldsList, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\noutFilePathDaily = " + outFilePathDaily +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList +
					"\nnumOfPeriods = " + numOfPeriods);
			generatingCTROnePeriodEach(config, pDateStr, numOfPeriods, interval, outFilePathHourly, 
					outFilePathDaily, pipeNamesList, keyFieldsList, hour, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROnePeriodEach(Configuration config, String pDateStr, int numOfPeriods, int interval, 
			String outFilePathHourly, String outFilePathDaily, String pipeNamesList, String keyFieldsList, int hour, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils pvUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the periodically aggregated ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
			Pipe[] ctrAssembly = new Pipe[numOfPipes];
			if(outfile_postfix.endsWith("_agg"))
				ctrAssembly = new Pipe[2*numOfPipes];
		    
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
//            String outFilePathDate = outFilePath+dayStr+"/";
//			if(hour>=0){
//				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour] + "/";
//			}
			String outFilePathDate = null;
			if(hour>=0){
				outFilePathDate = outFilePathHourly + dayStr + CTRConstants.hourFolders[hour] + "/";
			}else{
				outFilePathDate = outFilePathDaily + dayStr + "/";					
			}
            pvUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
//			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = null;
			
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

		    	ctrAssembly[i] = new HourlyAggregationCTRPipe(sources_agg, outFilePathHourly, outFilePathDaily, pDateStr, 
		    			numOfPeriods, interval, config, keyFieldsMore, pFields, pipeNamesArr[i], types, hour, infile_postfix).getTails()[0];

		    	fileName = outFilePathDate + pipeNamesArr[i]  + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				pvUtils.deleteFile(fileName);
				sinkScheme = new TextLine();
//				sinkScheme = new TextLine(Compress.ENABLE);
				if(pipeNamesArr[i].length()<20)
					sinkScheme.setNumSinkParts(5);
				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);
				
				if(outfile_postfix.endsWith("_agg")){
					//Constructing the required output format
					String pipeName = "rtb_" + ctrAssembly[i].getName();
					Pipe ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					Fields resultFields = new Fields("type", "keys");
					Fields allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					Function<?> keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
					ctrAssembly[numOfPipes+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
//					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					pvUtils.deleteFile(fileName);
					sinkScheme = new TextLine();
//					sinkScheme = new TextLine(Compress.ENABLE);
					if(pipeName.length()<24)
						sinkScheme.setNumSinkParts(5);
					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);	
				}
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_multiweeks", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriod");
			sLogger.info("Generating the aggregated ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
	
	
	public void generatingCTRPeriodsAll(Configuration config, int numOfPeriods, int interval, int hour, 
			String basePipeName, String baseKeyFields, String pipeNamesList, String keyFieldsList, 
			String infile_postfix, String outfile_postfix) throws Exception{
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
					"\nkeyFieldsList = " + keyFieldsList +
					"\nnumOfPeriodss = " + numOfPeriods);
			generatingCTROnePeriodBase(config, pDateStr, numOfPeriods, interval, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, hour, infile_postfix, outfile_postfix);
			generatingCTROnePeriodAll(config, pDateStr, outFilePathHourly, outFilePathDaily, basePipeName, baseKeyFields, pipeNamesList, 
					keyFieldsList, hour, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROnePeriodBase(Configuration config, String pDateStr, int numOfPeriods, int interval, 
			String outFilePathHourly, String outFilePathDaily, String basePipeName, String baseKeyFields, int hour, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String numOfReducers = config.get("NumOfReducers");
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("\nStarting to generate the periodically aggregated ctr on " + pDateStr + " ...");			
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
	    	
	    	Pipe ctrAssembly = new HourlyAggregationCTRPipe(sources_agg, outFilePathHourly, outFilePathDaily, pDateStr, 
	    			numOfPeriods, interval, config, keyFields, pFields, basePipeName, types, hour, infile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
//            String outFilePathDate = outFilePath+dayStr+"/";
//			if(hour>=0){
//				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour] + "/";
//			}
			String outFilePathDate = null;
			if(hour>=0){
				outFilePathDate = outFilePathHourly + dayStr + CTRConstants.hourFolders[hour] + "/";
			}else{
				outFilePathDate = outFilePathDaily + dayStr + "/";					
			}
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
//			if (!"adg".equalsIgnoreCase(basePipeName))
//				sinkScheme = new TextLine(Compress.ENABLE);
			Tap sink = null;

            fileName = outFilePathDate + basePipeName + outfile_postfix;
//			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
			adoptUtils.deleteFile(fileName);
			sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_aggregation_base", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriod");
			sLogger.info("Generating the aggregated ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
	
	private void generatingCTROnePeriodAll(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, String keyFieldsList, 
			int hour, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("\nStarting to generate the periodically aggregated ctr on " + pDateStr + " ...");			
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
//            String outFilePathDate = outFilePath+dayStr+"/";
//			if(hour>=0){
//				outFilePathDate = outFilePathDate + CTRConstants.hourFolders[hour] + "/";
//			}
			String outFilePathDate = null;
			if(hour>=0){
				outFilePathDate = outFilePathHourly + dayStr + CTRConstants.hourFolders[hour] + "/";
			}else{
				outFilePathDate = outFilePathDaily + dayStr + "/";					
			}
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			String fileName = null;
			Scheme sinkScheme = new TextLine();
			Tap sink = null;

			//Constructing the required output format
			String pipeName = null;
			Pipe ctrAssembly_i = null;
			Fields resultFields = null;
			Fields allFields = null;
			Function<?> keyFunc = null;
			
		    for(int i=0; i<numOfPipes; i++){
		    	String[] keyFieldsMoreArr = StringUtils.split(keyFieldsArr[i], ",");
		    	Fields keyFieldsMore = new Fields();
		    	for(int j=0; j<keyFieldsMoreArr.length;j++)
		    		keyFieldsMore = keyFieldsMore.append(new Fields(keyFieldsMoreArr[j]));
		    	ctrAssembly[i] = new AggregatingCTRAll(new Pipe[]{ctrAssembly_tmp}, pipeNamesArr[i], keyFieldsMore).getTails()[0];

		    	fileName = outFilePathDate + pipeNamesArr[i]  + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
//				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
//					sinkScheme = new TextLine(Compress.ENABLE);
//				else
					sinkScheme = new TextLine();
				if(pipeNamesArr[i].length()<20)
					sinkScheme.setNumSinkParts(5);
				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
					
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);
				
				//Constructing the required output format
				if(outfile_postfix.endsWith("_agg")){
					pipeName = "rtb_" + ctrAssembly[i].getName();
					ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
					resultFields = new Fields("type", "keys");
					allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
					keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
					ctrAssembly[numOfPipes+i] = new Each(ctrAssembly_i, keyFieldsMore, keyFunc, allFields);
					fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
//					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
					adoptUtils.deleteFile(fileName);
					sinkScheme = new TextLine();
//					sinkScheme = new TextLine(Compress.ENABLE);
					if(pipeName.length()<24)
						sinkScheme.setNumSinkParts(5);
					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);		  
				}
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_aggregation_all", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriod");
			sLogger.info("Generating the aggregated ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 	
}
