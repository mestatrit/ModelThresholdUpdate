package com.sharethis.adoptimization.clickthroughrate;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Filter;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;

import com.sharethis.adoptimization.common.AdoptimizationUtils;
import com.sharethis.adoptimization.common.CTRConstants;
import com.sharethis.adoptimization.common.FilterOutDataLEDouble;


/**
 * This is the class to run the flow.
 */

public class HourlyAggregationCTRRolling 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationCTRRolling.class);	
	
	public HourlyAggregationCTRRolling(){
	}
		
	public void generatingCTRRolling(Configuration config, int interval1, int interval2, int hour, 
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
					"\nkeyFieldsList = " + keyFieldsList);
			generatingCTROnePeriod(config, pDateStr, interval1, interval2, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, pipeNamesList, keyFieldsList, hour, infile_postfix, outfile_postfix);
//			gettingCTROneData(config, pDateStr, interval1, interval2, outFilePath, basePipeName, baseKeyFields, 
//					pipeNamesList, keyFieldsList, hour, infile_postfix, outfile_postfix);
			
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROnePeriod(Configuration config, String pDateStr, int interval1, int interval2,
			String outFilePathHourly, String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, int hour, String infile_postfix_rolling, String outfile_postfix) throws Exception{
		try{
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			double impThVal = config.getFloat("ImpThVal", (float) 0.9999);
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
	    	
	    	ctrAssembly[numOfPipes] = new HourlyAggregationCTRPipeRolling(sources_agg, outFilePathHourly, outFilePathDaily, pDateStr, 
	    			interval1, interval2, keyFields, pFields, basePipeName, types, hour, outfile_postfix, infile_postfix_rolling, 
	    			baseKeyFieldsArr.length, dataFields.size(), 0).getTails()[0];

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
				//Need to filter out the records with sum_imps<1.0.
				Filter<?> filter = new FilterOutDataLEDouble(impThVal);
				ctrAssembly[i] = new Each(ctrAssembly[i], new Fields("sum_imps"), filter); 

		    	fileName = outFilePathDate + pipeNamesArr[i]  + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
//				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
//					sinkScheme = new TextLine(Compress.ENABLE);
//				else
					sinkScheme = new TextLine();
//				if(pipeNamesArr[i].length()<20)
//					sinkScheme.setNumSinkParts(5);
//				else
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
//					if(pipeName.length()<24)
//						sinkScheme.setNumSinkParts(5);
//					else
						sinkScheme.setNumSinkParts(numOfReducersInt);
					sink = new Hfs(sinkScheme, fileName);
					sinks_agg.put(pipeName, sink);		  
				}
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTRRolling.class);
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
	
	private void gettingCTROneData(Configuration config, String pDateStr, int interval1, int interval2, 
			String outFilePathHourly, String outFilePathDaily, String basePipeName, String baseKeyFields, String pipeNamesList, 
			String keyFieldsList, int hour, String infile_postfix_rolling, String outfile_postfix) throws Exception{
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

			ctrAssembly = new HourlyAggregationAssemblyRolling(sources_agg, outFilePathHourly, outFilePathDaily, 
					pDateStr, interval1, interval2, pFields, basePipeName, types, hour, outfile_postfix, 
					infile_postfix_rolling, baseKeyFieldsArr.length, dataFields.size()).getTails();		

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

			for(int i=0; i<ctrAssembly.length; i++){
				String pipeName = ctrAssembly[i].getName();
		    	fileName = outFilePathDate + pipeName  + "_rolling" + outfile_postfix;
//				sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
//				if (!"adg".equalsIgnoreCase(pipeNamesArr[i]))
//					sinkScheme = new TextLine(Compress.ENABLE);
//				else
					sinkScheme = new TextLine();
//				if(pipeName.length()<20)
//					sinkScheme.setNumSinkParts(5);
//				else
					sinkScheme.setNumSinkParts(numOfReducersInt);
					
				sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeName, sink);
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTRRolling.class);
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
	
	public void generatingCTRPeriodsEach(Configuration config, int interval1, int interval2, int hour, 
			String pipeNamesList, String keyFieldsList, String infile_postfix_rolling, 
			String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\noutFilePathDaily = " + outFilePathDaily +
					"\npipeNamesList = " + pipeNamesList +
					"\nkeyFieldsList = " + keyFieldsList);
			generatingCTROnePeriodEach(config, pDateStr, interval1, interval2, outFilePathHourly, outFilePathDaily,
					pipeNamesList, keyFieldsList, hour, infile_postfix_rolling, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingCTROnePeriodEach(Configuration config, String pDateStr, int interval1, int interval2,
			String outFilePathHourly, String outFilePathDaily, String pipeNamesList, String keyFieldsList, int hour, 
			String infile_postfix_rolling, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils pvUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			double impThVal = config.getFloat("ImpThVal", (float) 0.9);
			sLogger.info("\nStarting to generate the periodically aggregated ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc");
		    String[] keyFieldsArr = StringUtils.split(keyFieldsList, ";");
		    String[] pipeNamesArr = StringUtils.split(pipeNamesList, ";");
		    int numOfPipes = pipeNamesArr.length;
		    
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
			
			List<Pipe> ctrPipe = new ArrayList<Pipe>(0);
			List<Fields> keyList = new ArrayList<Fields>(0);
			Map<String, Tap> sources = new HashMap<String, Tap>();
			int cnt_pipe = 0;
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

		    	Pipe ctrAssembly_tmp = new HourlyAggregationCTRPipeRolling(sources_agg, outFilePathHourly, outFilePathDaily, 
		    			pDateStr, interval1, interval2, keyFieldsMore, pFields, pipeNamesArr[i], types, hour, outfile_postfix, 
		    			infile_postfix_rolling, keyFieldsMoreArr.length, dataFields.size(), impThVal).getTails()[0];
		    	if(ctrAssembly_tmp!=null){
		    		ctrPipe.add(cnt_pipe, ctrAssembly_tmp);
		    		keyList.add(cnt_pipe, keyFieldsMore);
		    		cnt_pipe++;
		    	}
		    }
			Pipe[] ctrAssembly = new Pipe[cnt_pipe];
//************************************************************			
//			if(outfile_postfix.endsWith("_agg"))
//				ctrAssembly = new Pipe[2*cnt_pipe];
//************************************************************			
			for(int i=0; i<cnt_pipe; i++){
				ctrAssembly[i] = ctrPipe.get(i);
//		    	if(ctrAssembly[i]!=null){
				String pipeName_tmp = ctrAssembly[i].getName();
		    	fileName = outFilePathDate + pipeName_tmp + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
		    	pvUtils.deleteFile(fileName);
		    	sinkScheme = new TextLine();
//				sinkScheme = new TextLine(Compress.ENABLE);
//		    	if(pipeNamesArr[i].length()<20)
//		    		sinkScheme.setNumSinkParts(5);
//		    	else
		    		sinkScheme.setNumSinkParts(numOfReducersInt);
		    	sink = new Hfs(sinkScheme, fileName);
		    	sinks_agg.put(pipeName_tmp, sink);
		    	
//************************************************************							
/*
 		    	if(outfile_postfix.endsWith("_agg")&&pipeName_tmp.length()<20){
		    		//Constructing the required output format
		    		String pipeName = "rtb_" + ctrAssembly[i].getName();
		    		Pipe ctrAssembly_i = new Pipe(pipeName, ctrAssembly[i]);
		    		Fields resultFields = new Fields("type", "keys");
		    		Fields allFields = resultFields.append(new Fields("sum_imps", "sum_clicks", "ctr"));
		    		Function<?> keyFunc = new ConstructingCTRKeys(resultFields, ctrAssembly[i].getName());
		    		ctrAssembly[cnt_pipe+i] = new Each(ctrAssembly_i, keyList.get(i), keyFunc, allFields);
		    		fileName = outFilePathDate + "rtb/" + pipeName  + outfile_postfix;
//					sLogger.info("pipeName: " + pipeName + "    fileName: " + fileName);
		    		pvUtils.deleteFile(fileName);
		    		sinkScheme = new TextLine();
//					sinkScheme = new TextLine(Compress.ENABLE);
//		    		if(pipeName.length()<24)
//		    			sinkScheme.setNumSinkParts(5);
//		    		else
		    			sinkScheme.setNumSinkParts(numOfReducersInt);
		    		sink = new Hfs(sinkScheme, fileName);
		    		sinks_agg.put(pipeName, sink);	
		    	}
*/
//************************************************************	
		    	
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_aggregations_rolling", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriodEachRolling");
			sLogger.info("Generating the aggregated ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 

}
