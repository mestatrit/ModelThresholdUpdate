package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

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


/**
 * This is the class to run the flow.
 */

public class HourlyAggregationPredCTRRolling 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationPredCTRRolling.class);	
	
	public HourlyAggregationPredCTRRolling(){
	}
			
	public void generatingPredCTRPeriodsEach(Configuration config, int interval1, int interval2, int hour, 
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
			generatingPredCTROnePeriodEach(config, pDateStr, interval1, interval2, outFilePathHourly, outFilePathDaily,
					pipeNamesList, keyFieldsList, hour, infile_postfix_rolling, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingPredCTROnePeriodEach(Configuration config, String pDateStr, int interval1, int interval2,
			String outFilePathHourly, String outFilePathDaily, String pipeNamesList, String keyFieldsList, int hour, 
			String infile_postfix_rolling, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils pvUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			double impThVal = config.getFloat("ImpThVal", (float) 0.0);
			sLogger.info("\nStarting to generate the periodically aggregated predicted ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "sum_clicks_pred", "ctr", "e_cpm", "e_cpc", "ctr_pred");
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
			//Scheme sinkScheme = new TextLine();
			Scheme sinkScheme = new TextLine(Compress.ENABLE);
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
		    	for(int j=keyFieldsMoreArr.length; j<pFields.size()-2; j++){
		    		types[j] = Double.class;
		    	}
		    	types[pFields.size()-2] = String.class;
		    	types[pFields.size()-1] = String.class;
		    	
		    	Pipe ctrAssembly_tmp = new HourlyAggregationPredCTRPipeRolling(sources_agg, outFilePathHourly, outFilePathDaily, 
		    			pDateStr, interval1, interval2, keyFieldsMore, pFields, pipeNamesArr[i], types, hour, outfile_postfix, 
		    			infile_postfix_rolling, keyFieldsMoreArr.length, dataFields.size(), impThVal).getTails()[0];
		    	if(ctrAssembly_tmp!=null){
		    		ctrPipe.add(cnt_pipe, ctrAssembly_tmp);
		    		keyList.add(cnt_pipe, keyFieldsMore);
		    		cnt_pipe++;
		    	}
		    }
			Pipe[] ctrAssembly = new Pipe[cnt_pipe];
//			if(outfile_postfix.endsWith("_agg"))
//				ctrAssembly = new Pipe[2*cnt_pipe];
			for(int i=0; i<cnt_pipe; i++){
				ctrAssembly[i] = ctrPipe.get(i);
//		    	if(ctrAssembly[i]!=null){
				String pipeName_tmp = ctrAssembly[i].getName();
		    	fileName = outFilePathDate + pipeName_tmp + outfile_postfix;
//				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
		    	pvUtils.deleteFile(fileName);
//		    	sinkScheme = new TextLine();
				sinkScheme = new TextLine(Compress.ENABLE);
		    	sinkScheme.setNumSinkParts(numOfReducersInt);
		    	sink = new Hfs(sinkScheme, fileName);
		    	sinks_agg.put(pipeName_tmp, sink);				
		    }
		    
			sLogger.info("Connecting the aggregated pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);

		    AppProps.setApplicationJarClass(properties, HourlyAggregationPredCTRRolling.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_aggregations_rolling", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			pvUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingCTROnePeriodEachRolling");
			sLogger.info("Generating the aggregated predicted ctr on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	} 
}
