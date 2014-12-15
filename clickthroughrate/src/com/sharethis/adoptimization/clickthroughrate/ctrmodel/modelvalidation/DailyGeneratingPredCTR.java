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

public class DailyGeneratingPredCTR 
{	
	private static final Logger sLogger = Logger.getLogger(DailyGeneratingPredCTR.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public DailyGeneratingPredCTR(){
	}
		
	public void dailyGeneratingPredCTR(Configuration config, String basePipeName, String baseKeyFields, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\noutFilePathDaily = " + outFilePathDaily +
					"\nbasePipeName = " + basePipeName +
					"\nbaseKeyFields = " + baseKeyFields);
			aggregatingDailyPredCTR(config, pDateStr, outFilePathHourly, outFilePathDaily, 
					basePipeName, baseKeyFields, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyPredCTR(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String basePipeName, String baseKeyFields, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			if(basePipeName.contains("mv_ctr"))
				numOfReducersInt = 1;
			sLogger.info("\nStarting to generate the daily predicted ctr on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_imps", "sum_clicks", "sum_cost", "sum_clicks_pred", "ctr", "e_cpm", "e_cpc", "ctr_pred");
		    
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
	    	for(int j=baseKeyFieldsArr.length; j<pFields.size()-2; j++){
	    		types[j] = Double.class;
	    	}
	    	types[pFields.size()-2] = String.class;
	    	types[pFields.size()-1] = String.class;
	    		    	
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));
	    	int endHour = config.getInt("EndHour", 7);
	    	Pipe ctrAssembly = new DailyAggregationPredCTRPipe(sources_agg, outFilePathHourly, pDateStr1, 
	    			24, 1, config, keyFields, pFields, basePipeName, endHour, types, infile_postfix).getTails()[0];

		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			
			String fileName = outFilePathDate + basePipeName + outfile_postfix;
	
			sLogger.info("pipeName: " + basePipeName + "    fileName: " + fileName);
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();
			
			adoptUtils.deleteFile(fileName);
			Scheme sinkScheme = new TextLine();
			//Scheme sinkScheme = new TextLine(Compress.ENABLE);
    		sinkScheme.setNumSinkParts(numOfReducersInt);
			Tap sink = new Hfs(sinkScheme, fileName);
			sinks_agg.put(basePipeName, sink);			
				    
			sLogger.info("Connecting the daily predicted ctr aggregation pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, DailyGeneratingPredCTR.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily_pred", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyPredictedCTR");
			sLogger.info("Generating the daily predicted ctr aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	 		
}
