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

public class MappingsDailyGeneration 
{	
	private static final Logger sLogger = Logger.getLogger(MappingsDailyGeneration.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public MappingsDailyGeneration(){
	}
			
	public void dailyGeneratingMappings(Configuration config, String mapPipeNames, String mapKeys, 
			String infile_postfix, String outfile_postfix) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String outFilePathHourly = config.get("OutFilePathHourly");
			String outFilePathDaily = config.get("OutFilePathDaily");
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePathHourly = " + outFilePathHourly +
					"\noutFilePathDaily = " + outFilePathDaily +
					"\nmapPipeNames = " + mapPipeNames +
					"\nmapKeys = " + mapKeys);
			aggregatingDailyMappings(config, pDateStr, outFilePathHourly, outFilePathDaily, 
					mapPipeNames, mapKeys, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void aggregatingDailyMappings(Configuration config, String pDateStr, String outFilePathHourly, 
			String outFilePathDaily, String mapPipeNames, String mapKeys, String infile_postfix, 
			String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			sLogger.info("\nStarting to generate the daily mappings on " + pDateStr + " ...");			
			Map<String, Tap> sources_agg = new HashMap<String, Tap>();
			Fields dataFields = new Fields("sum_wt1", "sum_imp1");
		    String[] keyFieldsArr = StringUtils.split(mapKeys, ";");
		    String[] pipeNamesArr = StringUtils.split(mapPipeNames, ";");
		    int numOfPipes = pipeNamesArr.length;
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));	    	
		    String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePathDaily+dayStr+"/";
			
			Map<String, Tap> sinks_agg = new HashMap<String, Tap>();			
			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			Pipe[] ctrAssembly = new Pipe[numOfPipes];
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
		    	for(int j=keyFieldsMoreArr.length; j<pFields.size(); j++){
		    		types[j] = Double.class;
		    	}
		    	ctrAssembly[i] = new MappingsDataAggregation(sources_agg, outFilePathHourly, pDateStr1, 
		    			24, 1, config, keyFieldsMore, pFields, pipeNamesArr[i], 7, types, infile_postfix).getTails()[0];
				String fileName = outFilePathDate + pipeNamesArr[i] + outfile_postfix;
				sLogger.info("pipeName: " + pipeNamesArr[i] + "    fileName: " + fileName);
				adoptUtils.deleteFile(fileName);
				Tap sink = new Hfs(sinkScheme, fileName);
				sinks_agg.put(pipeNamesArr[i], sink);				
		    }
		    
			sLogger.info("Connecting the daily aggregation map pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", config.get("NumOfReducers"));
			
			AppProps.setApplicationID(properties);
		    AppProps.setApplicationJarClass(properties, MappingsDailyGeneration.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("map_daily", sources_agg, sinks_agg, ctrAssembly);
		    flow.complete();
			
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "AggregatingDailyMappings");
			sLogger.info("Generating the daily map aggregation on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  			
}
