package com.sharethis.adoptimization.clickthroughrate;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
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

public class ModDailyGeneratingData 
{	
	private static final Logger sLogger = Logger.getLogger(ModDailyGeneratingData.class);	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public ModDailyGeneratingData(){
	}
		
	public void dailyGeneratingData(Configuration config) throws Exception{
		try{
			String pDateStr = config.get("ProcessingDate");			
			String inFilePath = config.get("ModFilePathHourly");
			String outFilePath = config.get("OutFilePathDaily");
			String nonClickName = config.get("ModDataNonClick");
			String clickName = config.get("ModDataClick");
			String modDataName = config.get("ModDataName");
			String infile_postfix = "_hourly";
			String outfile_postfix = "_daily";
			sLogger.info("\nThe parameters: \nProcessingDate="+pDateStr + 
					"\noutFilePath = " + outFilePath);
			generatingDailyData(config, pDateStr, inFilePath, outFilePath, nonClickName, 
					clickName, modDataName, infile_postfix, outfile_postfix);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  		
	
	private void generatingDailyData(Configuration config, String pDateStr,  
			String inFilePath, String outFilePath, String nonClickName, String clickName,
			String modDataName, String infile_postfix, String outfile_postfix) throws Exception{
		try{
			AdoptimizationUtils adoptUtils = new AdoptimizationUtils(config);
			String numOfReducers = config.get("NumOfReducers");
			int numOfReducersInt = Integer.parseInt(numOfReducers);
			sLogger.info("\nStarting to generate the daily sampled data on " + pDateStr + " ...");			
			Map<String, Tap> sources = new HashMap<String, Tap>();
			int numOfHours = config.getInt("NumOfHours", 24);
	    	int endHour = config.getInt("EndHour", 7);
	    	String pDateStr1 = sdf.format(DateUtils.addDays(sdf.parse(pDateStr), 1));

            Pipe modAssembly = new ModDailyAllData(sources, inFilePath, pDateStr1, config, nonClickName, 
            		clickName, modDataName, numOfHours, endHour, infile_postfix).getTails()[0]; 

			String dayStr = pDateStr.substring(0,4)+pDateStr.substring(5,7)+pDateStr.substring(8,10);
            String outFilePathDate = outFilePath+dayStr+"/";
			
            String dataName = modAssembly.getName();
			String fileName = outFilePathDate + dataName + outfile_postfix;
	
			sLogger.info("modDataName: " + dataName + "    fileName: " + fileName);
            adoptUtils.makeFileDir(outFilePathDate);
			Map<String, Tap> sinks = new HashMap<String, Tap>();			
			adoptUtils.deleteFile(fileName);
			Scheme sinkScheme = new TextLine(Compress.ENABLE);
			sinkScheme.setNumSinkParts(50);
			Tap sink = new Hfs(sinkScheme, fileName);
			sinks.put(dataName, sink);			
		    
			sLogger.info("Connecting the daily sampling pipe line and assemblys ... ");
			long time_s0 = System.currentTimeMillis();
			
			Properties properties = new Properties();
			properties.setProperty("mapred.job.queue.name", config.get("QueueName"));
			properties.setProperty("mapred.reduce.tasks", numOfReducers);
			
			AppProps.setApplicationJarClass(properties, ModDailyGeneratingData.class);
		    FlowConnector flowConnector = new HadoopFlowConnector(properties);
		    Flow flow = flowConnector.connect("ctr_daily", sources, sinks, modAssembly);
		    flow.complete();
		    
			long time_s1 = System.currentTimeMillis();
			adoptUtils.loggingTimeUsed(time_s0, time_s1, "GeneratingDailySampledData");
			sLogger.info("Generating the daily sampled data on " + pDateStr + " is done.\n");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}  	
}
