package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This is the assembly to append the hourly data over the number of hours.
 */

public class DailyAggregationPredCTRPipe extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(DailyAggregationPredCTRPipe.class);
	private static final long serialVersionUID = 1L;
	
	public DailyAggregationPredCTRPipe(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, int numOfHours, int interval, Configuration config, 
			Fields keyFields, Fields pFields, String pipeName, int hour, Class[] types, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] ctrAssemblyData = new DailyAggregationPredCTRAssembly(sources, outFilePath, pDateStr, numOfHours, 
					interval, config, pFields, pipeName, hour, types, infile_postfix).getTails();		
			
			Pipe ctrAssembly = new AggregatingPredCTRAll(ctrAssemblyData, pipeName, keyFields).getTails()[0];

			setTails(ctrAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
