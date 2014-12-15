package com.sharethis.adoptimization.clickthroughrate;

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
 * This is the assembly to aggregate the data over periods.
 */

public class HourlyAggregationCTRPipe extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationCTRPipe.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAggregationCTRPipe(Map<String, Tap> sources, String outFilePathHourly, String outFilePathDaily, 
			String pDateStr, int numOfPeriods, int interval, Configuration config, 
			Fields keyFields, Fields pFields, String pipeName, Class[] types, int hour, String file_post) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] ctrAssemblyData = new HourlyAggregationAssembly(sources, outFilePathHourly, outFilePathDaily, 
					pDateStr, numOfPeriods, interval, config, pFields, pipeName, types, hour, file_post).getTails();		
			
			Pipe ctrAssembly = new AggregatingCTRAll(ctrAssemblyData, pipeName, keyFields).getTails()[0];

			setTails(ctrAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
