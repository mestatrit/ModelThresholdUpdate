package com.sharethis.adoptimization.clickthroughrate;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
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

public class MappingsDataAggregation extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(MappingsDataAggregation.class);
	private static final long serialVersionUID = 1L;
	
	public MappingsDataAggregation(Map<String, Tap> sources, 
			String outFilePathHourly, String pDateStr, int numOfHours, int interval, Configuration config, 
			Fields keyFields, Fields pFields, String pipeName, int hour, Class[] types, String infile_postfix) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] mapAssemblyData = new DailyAggregationAssembly(sources, outFilePathHourly, pDateStr, numOfHours, 
					interval, config, pFields, pipeName, hour, types, infile_postfix).getTails();		

			SumBy sum1 = new SumBy(new Fields("sum_wt1"), new Fields("sum_wt"), double.class); 
			SumBy sum2 = new SumBy(new Fields("sum_imp1"), new Fields("sum_imp"), double.class); 
			Pipe mapAssembly = new AggregateBy(pipeName, mapAssemblyData, keyFields, sum1, sum2); 
			Fields keptFields = keyFields.append(new Fields("sum_wt", "sum_imp"));
			mapAssembly = new Each(mapAssembly, keptFields, new Identity());				

//			Pipe mapAssembly = new Pipe(pipeName, new Unique(mapAssemblyData, keyFields, 100000));
			
  			setTails(mapAssembly);	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
