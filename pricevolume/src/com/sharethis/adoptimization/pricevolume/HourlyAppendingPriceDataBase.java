package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * This is the assembly to append the daily data over the number of days.
 */

public class HourlyAppendingPriceDataBase extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAppendingPriceDataBase.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAppendingPriceDataBase(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, String[] hourFolders, Configuration config, 
			Fields keyFields, Fields pFields, String pipeNameHour, Class[] types) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] winAssembly = new HourlyPriceDataSubAssembly(sources, outFilePath, pDateStr, hourFolders, config, 
					pFields, pipeNameHour, types).getTails();			
			// The 'head' of the pipe assembly
			String pPipeName = pipeNameHour+"_win";
			//Pipe pAssembly = new Pipe(pPipeName);

			// Accumulating the wins for all price points.							
			// Aggregating the winning data grouped by the groupFields
			Fields groupFields = keyFields.append(new Fields("winning_price"));
			// Now create a flow that does a a SumBy. 
					
			SumBy sum1 = new SumBy(new Fields("sum_wins"), new Fields("sum_wins"), Integer.class); 
			SumBy sum2 = new SumBy(new Fields("sum_mbids"), new Fields("sum_mbids"), Integer.class); 			       
			Pipe pAssembly = new AggregateBy(pPipeName, winAssembly, groupFields, sum1, sum2); 

			Fields sortField = new Fields("winning_price"); 
			Pipe pAssembly_cum = new GroupBy(pPipeName, pAssembly, keyFields, sortField); 
			Buffer<?> accum = new RunningCount(new Fields("cum_wins"));
			pAssembly= new Every(pAssembly_cum, new Fields("sum_wins"), accum, Fields.ALL); 

			setTails(pAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
