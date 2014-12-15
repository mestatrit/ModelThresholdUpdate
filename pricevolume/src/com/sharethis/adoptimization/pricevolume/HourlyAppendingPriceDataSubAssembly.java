package com.sharethis.adoptimization.pricevolume;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;

import java.io.IOException;
import java.text.ParseException;
import org.apache.log4j.Logger;


/**
 * This is the assembly to append the daily data over the number of days.
 */

public class HourlyAppendingPriceDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAppendingPriceDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAppendingPriceDataSubAssembly(Pipe pAssemblyBase,
			Fields[] keyFields, String[] pipeNames) 
		throws IOException, ParseException, Exception
	{
		try{		
			int pipeLen = pipeNames.length;
			Pipe[] pAssembly = new Pipe[pipeLen];
			for(int i_p=0; i_p<pipeLen;i_p++){
			
				// The 'head' of the pipe assembly
				String pPipeName = pipeNames[i_p]+"_win";

				// Accumulating the wins for all price points.							
				// Aggregating the winning data grouped by the groupFields
				Fields groupFields = keyFields[i_p].append(new Fields("winning_price"));
			    // Now create a flow that does a a SumBy. 
					
			    SumBy sum1 = new SumBy(new Fields("sum_wins"), new Fields("sum_wins"), Integer.class); 
			    SumBy sum2 = new SumBy(new Fields("sum_mbids"), new Fields("sum_mbids"), Integer.class); 			       
			    pAssembly[i_p] = new AggregateBy(pPipeName, new Pipe[]{pAssemblyBase}, groupFields, sum1, sum2); 

				Fields sortField = new Fields("winning_price"); 
				pAssembly[i_p] = new GroupBy(pAssembly[i_p], keyFields[i_p], sortField); 
				Buffer<?> accum = new RunningCount(new Fields("cum_wins"));
				pAssembly[i_p] = new Every(pAssembly[i_p], new Fields("sum_wins"), accum, Fields.ALL); 
			}
			setTails(pAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
