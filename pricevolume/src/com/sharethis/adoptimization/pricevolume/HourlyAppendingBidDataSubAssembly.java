package com.sharethis.adoptimization.pricevolume;

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

public class HourlyAppendingBidDataSubAssembly extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAppendingBidDataSubAssembly.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAppendingBidDataSubAssembly(){}
	
	public HourlyAppendingBidDataSubAssembly(Pipe bAssemblyBase, Fields[] keyFields, String[] pipeNames) 
		throws IOException, ParseException, Exception
	{
		try{						
			int pipeLen = pipeNames.length;
			Pipe[] bAssembly = new Pipe[pipeLen];
			for(int i_p=0; i_p<pipeLen;i_p++){
			
				// The 'head' of the pipe assembly
				String bPipeName = pipeNames[i_p]+"_bid";
	            
			    SumBy sum1 = new SumBy(new Fields("sum_bids"), new Fields("sum_bids"), Integer.class); 
			    SumBy sum2 = new SumBy(new Fields("sum_nobids"), new Fields("sum_nobids"), Integer.class); 
			    bAssembly[i_p] = new AggregateBy(bPipeName, new Pipe[]{bAssemblyBase}, keyFields[i_p], sum1, sum2); 
			}
			setTails(bAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
