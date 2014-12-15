package com.sharethis.adoptimization.pricevolume;

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

public class HourlyAppendingBidDataBase extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAppendingBidDataBase.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAppendingBidDataBase(){}
	
	public HourlyAppendingBidDataBase(Map<String, Tap> sources, 
			String outFilePath, String pDateStr, String[] hourFolders, Configuration config, 
			Fields keyFields, Fields bFields, String pipeNameHour, Class[] types) 
		throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] bidAssembly = new HourlyBidDataSubAssembly(sources, outFilePath, pDateStr, hourFolders, config, 
					bFields, pipeNameHour, types).getTails();
							
			// The 'head' of the pipe assembly
			String bPipeName = pipeNameHour+"_bid";
	
			SumBy sum1 = new SumBy(new Fields("sum_bids"), new Fields("sum_bids"), Integer.class); 
			SumBy sum2 = new SumBy(new Fields("sum_nobids"), new Fields("sum_nobids"), Integer.class); 
			Pipe bAssembly = new AggregateBy(bPipeName, bidAssembly, keyFields, sum1, sum2); 

			setTails(bAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
