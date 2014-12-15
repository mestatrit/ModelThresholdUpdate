package com.sharethis.adoptimization.clickthroughrate.ctrmodel.modelvalidation;

import cascading.operation.Filter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sharethis.adoptimization.common.FilterOutDataLEDouble;

/**
 * This is the assembly to aggregate the data over periods.
 */

public class HourlyAggregationPredCTRPipeRolling extends SubAssembly
{
	private static final Logger sLogger = Logger.getLogger(HourlyAggregationPredCTRPipeRolling.class);
	private static final long serialVersionUID = 1L;
	
	public HourlyAggregationPredCTRPipeRolling(Map<String, Tap> sources, String outFilePathHourly, String outFilePathDaily, 
			String pDateStr, int interval1, int interval2, Fields keyFields, Fields pFields, String pipeName, 
			Class[] types, int hour, String outfile_postfix, String infile_postfix_rolling, int numOfKeyFields, 
			int numOfDataFields, double impThVal) throws IOException, ParseException, Exception
	{
		try{		
			Pipe[] ctrAssemblyData = new HourlyAggregationAssemblyRollingPred(sources, outFilePathHourly, outFilePathDaily, 
					pDateStr, interval1, interval2, pFields, pipeName, types, hour, outfile_postfix, infile_postfix_rolling,
					numOfKeyFields, numOfDataFields).getTails();
			Pipe ctrAssembly = null;
			if (ctrAssemblyData!=null){
				sLogger.info("ctrAssemblyData is not null!");
				if(ctrAssemblyData.length>1)
					ctrAssembly = new AggregatingPredCTRAll(ctrAssemblyData, pipeName, keyFields).getTails()[0];
				else
					ctrAssembly = new Pipe(pipeName, ctrAssemblyData[0]);
				//Need to filter out the records with sum_imps<1.0.
				Filter<?> filter = new FilterOutDataLEDouble(impThVal);
				ctrAssembly = new Each(ctrAssembly, new Fields("sum_imps"), filter); 
			}
			setTails(ctrAssembly);
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
