package com.sharethis.adoptimization.brandlift.aggregating;

import org.apache.log4j.Logger;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class AggregatingBLData extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(AggregatingBLData.class);
	private static final long serialVersionUID = 1L;

	public AggregatingBLData(Pipe[] blAssembly, String pipeName, Fields keyFields,
			Fields[] inFields, Fields[] outFields) throws Exception
	{
		try{
			SumBy sum1 = new SumBy(inFields[0], outFields[0], double.class); 
			SumBy sum2 = new SumBy(inFields[1], outFields[1], double.class); 
			SumBy sum3 = new SumBy(inFields[2], outFields[2], double.class); 
			SumBy sum4 = new SumBy(inFields[3], outFields[3], int.class); 
			Pipe sumAssembly = new AggregateBy(pipeName, blAssembly, keyFields, sum1, sum2, sum3, sum4); 
  			setTails(sumAssembly);	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
