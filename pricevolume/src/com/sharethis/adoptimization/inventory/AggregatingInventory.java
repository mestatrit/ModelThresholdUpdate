package com.sharethis.adoptimization.inventory;

import org.apache.log4j.Logger;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class AggregatingInventory extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(AggregatingInventory.class);
	private static final long serialVersionUID = 1L;

	public AggregatingInventory(Pipe[] dataAssembly, String pipeName, Fields keyFields) throws Exception
	{
		try{
			SumBy sum1 = new SumBy(new Fields("sum_nobids"), new Fields("sum_nobids"), Double.class); 
			SumBy sum2 = new SumBy(new Fields("sum_bp"), new Fields("sum_bp"), Double.class); 
			SumBy sum3 = new SumBy(new Fields("sum_mbp"), new Fields("sum_mbp"), Double.class); 
			Pipe nbAssembly = new AggregateBy(pipeName, dataAssembly, keyFields, sum1, sum2, sum3); 
			Function<?> wt_avg = new ComputingWeightedAverage(new Fields("avg_bp", "avg_mbp"));
			nbAssembly = new Each(nbAssembly, new Fields("sum_bp", "sum_mbp", "sum_nobids"), wt_avg, Fields.ALL);			
  			setTails(nbAssembly);	
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
