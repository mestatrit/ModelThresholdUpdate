package com.sharethis.adoptimization.clickthroughrate;

import org.apache.log4j.Logger;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class AggregatingCTRAll extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(AggregatingCTRAll.class);
	private static final long serialVersionUID = 1L;

	public AggregatingCTRAll(Pipe[] ciAssembly, String pipeName, Fields keyFields) throws Exception
	{
		try{
//			sLogger.info("Entering AggregatingPredCTRAll ...");
			SumBy sum1 = new SumBy(new Fields("sum_imps"), new Fields("sum_imps1"), double.class); 
			SumBy sum2 = new SumBy(new Fields("sum_clicks"), new Fields("sum_clicks1"), double.class); 
			SumBy sum3 = new SumBy(new Fields("sum_cost"), new Fields("sum_cost1"), double.class); 
			Pipe ctrAssembly = new AggregateBy(pipeName, ciAssembly, keyFields, sum1, sum2, sum3); 
			Fields allFields = keyFields;
			allFields = allFields.append(new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc"));
			Function<?> ctrFunc = new ComputingCTRValue(new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc"));
			ctrAssembly = new Each(ctrAssembly, new Fields("sum_imps1", "sum_clicks1", "sum_cost1"), ctrFunc, allFields);
  			setTails(ctrAssembly);	
//			sLogger.info("Exiting from AggregatingPredCTRAll.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
