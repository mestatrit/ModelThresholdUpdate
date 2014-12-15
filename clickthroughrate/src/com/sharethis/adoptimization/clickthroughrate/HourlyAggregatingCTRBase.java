package com.sharethis.adoptimization.clickthroughrate;

import org.apache.log4j.Logger;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class HourlyAggregatingCTRBase extends SubAssembly 
{	
	private static final Logger sLogger = Logger.getLogger(HourlyAggregatingCTRBase.class);
	private static final long serialVersionUID = 1L;

	public HourlyAggregatingCTRBase(Pipe ciAssembly, String basePipeName, Fields baseKeyFields) throws Exception
	{
		try{
			sLogger.info("Entering HourlyAggregatingCTRData ...");
			SumBy sum1 = new SumBy(new Fields("imp_flag"), new Fields("sum_imps1"), double.class); 
			SumBy sum2 = new SumBy(new Fields("click_flag"), new Fields("sum_clicks1"), double.class); 
			SumBy sum3 = new SumBy(new Fields("cost"), new Fields("sum_cost1"), double.class); 
			Pipe ctrAssembly = new AggregateBy(basePipeName, new Pipe[]{ciAssembly}, baseKeyFields, sum1, sum2, sum3); 
			Fields allFields = baseKeyFields;
			allFields = allFields.append(new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc"));
			Function<?> ctrFunc = new ComputingCTRValue(new Fields("sum_imps", "sum_clicks", "sum_cost", "ctr", "e_cpm", "e_cpc"));
			ctrAssembly = new Each(ctrAssembly, new Fields("sum_imps1", "sum_clicks1", "sum_cost1"), ctrFunc, allFields);
  			setTails(ctrAssembly);	
			sLogger.info("Exiting from HourlyAggregatingCTRData.");
		}catch(Exception ee){
			sLogger.info(ee.toString());
			throw new Exception(ee);
		}
	}
}
